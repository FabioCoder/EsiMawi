import requests
import sys
import logging
import os
import simplejson as json
from sqlalchemy import create_engine
from sqlalchemy import func, update
from sqlalchemy.orm import sessionmaker, relationship
import ast
from contextlib import contextmanager
from schema import Inventory, Place, Stock, Material, StockEntry, GoodsOrder, GoodsOrderPosition, MaterialSchema, \
    PlaceSchema, \
    InventorySchema, StockEntrySchema, BookMaterialSchema, BookProductToStockSchema, ReservationOrderPositionSchema, \
    ReservationOrderSchema, GoodsOrderSchema, BookProductFromStockSchema

rds_host = os.environ['DB_HOST']
name = os.environ['DB_USER']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

rds_host = os.environ['DB_HOST']
name = os.environ['DB_USER']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']

ApiProductionUrl = 'https://2pkivl4tnh.execute-api.eu-central-1.amazonaws.com/prod/readorderinfo'
ApiEndpointProdOrder = 'readorderinfo'

engine = create_engine('mysql+mysqlconnector://' + name + ':' + password + '@' + rds_host + '/' + db_name, echo=True)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


# LAMBDA
def getInventory(event, context):
    with session_scope() as session:
        inventory = session.query(Inventory).order_by(Inventory.fkplaces)

        # Serialize the queryset
        result = InventorySchema().dump(inventory, many=True)

    return {
        "statusCode": 200,
        "body": json.dumps(result, use_decimal=True),
    }


# Buchen von Material mit Materialnummer
def bookMaterial(event, context):
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
    logger.info(body)

    schema = BookMaterialSchema()
    bookMaterial = schema.load(data=body)

    if bookMaterial['fkmaterials'] < 50000000:
        return {
            "statusCode": 400,
            "body": {
                "errorMessage": "Die Buchung von Fertigware mit diesem Service ist nicht erlaubt.",
            }
        }

    return bookToStock(fkmaterials=bookMaterial['fkmaterials'], fkplaces=bookMaterial['fkplaces'],
                       opened=bookMaterial['opened'], quantity=bookMaterial['quantity'], productionOrderNr='')


# Zubuchen von Produkten mit ProductionOrderNr
def bookProductToStock(event, context):
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
    logger.info(body)

    schema = BookProductToStockSchema()
    bookProduct = schema.load(data=body)

    data = json.dumps({'prodOrderNum': bookProduct.get('productionOrderNr')})
    r = requests.post(ApiProductionUrl, data=data)

    if r.status_code != 200:
        return {
            "statusCode": 400,
            "body": 'Fehler bei der Abfrage der ProductionOrderNr: ' + json.dumps(r.json()),
        }

    prodOrder = json.loads(r.text)

    if prodOrder['statusCode'] != 200:
        return {
            "statusCode": 400,
            "body": 'Fehler bei der Abfrage der ProductionOrderNr: ' + json.dumps(r.json()),
        }

    fkmaterials = prodOrder['body'][0]['articleNumber']
    quantity = prodOrder['body'][0]['quantity']

    with session_scope() as session:
        query_material = session.query(Material).filter(Material.idmaterials == fkmaterials).first()

        # Anlegen des Materials (falls nicht vorhanden!)
        if query_material is None:
            material_new = Material(idmaterials=fkmaterials, name='Fertigware eines Kunden', size=1, measure='st',
                                    art='Fertigware')
            session.add(material_new)
            session.commit()

    return bookToStock(fkmaterials, bookProduct['fkplaces'], 0, quantity, bookProduct['productionOrderNr'])


# Abbuchen eines reservierten Bestands
def bookProductFromStock(event, context):
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
    logger.info(body)

    schema = BookProductFromStockSchema()
    bookProduct = schema.load(data=body)

    with session_scope() as session:
        reservation = session.query(GoodsOrderPosition, GoodsOrder).filter(
            (GoodsOrderPosition.fkgoodsOrders == bookProduct.get('fkgoodsOrders')) & \
            (GoodsOrderPosition.productionOrderNr == bookProduct.get('productionOrderNr'))).first()

        if reservation is None:
            return {
                "statusCode": 400,
                "body": "Die Reservierung existiert nicht.",
            }

        place = session.query(func.ifnull(func.sum(StockEntry.quantity), 0)). \
            filter((StockEntry.productionOrderNr == bookProduct.get('productionOrderNr')) &
                   (StockEntry.fkplaces == bookProduct.get('fkplaces'))). \
            group_by(StockEntry.productionOrderNr).having(func.ifnull(func.sum(StockEntry.quantity), 0) > 0).first()

        if place is None:
            return {
                "statusCode": 400,
                "body": "Für die Production-Order-Nr. existiert auf dem Lagerplatz kein Bestand.",
            }

        if place[0] < reservation.GoodsOrderPosition.quantity:
            return {
                "statusCode": 400,
                "body": "Der Lagerplatz hat nicht genügend Bestand Für die Production-Order-Nr.",
            }

        reservation.GoodsOrderPosition.done = 1

        return bookToStock(fkmaterials=reservation.GoodsOrder.fkmaterials, fkplaces=bookProduct.get('fkplaces'),
                           opened=0, quantity=reservation.GoodsOrderPosition.quantity * (-1),
                           productionOrderNr=bookProduct.get('productionOrderNr'))


def bookToStock(fkmaterials, fkplaces, opened, quantity, productionOrderNr):
    with session_scope() as session:
        # Prüfen ob das Material existiert
        query_material = session.query(Material).filter(Material.idmaterials == fkmaterials).first()

        if query_material is None:
            # Material existiert nicht
            return {
                "statusCode": 400,
                "body": "Die Materialnummer existiert nicht.",
            }

        if quantity < 0:
            # das Material existiert in der Materialliste. Prüfen ob das Material auch genügend Bestand hat.
            query_inventory = session.query(Inventory).filter(Inventory.fkmaterials == fkmaterials).first()

            # kein Bestand oder
            if (query_inventory is None) or (query_inventory.quantity < abs(quantity)):
                # nicht genug Lagerbestand
                return {
                    "statusCode": 400,
                    "body": "Der Lagerbestand für das Material ist zu niedrig.",
                }

        # Buchen des Produktionsauftrags
        stockEntry_new = StockEntry(fkmaterials=fkmaterials, fkplaces=fkplaces, opened=opened,
                                    quantity=quantity, productionOrderNr=productionOrderNr)
        session.add(stockEntry_new)
        session.commit()

        # Serialize the queryset
        result = StockEntrySchema().dump(stockEntry_new)
        return {
            "statusCode": 200,
            "body": json.dumps(result),
        }


def getPackageList(event, context):
    with session_scope() as session:
        goodsOrdersPositions = session.query(GoodsOrderPosition).\
            filter((GoodsOrderPosition.done != 1) | (GoodsOrderPosition.done.is_(None))).\
            order_by(GoodsOrderPosition.fkgoodsOrders).all()

        # Serialize the queryset
        result = ReservationOrderPositionSchema().dump(goodsOrdersPositions, many=True)
    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }


# Reservieren von Produkten aus dem Lager entweder mit Materialnummer + Menge oder ProductionOrderNr
def createGoodsOrders(event, context):
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
    logger.info(body)

    schema = GoodsOrderSchema()
    orders = schema.load(data=body, many=True)

    reservations = []
    error_messages = []
    for order in orders:
        # Eine Order kann folgendes enthalten:
        # - Materialnummer + Menge : Ermitteln von ProductionOrders + Reservieren
        # - ProductionOrderNr : Reservieren

        with session_scope() as session:
            if (order.get('fkmaterials') is not None) and (order.get('quantity') is not None):
                reservation, error_message = reserveProductsWithArticelNr(fkmaterials=order.get('fkmaterials'),
                                                                          quantity=order.get('quantity'),
                                                                          session=session)
            elif order.get('productionOrderNr') is not None:
                reservation, error_message = reserveProductsWithProdOrderNr(order.get('productionOrderNr'),
                                                                            session=session)
            else:
                reservation = None
                error_messages = 'Bad Request.'

            reservation_json = ReservationOrderSchema().dump(reservation)

            if reservation is not None:
                reservations.append(reservation_json)

            if error_message.strip() != '':
                error_messages.append(error_message)

    return {
        "statusCode": 200,
        "body": {
            "reservations": json.dumps(reservations),
            "errorMessage": json.dumps(error_messages),
        }
    }


def reserveProductsWithArticelNr(fkmaterials, quantity, session):
    stock = session.query(func.ifnull(func.sum(StockEntry.quantity), 0),
                          func.min(StockEntry.booking_date), StockEntry.productionOrderNr, StockEntry.fkmaterials). \
        filter((StockEntry.productionOrderNr != '') and (StockEntry.fkmaterials == fkmaterials)). \
        group_by(StockEntry.productionOrderNr, StockEntry.fkmaterials). \
        having(func.sum(StockEntry.quantity) > 0). \
        order_by(StockEntry.booking_date).all()

    if stock is None:
        # Problem: Für die Materialnummer gibt es keinen Bestand, daher kan dieser auch nicht ausgeliefert werden.
        return None, 'Für den Artikel ' + str(fkmaterials) + ' gibt es keinen Bestand.'

    # Anlegen des Reservierungskopfes
    new_goodsOrder = GoodsOrder(fkmaterials=fkmaterials)
    session.add(new_goodsOrder)
    session.commit()
    fkgoodsOrders = new_goodsOrder.idgoodsOrders

    # Reservieren der Menge
    remaining_quantity = quantity
    for pos in stock:
        if remaining_quantity <= 0:
            break
        # Ermittlung des bereits reservierten Bestands
        reserved_stock = session.query(func.ifnull(func.sum(GoodsOrderPosition.quantity), 0)). \
            filter((GoodsOrderPosition.productionOrderNr == pos[2]) & (
                (GoodsOrderPosition.done != 1) | (GoodsOrderPosition.done.is_(None)))). \
            group_by(GoodsOrderPosition.productionOrderNr).first()

        if reserved_stock is not None:
            # Berechnung des nicht reservierten Bestands
            not_reserved_stock = pos[0] - reserved_stock[0]
        else:
            not_reserved_stock = pos[0]

        # Der Bestand der ProductionOrderNr ist komplett reserviert. Zur nächsten ProductionOrderNr springen.
        if not_reserved_stock <= 0:
            continue

        # Menge der Reservierung ermitteln:
        if not_reserved_stock >= remaining_quantity:
            # Der nicht reservierte Bestand ist größer als die Restmenge. -> Die komplette Restmenge bei dieser
            # ProductionOrderNr reservieren.
            booked_stock = remaining_quantity
        elif remaining_quantity > not_reserved_stock:
            # Die Restmenge ist größer als der Bestand -> Den kompletten Bestand reservieren
            booked_stock = not_reserved_stock

        # Buchen der Reservierung
        new_goodsOrderPos = GoodsOrderPosition(fkgoodsOrders=fkgoodsOrders,
                                               productionOrderNr=pos[2],
                                               quantity=booked_stock)

        session.add(new_goodsOrderPos)
        remaining_quantity = remaining_quantity - booked_stock

    if remaining_quantity > 0:
        # Fehler:  Der Bestand für die Reservierung ist nicht ausreichend!
        return new_goodsOrder, 'Der Bestand für den Artikel ' + str(fkmaterials) + \
               ' ist nicht ausreichend. Insgesamt  konnten' + str(remaining_quantity) + ' Stück nicht gebucht werden.'

    return new_goodsOrder, ''


def reserveProductsWithProdOrderNr(ProductionOrderNr, session):
    query_StockEntry = session.query(func.ifnull(func.sum(StockEntry.quantity), 0), StockEntry.fkmaterials).filter(
        StockEntry.productionOrderNr == ProductionOrderNr).group_by(StockEntry.fkmaterials).first()

    if query_StockEntry[0] == 0:
        # Der Produktionsauftrag existiert nicht (mehr)
        return None, 'Der Produktionsauftrag ' + ProductionOrderNr + ' existiert nicht.'

    query_Reservations = session.query(func.ifnull(func.sum(GoodsOrderPosition.quantity), 0)). \
        filter((GoodsOrderPosition.productionOrderNr == ProductionOrderNr) & (
            (GoodsOrderPosition.done != 1) | (GoodsOrderPosition.done.is_(None)))). \
        group_by(GoodsOrderPosition.productionOrderNr).first()

    if query_Reservations is not None:
        # Berechnung des nicht reservierten Bestands
        not_reserved_stock = query_StockEntry[0] - query_Reservations[0]
    else:
        not_reserved_stock = query_StockEntry[0]

    if not_reserved_stock <= 0:
        # Der Produktionauftrag wurde vollständig reserviert
        return None, 'Der Produktionsauftrag ' + ProductionOrderNr + ' wurde bereits vollständig reserviert.'

    # Anlegen des Reservierungskopfes
    new_goodsOrder = GoodsOrder(fkmaterials=query_StockEntry[1])
    session.add(new_goodsOrder)
    session.commit()
    fkgoodsOrders = new_goodsOrder.idgoodsOrders

    # Buchen der Reservierung
    new_goodsOrderPos = GoodsOrderPosition(fkgoodsOrders=fkgoodsOrders,
                                           productionOrderNr=ProductionOrderNr,
                                           quantity=not_reserved_stock)
    session.add(new_goodsOrderPos)

    return new_goodsOrder, ''
