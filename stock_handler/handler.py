import requests
import sys
import logging
import os
import simplejson as json
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
import ast
from contextlib import contextmanager
from schema_stock import Inventory, Material, StockEntry, GoodsOrder, GoodsOrderPosition, \
    InventorySchema, StockEntrySchema, BookMaterialSchema, BookProductToStockSchema, ReservationOrderPositionSchema, \
    GoodsOrderSchema, BookProductFromStockSchema, ReservationResponseSchema, Receiving, ReceivingPosition

rds_host = os.environ['DB_HOST']
name = os.environ['DB_USER']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Datenbank
rds_host = os.environ['DB_HOST']
name = os.environ['DB_USER']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']

engine = create_engine('mysql+mysqlconnector://' + name + ':' + password + '@' + rds_host + '/' + db_name, echo=True)

# Urls der anderen Webservices
ApiProductionUrl = 'https://2pkivl4tnh.execute-api.eu-central-1.amazonaws.com/prod/readorderinfo'
ApiVersandUrl = 'https://5club7wre8.execute-api.eu-central-1.amazonaws.com/sales/updatestatus'


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


def calcMaterialValue(session, fkmaterials, quantity):
    """Ermittelt den Wert des Materials mittels des Preises aus den letzten Wareneingängen (LIFO)."""

    # letzte Wareneingänge mit Preise ermitteln
    receiving_query = session.query(ReceivingPosition.price, ReceivingPosition.quantity, Receiving.receiving_date). \
        filter(ReceivingPosition.fkmaterials == fkmaterials). \
        filter(ReceivingPosition.fkreceivings == Receiving.id). \
        order_by(Receiving.receiving_date.desc()).all()

    # Keine Wareneingänge vorhanden.
    if (receiving_query is None) or (len(receiving_query) <= 0):
        return 0.0

    # Einzelne Wareneingänge verechnen.
    remaining_quantity = quantity
    value = 0.0
    for receiving in receiving_query:
        # Keine zu verrechnende Menge übrig.
        if remaining_quantity <= 0:
            break

        # Kein Preis vorhanden. Ermittlung abbrechen, da die Kennzahl nun nicht valide berechnet werden kann.
        if receiving.price is None:
            value = 0.0
            break

        # Wareneingang hat keine Menge.
        if receiving.quantity <= 0:
            continue

        # Menge ermitteln:
        current_quantity = 0
        if receiving.quantity >= remaining_quantity:
            # Die Menge des Wareingangs ist größer als die Restmenge. -> Die komplette Restmenge mit dem
            # Preis des Wareingangs verrechnen.
            current_quantity = remaining_quantity
        elif remaining_quantity > receiving.quantity:
            # Die Restmenge ist größer als die Menge des Wareingangs -> Die komplette Menge des Wareingangs
            # verrechnen.
            current_quantity = receiving.quantity

        # Wert summieren.
        value = value + (current_quantity * float(receiving.price))
        remaining_quantity = remaining_quantity - current_quantity

    if remaining_quantity <= 0:
        return value
    else:
        return 0.0


def getInventory(event, context):  # Lambda Function
    """Gibt das aktuelle Inventar zurück.

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    with session_scope() as session:
        inventory = session.query(Inventory).order_by(Inventory.fkplaces).all()

        # Serialize the queryset
        result = InventorySchema().dump(inventory, many=True)

        # Ermittle den Wert der Position mittels dem Preis aus dem Wareneingang
        for pos in result:
            pos['value_of_materials'] = calcMaterialValue(session, pos.get('fkmaterials'), pos.get('quantity'))

    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result, use_decimal=True),
    }


def bookMaterial(event, context):  # Lambda Function
    """Zu- und Abbuchung von Material."""
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
    logger.info(body)

    schema = BookMaterialSchema()
    bookMaterial = schema.load(data=body)

    if bookMaterial['fkmaterials'] < 50000000:
        return {
            "statusCode": 400,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            "body": json.dumps({"message": "Die Buchung von Fertigware mit diesem Service ist nicht erlaubt."}),
        }

    return bookToStock(fkmaterials=bookMaterial['fkmaterials'], fkplaces=bookMaterial['fkplaces'],
                       opened=bookMaterial['opened'], quantity=bookMaterial['quantity'], productionOrderNr='')


def bookProductToStock(event, context):  # Lambda Function
    """Zubuchung von Produkten mit Produktions-Order-Nr."""
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
    logger.info(body)

    schema = BookProductToStockSchema()
    bookProduct = schema.load(data=body)

    logger.info('Start Request')

    data = json.dumps({'prodOrderNum': bookProduct.get('productionOrderNr')})
    r = requests.post(ApiProductionUrl, data=data)
    logger.info('End Request')

    if r.status_code != 200:
        return {
            "statusCode": 400,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            "body": json.dumps({"message": 'Fehler bei der Abfrage der ProductionOrderNr. ' + json.dumps(r.json())}),
        }

    prodOrder = json.loads(r.text)

    if prodOrder['statusCode'] != 200:
        return {
            "statusCode": 400,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            "body": json.dumps({"message": 'Fehler bei der Abfrage der ProductionOrderNr: ' + json.dumps(r.json())}),
        }

    try:
        fkmaterials = prodOrder['body'][0]['articleNumber']
        quantity = prodOrder['body'][0]['quantity']
    except:
        e = sys.exc_info()[0]
        return {
            "statusCode": 400,
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            "body": json.dumps({"message": 'Fehler bei der Abfrage der ProductionOrderNr. Der Rückgabewert ist '
                                           'invalide. :' + str(e)})
        }

    logger.info('Datenbank Änderungen')
    with session_scope() as session:
        query_material = session.query(Material).filter(Material.idmaterials == fkmaterials).first()

        # Anlegen des Materials (falls nicht vorhanden!)
        if query_material is None:
            material_new = Material(idmaterials=fkmaterials, name='Fertigware eines Kunden', size=1, measure='st',
                                    art='Fertigware')
            session.add(material_new)
            session.commit()

    data = json.dumps(
        {'prodOrderNr': bookProduct.get('productionOrderNr'), 'statusID': 4, 'statusDescription': 'Auf Lager'})
    r = requests.patch(ApiVersandUrl, data=data)
    logger.info(json.dumps(r.json()))
    logger.info('End Request')

    return bookToStock(fkmaterials, bookProduct['fkplaces'], 0, quantity, bookProduct['productionOrderNr'])


def bookProductFromStock(event, context):  # Lambda Function
    """Abbuchen von Produkten mit Produktions-Order-Nr. und Reservierung."""
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
    logger.info(body)

    schema = BookProductFromStockSchema()
    bookProduct = schema.load(data=body)

    with session_scope() as session:
        reservation = session.query(GoodsOrderPosition, GoodsOrder).filter(
            (GoodsOrderPosition.fkgoodsOrders == GoodsOrder.idgoodsOrders) &
            (GoodsOrderPosition.fkgoodsOrders == bookProduct.get('fkgoodsOrders')) &
            (GoodsOrderPosition.productionOrderNr == bookProduct.get('productionOrderNr')) &
            (GoodsOrderPosition.fkplaces == bookProduct.get('fkplaces')) &
            ((GoodsOrderPosition.done != 1) | (GoodsOrderPosition.done.is_(None)))).first()

        if reservation is None:
            return {
                "statusCode": 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                "body": json.dumps({"message": "Die Reservierung existiert nicht."}),
            }

        place = session.query(func.ifnull(func.sum(StockEntry.quantity), 0)). \
            filter((StockEntry.productionOrderNr == bookProduct.get('productionOrderNr')) &
                   (StockEntry.fkplaces == bookProduct.get('fkplaces')) &
                   (StockEntry.fkmaterials == reservation.GoodsOrder.fkmaterials)). \
            group_by(StockEntry.productionOrderNr).having(func.ifnull(func.sum(StockEntry.quantity), 0) > 0).first()

        if place is None:
            return {
                "statusCode": 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                "body": json.dumps(
                    {"message": "Für die Production-Order-Nr. existiert auf dem Lagerplatz kein Bestand."}),
            }

        if place[0] < reservation.GoodsOrderPosition.quantity:
            return {
                "statusCode": 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                "body": json.dumps(
                    {"message": "Der Lagerplatz hat nicht genügend Bestand Für die Production-Order-Nr."}),
            }

        reservation.GoodsOrderPosition.done = 1

        return bookToStock(fkmaterials=reservation.GoodsOrder.fkmaterials, fkplaces=bookProduct.get('fkplaces'),
                           opened=0, quantity=reservation.GoodsOrderPosition.quantity * (-1),
                           productionOrderNr=bookProduct.get('productionOrderNr'))


def bookToStock(fkmaterials, fkplaces, opened, quantity, productionOrderNr):
    """Allgemeine Lagerbuchungsfunktion."""
    with session_scope() as session:
        # Prüfen ob das Material existiert
        query_material = session.query(Material).filter(Material.idmaterials == fkmaterials).first()

        if query_material is None:
            # Material existiert nicht
            return {
                "statusCode": 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET, DELETE'
                },
                "body": json.dumps({"message": "Die Materialnummer existiert nicht."}),
            }

        if quantity < 0:
            # das Material existiert in der Materialliste. Prüfen ob das Material auch genügend Bestand hat.
            query_inventory = session.query(Inventory).filter(Inventory.fkmaterials == fkmaterials).first()

            # kein Bestand oder
            if (query_inventory is None) or (query_inventory.quantity < abs(quantity)):
                # nicht genug Lagerbestand
                return {
                    "statusCode": 400,
                    'headers': {
                        'Access-Control-Allow-Headers': 'Content-Type',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET, DELETE'
                    },
                    "body": json.dumps({"message": "Der Lagerbestand für das Material ist zu niedrig."}),
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
            'headers': {
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET, DELETE'
            },
            "body": json.dumps(result),
        }


def getPackageList(event, context):  # Lambda Function
    """Gibt alle offenen Reservierungen bzw. Bestellungen des Versands als Packliste zurück."""
    with session_scope() as session:
        goodsOrdersPositions = session.query(GoodsOrderPosition). \
            filter((GoodsOrderPosition.done != 1) | (GoodsOrderPosition.done.is_(None))). \
            order_by(GoodsOrderPosition.fkgoodsOrders).all()

        # Serialize the queryset
        result = ReservationOrderPositionSchema().dump(goodsOrdersPositions, many=True)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def createGoodsOrders(event, context):  # Lambda Function
    """Anlage von einer oder mehreren Reservierungen/Bestellungen von Produkten entweder mit Materialnummer + Menge
    oder ProductionOrderNr """
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
    logger.info(body)

    schema = GoodsOrderSchema()
    orders = schema.load(data=body, many=True)

    reservations = []
    error_messages = []
    response = []
    with session_scope() as session:

        # Eine Order kann folgendes enthalten:
        # - Materialnummer + Menge : Ermitteln von ProductionOrders + Reservieren
        # - ProductionOrderNr : Reservieren

        for order in orders:
            if (order.get('fkmaterials') is not None) and (order.get('quantity') is not None):
                reservation, error_message = reserveProductsWithArticelNr(fkmaterials=order.get('fkmaterials'),
                                                                          quantity=order.get('quantity'),
                                                                          session=session)
            elif order.get('productionOrderNr') is not None:
                reservation, error_message = reserveProductsWithProdOrderNr(order.get('productionOrderNr'),
                                                                            session=session)
            else:
                reservation = None
                error_message = 'Bad Request.'

            response.append(dict(error_message=error_message, reservation=reservation))

        result = ReservationResponseSchema().dump(response, many=True)
    logger.info(result)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def reserveProductsWithArticelNr(fkmaterials, quantity, session):
    """Reservierung von Produkten mit Materialnummer und Menge."""
    stock = session.query(func.ifnull(func.sum(StockEntry.quantity), 0), func.min(StockEntry.booking_date),
                          StockEntry.productionOrderNr, StockEntry.fkmaterials, StockEntry.fkplaces). \
        filter((StockEntry.productionOrderNr != '') and (StockEntry.fkmaterials == fkmaterials)). \
        group_by(StockEntry.productionOrderNr, StockEntry.fkmaterials, StockEntry.fkplaces). \
        having(func.ifnull(func.sum(StockEntry.quantity), 0) > 0). \
        order_by(StockEntry.booking_date).all()

    if stock is None:
        # Problem: Für die Materialnummer gibt es keinen Bestand, daher kann dieser auch nicht ausgeliefert werden.
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
            filter((GoodsOrderPosition.productionOrderNr == pos[2]) & (GoodsOrderPosition.fkplaces == pos[4]) &
                   ((GoodsOrderPosition.done != 1) | (GoodsOrderPosition.done.is_(None)))). \
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
                                               quantity=booked_stock, fkplaces=pos[4])

        session.add(new_goodsOrderPos)
        remaining_quantity = remaining_quantity - booked_stock

    session.refresh(new_goodsOrder)

    if remaining_quantity > 0:
        # Fehler:  Der Bestand für die Reservierung ist nicht ausreichend!
        return new_goodsOrder, 'Der Bestand für den Artikel ' + str(fkmaterials) + \
               ' ist nicht ausreichend. Insgesamt  konnten ' + str(remaining_quantity) + ' Stück nicht gebucht werden.'

    return new_goodsOrder, ''


def reserveProductsWithProdOrderNr(ProductionOrderNr, session):
    """Reservierung von Produkten mit ProductionOrderNr."""
    stock = session.query(func.ifnull(func.sum(StockEntry.quantity), 0), StockEntry.fkmaterials,
                          StockEntry.fkplaces). \
        filter(StockEntry.productionOrderNr == ProductionOrderNr). \
        group_by(StockEntry.fkmaterials, StockEntry.fkplaces). \
        having(func.ifnull(func.sum(StockEntry.quantity), 0) > 0).all()

    if stock is None:
        # Der Produktionsauftrag existiert nicht (mehr)
        return None, 'Der Produktionsauftrag ' + ProductionOrderNr + ' existiert nicht.'

    # Anlegen des Reservierungskopfes
    new_goodsOrder = GoodsOrder(fkmaterials=stock[0][1])
    session.add(new_goodsOrder)
    session.commit()
    fkgoodsOrders = new_goodsOrder.idgoodsOrders

    for pos in stock:
        reserved_stock = session.query(func.ifnull(func.sum(GoodsOrderPosition.quantity), 0)). \
            filter(
            (GoodsOrderPosition.productionOrderNr == ProductionOrderNr) & (GoodsOrderPosition.fkplaces == pos[2]) &
            ((GoodsOrderPosition.done != 1) | (GoodsOrderPosition.done.is_(None)))). \
            group_by(GoodsOrderPosition.productionOrderNr).first()

        if reserved_stock is not None:
            # Berechnung des nicht reservierten Bestands
            not_reserved_stock = pos[0] - reserved_stock[0]
        else:
            not_reserved_stock = pos[0]

        if not_reserved_stock <= 0:
            # Der Produktionauftrag wurde vollständig reserviert
            return None, 'Der Produktionsauftrag ' + ProductionOrderNr + ' wurde bereits vollständig reserviert.'

        # Buchen der Reservierung
        new_goodsOrderPos = GoodsOrderPosition(fkgoodsOrders=fkgoodsOrders,
                                               productionOrderNr=ProductionOrderNr,
                                               quantity=not_reserved_stock, fkplaces=pos[2])
        session.add(new_goodsOrderPos)

    return new_goodsOrder, ''
