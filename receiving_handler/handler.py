import simplejson as json
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from schema import Material, Receiving, ReceivingPosition, Order, OrderPosition, Charge, ChargeShirt, ChargeColor, \
    MaterialSchema, ReceivingSchema, ReceivingPositionSchema, OrderSchema, OrderPositionSchema, ChargeSchema, \
    ChargeShirtSchema, ChargeColorSchema, Supplier, SupplierSchema

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Datenbank
rds_host = os.environ['DB_HOST']
name = os.environ['DB_USER']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']

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


def getReceiving(event, context):
    """Gibt den Wareneingang mit einer bestimmten ID zurück."""
    params = event["pathParameters"]
    id = params["id"]

    with session_scope() as session:
        receiving = session.query(Receiving).filter(Receiving.id == id).first()
        if receiving is None:
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps({"message": "[BadRequest] Ungültige Wareneingang-ID übergeben."})
            }

        # Serialize the queryset
        result = ReceivingSchema().dump(receiving)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def createReceiving(event, context):
    """Anlage oder Änderung eines Wareneingangs."""
    logger.info(event)

    body = json.loads(event.get('body'))
    logger.info(body)

    with session_scope() as session:
        receiving_new = ReceivingSchema().load(body, session=session)
        session.add(receiving_new)
        session.commit()
        # Serialize the queryset
        result = ReceivingSchema().dump(receiving_new)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def createReceivingPos(event, context):
    """Anlage oder Änderung einer Wareneingangsposition."""
    logger.info(event)

    body = json.loads(event.get('body'))
    logger.info(body)

    with session_scope() as session:
        receivingPos_new = ReceivingPositionSchema().load(body, session=session)
        session.add(receivingPos_new)
        session.commit()
        # Serialize the queryset
        result = ReceivingPositionSchema().dump(receivingPos_new)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def get_allReceiving(event, context):
    """Gibt alle Wareneingänge zurück."""
    with session_scope() as session:
        receivings = session.query(Receiving).with_entities(Receiving.id, Receiving.receiving_date). \
            order_by(Receiving.receiving_date)

        # Serialize the queryset
        result = ReceivingSchema().dump(receivings, many=True)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def getOrder(event, context):
    """Gibt eine Bestellung mit einer bestimmten ID zurück."""
    params = event["pathParameters"]
    id = params["id"]

    with session_scope() as session:
        order = session.query(Order).filter(Order.idorders == id).first()
        if order is None:
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps({"message": "[BadRequest] Ungültige Wareneingang-ID übergeben."})
            }

        # Serialize the queryset
        result = OrderSchema().dump(order)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def createOrder(event, context):
    """Anlage oder Änderung einer Bestellung."""
    logger.info(event)

    body = json.loads(event.get('body'))
    logger.info(body)

    with session_scope() as session:
        order_new = OrderSchema().load(body, session=session)
        session.add(order_new)
        session.commit()
        # Serialize the queryset
        result = OrderSchema().dump(order_new)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def createOrderPos(event, context):
    """Anlage oder Änderung einer Bestellposition."""
    logger.info(event)

    body = json.loads(event.get('body'))
    logger.info(body)

    with session_scope() as session:
        orderPos_new = OrderPositionSchema().load(body, session=session, many=True)
        for pos in orderPos_new:
            session.add(pos)
        session.commit()
        # Serialize the queryset
        result = OrderPositionSchema().dump(orderPos_new, many=True)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def get_allOrders(event, context):
    """Gibt alle Bestellungen zurück."""
    with session_scope() as session:
        orders = session.query(Order).with_entities(Order.idorders, Order.order_date, Order.fksupplier). \
            order_by(Order.order_date)

        # Serialize the queryset
        result = OrderSchema().dump(orders, many=True)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def getCharge(event, context):
    """Gibt eine Charge mit einer bestimmten ID zurück."""
    params = event["pathParameters"]
    id = params["id"]

    with session_scope() as session:
        charge = session.query(Charge).filter(Charge.idcharges == id).first()

        if charge is None:
            return {
                "statusCode": 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                "body": json.dumps({"message": "Die Charge mit der ID " + str(id) + " existiert nicht."}),
            }

        if charge.material.art == 'T-Shirt':
            # Serialize the queryset
            result = ChargeSchema(exclude=['chargeColor']).dump(charge)

        elif charge.material.art == 'Farbe':
            result = ChargeSchema(exclude=['chargeShirt']).dump(charge)
        else:
            result = ChargeSchema(exclude=['chargeShirt', 'chargeColor']).dump(charge)

    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def createCharge(event, context):
    """Anlage oder Änderung einer Charge."""
    logger.info(event)

    body = json.loads(event.get('body'))
    logger.info(body)

    with session_scope() as session:
        charge_new = ChargeSchema().load(body, session=session)
        session.add(charge_new)
        session.commit()

        # Serialize the queryset
        result = ChargeSchema().dump(charge_new)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def getMaterial(event, context):
    """Gibt ein Material mit einer bestimmten ID zurück."""
    params = event["pathParameters"]
    id = params["id"]

    with session_scope() as session:
        material = session.query(Material).filter(Material.idmaterials == id).first()
        if material is None:
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps({"message": "[BadRequest] Ungültige Material-ID übergeben."})
            }

        # Serialize the queryset
        result = MaterialSchema().dump(material)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def get_allMaterials(event, context):
    """Gibt alle Materialien zurück."""
    with session_scope() as session:
        materials = session.query(Material).with_entities(Material.idmaterials, Material.name, Material.art). \
            order_by(Material.idmaterials)

        # Serialize the queryset
        result = MaterialSchema().dump(materials, many=True)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def createMaterial(event, context):
    """Anlage oder Änderung eines Materials."""
    logger.info(event)

    body = json.loads(event.get('body'))
    logger.info(body)

    with session_scope() as session:
        material_new = MaterialSchema().load(body, session=session)
        session.add(material_new)
        session.commit()

        # Serialize the queryset
        result = MaterialSchema().dump(material_new)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def getSupplier(event, context):
    """Gibt einen Lieferanten mit einer bestimmten ID zurück."""
    params = event["pathParameters"]
    id = params["id"]

    with session_scope() as session:
        supplier = session.query(Supplier).filter(Supplier.idsuppliers == id).first()
        if supplier is None:
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps({"message": "[BadRequest] Ungültige Lieferanten-ID übergeben."})
            }

        # Serialize the queryset
        result = SupplierSchema().dump(supplier)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def get_allSuppliers(event, context):
    """Gibt alle Lieferanten zurück."""
    with session_scope() as session:
        suppliers = session.query(Supplier).with_entities(Supplier.idsuppliers, Supplier.name, Supplier.ort). \
            order_by(Supplier.idsuppliers)

        # Serialize the queryset
        result = SupplierSchema().dump(suppliers, many=True)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }


def createSupplier(event, context):
    """Anlage oder Änderung eines lieferanten."""
    logger.info(event)

    body = json.loads(event.get('body'))
    logger.info(body)

    with session_scope() as session:
        supplier_new = SupplierSchema().load(body, session=session)
        session.add(supplier_new)
        session.commit()

        # Serialize the queryset
        result = SupplierSchema().dump(supplier_new)
    return {
        "statusCode": 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps(result),
    }