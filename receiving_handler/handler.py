import simplejson as json
import sys
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import ast
from contextlib import contextmanager
from schema import Material, Receiving, ReceivingPosition, Order, OrderPosition, Charge, ChargeShirt, ChargeColor, \
    MaterialSchema, ReceivingSchema, ReceivingPositionSchema, OrderSchema, OrderPositionSchema, ChargeSchema, \
    ChargeShirtSchema, ChargeColorSchema

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    params = event["pathParameters"]
    id = params["id"]

    with session_scope() as session:
        receiving = session.query(Receiving).filter(Receiving.id==id).first()
        if receiving is None:
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps("[BadRequest] Ungültige Wareneingang-ID übergeben.")
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
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
    logger.info(body)

    # Ermitteln der Bestellung aus der Anfrage
    fkorders = body.get('fkorders')

    with session_scope() as session:
        # Erzeugen eines neuen Wareneingangs
        receiving_new = Receiving()
        session.add(receiving_new)
        session.commit()

        receiving_id = receiving_new.id

        # Übernahme der Order Positionen in den Wareneingang
        orderPositions = session.query(OrderPosition).filter(OrderPosition.fkorders == fkorders)

        idx = 1
        for position in orderPositions:
            receivingPos = ReceivingPosition(fkreceivings=receiving_id, position=idx, fkmaterials=position.fkmaterials,
                                     quantity=position.quantity, checked=0, price=0, fkordersPos=fkorders)
            idx = idx +1
            session.add(receivingPos)
            session.commit()

        # Serialize the queryset
        receiving = session.query(Receiving).filter(Receiving.id == receiving_id).first()
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


def get_allReceiving(event, context):
    with session_scope() as session:
        receivings = session.query(Receiving).with_entities(Receiving.id, Receiving.receiving_date).\
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
    params = event["pathParameters"]
    id = params["id"]

    with session_scope() as session:
        order = session.query(Order).filter(Order.idorders==id).first()
        if order is None:
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps("[BadRequest] Ungültige Wareneingang-ID übergeben.")
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
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
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


def get_allOrders(event, context):
    with session_scope() as session:
        orders = session.query(Order).with_entities(Order.idorders, Order.order_date, Order.fksupplier).\
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
    params = event["pathParameters"]
    id = params["id"]

    with session_scope() as session:
        charge = session.query(Charge).filter(Charge.idcharges==id).first()

        if charge is None:
            return {
                "statusCode": 400,
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                "body": "Die Charge mit der ID " + str(id) + " existiert nicht.",
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
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
    logger.info(body)

    with session_scope() as session:
        charge_new = ChargeSchema().load(body, session=session)

        chargeShirt = None
        chargeColor = None

        if charge_new.get('chargeColor') is not None:
            data = charge_new.get('chargeColor')
            chargeColor = ChargeColorSchema().load(data=data, session=session)
        elif charge_new.get('chargeShirt') is not None:
            chargeShirt = ChargeShirtSchema().load(data=charge_new.get('chargeShirt'), session=session)

        if (chargeShirt is not None) or (chargeColor is not None):
            chargeHead = Charge(fkmaterials=charge_new.get('fk_materials'))
            session.add(chargeHead)
            session.commit()

            if chargeShirt is not None:
                chargeShirt['fkcharges'] = chargeHead.idcharges
                session.add(chargeShirt)
            elif  chargeColor is not None:
                chargeColor['fkcharges'] = chargeHead.idcharges
                session.add(chargeColor)

            session.commit()

        charge_new = session.query(Charge).filter(Charge.idcharges==chargeHead.idcharges)

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