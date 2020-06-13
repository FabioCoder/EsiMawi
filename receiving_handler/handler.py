import simplejson as json
import sys
import logging
import os
from datetime import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, DateTime, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
import ast
from contextlib import contextmanager
from sqlalchemy.dialects.mysql import TINYINT, DOUBLE
from marshmallow_sqlalchemy.fields import Nested

logger = logging.getLogger()
logger.setLevel(logging.INFO)

rds_host = os.environ['DB_HOST']
name = os.environ['DB_USER']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']

Base = declarative_base()
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

#ORM
class Material(Base):
    __tablename__ = 'materials'
    idmaterials = Column(Integer, primary_key=True)
    name = Column(String(20), nullable=False)
    description = Column(String(45))
    size = Column(DOUBLE)
    measure = Column(String(10))
    minStock = Column(Integer)
    art = Column(String(45), nullable=False)

    receivingPos = relationship("ReceivingPosition", back_populates="material")
    orderPos = relationship("OrderPosition", back_populates="material")
    charges = relationship("Charge", back_populates="material")

class Receiving(Base):
    __tablename__ = 'receivings'
    id = Column(Integer, primary_key=True)
    receiving_date = Column(DateTime, default=dt.now)

    receivingPos = relationship("ReceivingPosition", back_populates="receiving")


class ReceivingPosition(Base):
    __tablename__ = 'receivingsPos'
    fkreceivings = Column(Integer, ForeignKey('receivings.id'), primary_key=True)
    position = Column(Integer, primary_key=True)
    fkmaterials = Column(Integer,  ForeignKey('materials.idmaterials'), nullable=False)
    quantity = Column(Integer, nullable=False)
    checked = Column(TINYINT)
    price = Column(DOUBLE)
    fkordersPos = Column(Integer, ForeignKey('ordersPos.idordersPos'))

    material = relationship("Material", back_populates="receivingPos")
    receiving = relationship("Receiving", back_populates="receivingPos")


class Order(Base):
    __tablename__ = 'orders'
    idorders = Column(Integer, primary_key=True)
    order_date = Column(DateTime, default=dt, nullable=False)
    capturer = Column(String(10))
    state = Column(String(7))
    fksupplier = Column(Integer)

    orderPos = relationship("OrderPosition", back_populates="order")


class OrderPosition(Base):
    __tablename__ = 'ordersPos'
    idordersPos = Column(Integer, primary_key=True)
    fkorders = Column(Integer, ForeignKey('orders.idorders'), nullable=False)
    position = Column(Integer, nullable=False)
    fkmaterials = Column(Integer,  ForeignKey('materials.idmaterials'), nullable=False)
    quantity = Column(Integer, nullable=False)

    material = relationship("Material", back_populates="orderPos")
    order = relationship("Order", back_populates="orderPos")


class Charge(Base):
    __tablename__ = 'charges'
    idcharges = Column(Integer, ForeignKey('chargesShirt.fkcharges'), primary_key=True)
    fkmaterials = Column(Integer, ForeignKey('materials.idmaterials'), nullable=False)
    date = Column(DateTime, default=dt.now)

    material = relationship("Material", back_populates="charges")
    chargeShirt = relationship("ChargeShirt", back_populates="charge")
    chargeColor = relationship("ChargeColor", back_populates="charge")


class ChargeShirt(Base):
    __tablename__ = 'chargesShirt'
    fkcharges = Column(Integer, ForeignKey('charges.idcharges'), primary_key=True)
    whiteness = Column(Integer, nullable=False)
    absorbency = Column(DOUBLE, nullable=False)

    charge = relationship("Charge", back_populates="chargeShirt")


class ChargeColor(Base):
    __tablename__ = 'chargesColor'
    fkcharges = Column(Integer, ForeignKey('charges.idcharges'), primary_key=True)
    ppml = Column(Integer, nullable=False)
    viscosity = Column(DOUBLE, nullable=False)
    deltaE = Column(DOUBLE, nullable=False)

    charge = relationship("Charge", back_populates="chargeColor")


#SCHEMA
class MaterialSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Material


class ReceivingPositionSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = ReceivingPosition
        include_fk = True

    material = Nested(lambda: MaterialSchema(), dump_only=True)


class ReceivingSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Receiving
        include_fk = True

    receivingPos = Nested(ReceivingPositionSchema(), many=True)


class OrderPositionSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = OrderPosition
        load_instance = True

    material = Nested(lambda: MaterialSchema(), dump_only=True)


class OrderSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Order
        load_instance = True

    orderPos = Nested(OrderPositionSchema(), many=True)


class ChargeShirtSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = ChargeShirt


class ChargeColorSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = ChargeColor


class ChargeSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Charge
        include_fk = True

    material = Nested(lambda: MaterialSchema(), dump_only=True)
    chargeShirt = Nested(lambda:ChargeShirtSchema())
    chargeColor = Nested(lambda:ChargeColorSchema())

def getReceiving(event, context):
    params = event["pathParameters"]
    id = params["id"]

    with session_scope() as session:
        receiving = session.query(Receiving).filter(Receiving.id==id).first()
        if receiving is None:
            return {
                'statusCode': 400,
                'body': json.dumps("[BadRequest] Ungültige Wareneingang-ID übergeben.")
            }

        # Serialize the queryset
        result = ReceivingSchema().dump(receiving)
    return {
        "statusCode": 200,
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
                'body': json.dumps("[BadRequest] Ungültige Wareneingang-ID übergeben.")
            }

        # Serialize the queryset
        result = OrderSchema().dump(order)
    return {
        "statusCode": 200,
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
        "body": json.dumps(result),
    }


def getCharge(event, context):
    params = event["pathParameters"]
    id = params["id"]

    with session_scope() as session:
        charge = session.query(Charge).filter(Charge.idcharges==id).first()

        if charge.material.art == 'Shirt':
            # Serialize the queryset
            result = ChargeSchema(exclude=['chargeColor']).dump(charge)

        elif charge.material.art == 'Farbe':
            result = ChargeSchema(exclude=['chargeShirt']).dump(charge)
        else:
            result = ChargeSchema(exclude=['chargeShirt', 'chargeColor']).dump(charge)

    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }
