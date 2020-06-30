from sqlalchemy.dialects.mysql import TINYINT, DOUBLE
from marshmallow_sqlalchemy.fields import Nested
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from sqlalchemy import Column, Integer, DateTime, String, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime as dt
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
dtf = "%Y/%m/%d %H:%M:%S"


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
    capturer = Column(String)
    fksuppliers = Column(Integer, ForeignKey('suppliers.idsuppliers'))

    receivingPos = relationship("ReceivingPosition", back_populates="receiving")
    supplier = relationship("Supplier", back_populates="receivings")

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
    order_date = Column(DateTime, default=dt.now)
    capturer = Column(String(10))
    state = Column(String(7))
    fksuppliers = Column(Integer, ForeignKey('suppliers.idsuppliers'))

    orderPos = relationship("OrderPosition", back_populates="order")
    supplier = relationship("Supplier", back_populates="orders")


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
    idcharges = Column(Integer, primary_key=True)
    fkmaterials = Column(Integer, ForeignKey('materials.idmaterials'), nullable=False)
    date = Column(DateTime, default=dt.now)

    material = relationship("Material", back_populates="charges")
    chargeShirt = relationship("ChargeShirt", uselist=False , back_populates="charge")
    chargeColor = relationship("ChargeColor", uselist=False, back_populates="charge")


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


class Supplier(Base):
    __tablename__ = 'suppliers'
    idsuppliers = Column(Integer, primary_key=True)
    name = Column(String(100))
    address = Column(String(255))
    postcode = Column(String(20))
    ort = Column(String(45))
    contact = Column(String(100))
    phone = Column(String(45))
    fax = Column(String(45))
    email = Column(String(45))

    receivings = relationship("Receiving", back_populates="supplier")
    orders = relationship("Order", back_populates="supplier")


#SCHEMA
class MaterialSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Material
        include_fk = True
        load_instance = True


class ReceivingPositionSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = ReceivingPosition
        include_fk = True
        load_instance = True

    material = Nested(lambda: MaterialSchema(), dump_only=True)


class ReceivingSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Receiving
        include_fk = True
        load_instance = True
        datetimeformat = dtf

    receivingPos = Nested(ReceivingPositionSchema(), many=True)
    supplier = Nested(lambda: SupplierSchema(), dump_only=True,
                      exclude=('address', 'postcode', 'contact', 'email', 'phone', 'fax', ))


class OrderPositionSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = OrderPosition
        load_instance = True

    material = Nested(lambda: MaterialSchema(), dump_only=True)


class OrderSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Order
        include_fk = True
        load_instance = True
        datetimeformat = dtf

    orderPos = Nested(OrderPositionSchema(), many=True)
    supplier = Nested(lambda: SupplierSchema(), dump_only=True,
                      exclude=('address', 'postcode', 'contact', 'email', 'phone', 'fax',))


class ChargeShirtSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = ChargeShirt
        load_instance = True


class ChargeColorSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = ChargeColor
        load_instance = True


class ChargeSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Charge
        include_fk = True
        load_instance = True

    material = Nested(lambda: MaterialSchema(), dump_only=True)
    chargeShirt = Nested(ChargeShirtSchema())
    chargeColor = Nested(ChargeColorSchema())


class SupplierSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Supplier
        include_fk = True
        load_instance = True

