from marshmallow_sqlalchemy import SQLAlchemyAutoSchema, auto_field
from marshmallow_sqlalchemy.fields import Nested
from marshmallow import Schema, fields
from sqlalchemy.dialects.mysql import TINYINT, DOUBLE
from datetime import datetime as dt
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# ORM
class Inventory(Base):
    __tablename__ = 'inventory'
    fkplaces = Column(Integer, ForeignKey('places.idplaces'), primary_key=True)
    fkmaterials = Column(Integer, ForeignKey('materials.idmaterials'), primary_key=True)
    opened = Column(TINYINT, primary_key=True)
    quantity = Column(Integer)

    material = relationship("Material", back_populates="inventories")
    place = relationship("Place", back_populates="inventories")


class Place(Base):
    __tablename__ = 'places'
    idplaces = Column(Integer, primary_key=True)
    description = Column(String(45))
    fkstocks = Column(Integer, ForeignKey('stocks.idstocks'), nullable=False)

    inventories = relationship("Inventory", back_populates="place")
    stocks = relationship("Stock", back_populates="place")


class Stock(Base):
    __tablename__ = 'stocks'
    idstocks = Column(Integer, primary_key=True)
    description = Column(String(45))

    place = relationship("Place", back_populates="stocks")


class Material(Base):
    __tablename__ = 'materials'
    idmaterials = Column(Integer, primary_key=True)
    name = Column(String(20), nullable=False)
    description = Column(String(45))
    size = Column(DOUBLE)
    measure = Column(String(10))
    minStock = Column(Integer)
    art = Column(String(45), nullable=False)

    inventories = relationship("Inventory", back_populates="material")


class StockEntry(Base):
    __tablename__ = 'stockEntries'
    idstockEntries = Column(Integer, primary_key=True)
    fkplaces = Column(Integer, ForeignKey('places.idplaces'), nullable=False)
    fkmaterials = Column(Integer, ForeignKey('materials.idmaterials'), nullable=False)
    productionOrderNr = Column(String)
    opened = Column(TINYINT, nullable=False)
    quantity = Column(Integer, nullable=False)
    booking_date = Column(DateTime, nullable=False, default=dt.now)


class GoodsOrder(Base):
    __tablename__ = 'goodsOrders'
    idgoodsOrders = Column(Integer, primary_key=True)
    fkmaterials = Column(Integer, ForeignKey('materials.idmaterials'))
    creation_date = Column(DateTime, nullable=False, default=dt.now)

    goodsOrderPos = relationship("GoodsOrderPosition", back_populates="goodsOrder")


class GoodsOrderPosition(Base):
    __tablename__ = 'goodsOrdersPos'
    fkgoodsOrders = Column(Integer, ForeignKey('goodsOrders.idgoodsOrders'), primary_key=True)
    productionOrderNr = Column(String, primary_key=True)
    quantity = Column(Integer)
    done = Column(TINYINT)

    goodsOrder = relationship("GoodsOrder", back_populates="goodsOrderPos")

# SCHEMA
class MaterialSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Material


class PlaceSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Place


class InventorySchema(SQLAlchemyAutoSchema):
    class Meta:
        model = Inventory
        include_fk = True

    # Override materials field to use a nested representation rather than pks
    material = Nested(lambda: MaterialSchema(), dump_only=True)
    place = Nested(lambda: PlaceSchema(), dump_only=True)


class StockEntrySchema(SQLAlchemyAutoSchema):
    class Meta:
        model = StockEntry
        include_fk = True


class BookMaterialSchema(Schema):
    fkmaterials = fields.Integer(required=True)
    quantity = fields.Integer(required=True)
    fkplaces = fields.Integer(required=True)
    opened = fields.Boolean(default=0)


class BookProductToStockSchema(Schema):
    productionOrderNr = fields.String(required=True)
    fkplaces = fields.Integer(required=True)


class BookProductFromStockSchema(Schema):
    productionOrderNr = fields.String(required=True)
    fkplaces = fields.Integer(required=True)
    fkgoodsOrders = fields.Integer(required=True)


class ReservationOrderPositionSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = GoodsOrderPosition
        include_fk = True
        load_instance = True
        exclude = ["done"]


class ReservationOrderSchema(SQLAlchemyAutoSchema):
    class Meta:
        model = GoodsOrder
        include_fk = True
        load_instance = True
        exclude = ["idgoodsOrders"]

    goodsOrderPos = Nested(ReservationOrderPositionSchema(), many=True)


class GoodsOrderSchema(Schema):
    productionOrderNr = fields.String()
    fkmaterials = fields.Integer()
    quantity = fields.Integer()
