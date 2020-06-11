import sys
import logging
import os
import simplejson as json
from datetime import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, relationship
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from marshmallow_sqlalchemy.fields import Nested
from sqlalchemy.dialects.mysql import TINYINT, DOUBLE

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

Base = declarative_base()
engine = create_engine('mysql+mysqlconnector://' + name + ':' + password + '@' + rds_host + '/' + db_name, echo=True)

try:
    # create a session
    Session = sessionmaker(bind=engine)
    session = Session()
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    sys.exit()


class Inventory(Base):
    __tablename__ = 'inventory'
    fkplaces = Column(Integer, ForeignKey('places.idplaces'),primary_key=True)
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

    place = relationship("Place",  back_populates="stocks")


class Material(Base):
    __tablename__ = 'materials'
    idmaterials = Column(Integer, primary_key=True)
    name = Column(String(20), nullable=False)
    description = Column(String(45))
    size = Column(DOUBLE)
    measure = Column(String(10))
    minStock = Column(Integer)
    art = Column(String(45), nullable=False)

    inventories = relationship("Inventory",  back_populates="material")


class StockEntry(Base):
    __tablename__ = 'stockEntries'
    idstockEntries = Column(Integer, primary_key=True)
    fkplaces = Column(Integer, ForeignKey('places.idplaces'), nullable=False)
    fkmaterials = Column(Integer, ForeignKey('materials.idmaterials'), nullable=False)
    fkorders = Column(Integer)
    opened = Column(TINYINT,  nullable=False)
    quantity = Column(Integer,  nullable=False)
    booking_date = Column(DateTime,  nullable=False, default=dt.now)


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
        load_instance = True


def getInventory(event, context):
    inventory = session.query(Inventory).order_by(Inventory.fkplaces)

    # Serialize the queryset
    result = InventorySchema().dump(inventory, many=True)

    return {
        "statusCode": 200,
        "body": json.dumps(result, use_decimal=True),
    }


def bookToStock(event, context):
    logger.info(event)

    stockEntry_new = StockEntrySchema().load(event, session=session)

    session.add(stockEntry_new)
    session.commit()

    # Serialize the queryset
    result = StockEntrySchema().dump(stockEntry_new)

    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }