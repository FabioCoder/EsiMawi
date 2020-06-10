import sys
import logging
import os
import json
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, relationship
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field
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


class Place(Base):
    __tablename__ = 'places'
    idplaces = Column(Integer, primary_key=True)
    description = Column(String(45))
    fkstocks = Column(Integer, ForeignKey('stocks.idstocks'), nullable=False)
    inventories = relationship("Inventory")


class Stock(Base):
    __tablename__ = 'stocks'
    idstocks = Column(Integer, primary_key=True)
    description = Column(String(45))
    places = relationship("Place")


class Material(Base):
    __tablename__ = 'materials'
    idmaterials = Column(Integer, primary_key=True)
    name = Column(String(20), nullable=False)
    description = Column(String(45))
    size = Column(DOUBLE)
    measure = Column(String(10))
    minStock = Column(Integer)
    art = Column(String(45), nullable=False)
    inventories = relationship("Inventory")



def getInventory(event, context):
    with conn.cursor(cursor=pymysql.cursors.DictCursor) as cur:
        sql = """SELECT inventory.fkplaces, places.description, inventory.fkmaterials, materials.name as materialName, 
                materials.size, inventory.quantity, (materials.size * inventory.quantity) as quantity_in_measure,
                materials.measure, materials.minStock, inventory.opened
                FROM innodb.inventory as inventory
                join innodb.materials as materials on inventory.fkmaterials = materials.idmaterials
                join innodb.places as places on inventory.fkplaces = places.idplaces
            """
        cur.execute(sql)
        rows = cur.fetchall()

        inventory = []
        for row in rows:
            i = Inventory.rowToObject(row)
            logger.info(i.toJson())
            inventory.append(i.toJson())

    conn.commit()

    return {
        "statusCode": 200,
        "body": json.dumps(inventory),
    }


def bookToStock(event, context):
    logger.info(event)

    try:
        se = StockEntry.eventToObject(event)
    except KeyError:
        return {
            'statusCode': 400,
            'body': json.dumps("[BadRequest] Ungültige Lagerbuchung.")
        }

    with conn.cursor(cursor=pymysql.cursors.DictCursor) as cur:
        booking_date = datetime.now()
        sql = "INSERT INTO `stockEntries` (`fkplaces`, `fkmaterials`, `opened`, `quantity`, `booking_date`) " \
              "VALUES (%s,%s,%s, %s, %s)"
        cur.execute(sql, (se.getFkPlaces(), se.getFkMaterials(), se.getOpened(), se.getQuantity(), booking_date))
    conn.commit()

    return {
        "statusCode": 200,
        "body": json.dumps(se.toJson()),
    }