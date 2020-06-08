import sys
import logging
import os
import pymysql
import json
from datetime import datetime
from inventory import Inventory
from stockentry import StockEntry

rds_host = os.environ['DB_HOST']
name = os.environ['DB_USER']
password = os.environ['DB_PASSWORD']
db_name = os.environ['DB_NAME']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")


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