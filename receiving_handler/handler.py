import json
import sys
import logging
import os
import pymysql
from receiving import Receiving


# import requests

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


def get(event, context):

    params = event["pathParameters"]
    id = params["id"]
    logger.info('id:' + id)

    if not id.isdigit():
        logger.info('Ungültige Wareneingang-ID.')
        return {
            'statusCode': 400,
            'body': json.dumps("[BadRequest] Ungültige Wareneingang-ID übergeben.")
        }

    with conn.cursor(cursor=pymysql.cursors.DictCursor) as cur:
        cur.execute("select * from receivings r where r.id = " + id)

        count = cur.rowcount
        if count > 0:
            row = cur.fetchone()
            r = Receiving.rowToObject(row)
            logger.info(r.toJson())
    conn.commit()

    if count <= 0:
        logger.info('Wareneingang nicht gefunden.')
        return {
            'statusCode': 400,
            'body': json.dumps("[BadRequest] Ungültige Wareneingang-ID übergeben.")
        }

    return {
        "statusCode": 200,
        "body": json.dumps(r.toJson()),
    }


def create(event, context):
    logger.info(event)

    try:
        r = Receiving.eventToObject(event)
    except KeyError:
        return {
            'statusCode': 400,
            'body': json.dumps("[BadRequest] Ungültiger Wareneingang.")
        }

    with conn.cursor(cursor=pymysql.cursors.DictCursor) as cur:
        sql = "INSERT INTO `receivings` (`receiving_date`) VALUES (%s)"
        cur.execute(sql, (r.getReceivingDate()))
        r.id = cur.lastrowid
    conn.commit()

    return {
        "statusCode": 200,
        "body": json.dumps(r.toJson()),
    }


def get_all(event, context):
    with conn.cursor(cursor=pymysql.cursors.DictCursor) as cur:
        cur.execute("select * from receivings")
        rows = cur.fetchall()

        receivings = []
        for row in rows:
            logger.info(row["id"])
            logger.info(row["receiving_date"])
            r = Receiving.rowToObject(row)
            logger.info(r.toJson())
            receivings.append(r.toJson())

    conn.commit()

    return {
        "statusCode": 200,
        "body": json.dumps(receivings),
    }