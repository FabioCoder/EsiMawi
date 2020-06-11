import json
import sys
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, DateTime
from sqlalchemy.orm import sessionmaker
from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field
import ast
from contextlib import contextmanager

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


class Receiving(Base):
    __tablename__ = 'receivings'
    id = Column(Integer, primary_key=True)
    receiving_date = Column(DateTime)


class ReceivingSchema(SQLAlchemySchema):
    class Meta:
        model = Receiving
        load_instance = True

    id = auto_field()
    receiving_date = auto_field()


def get(event, context):
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


def create(event, context):
    logger.info(event)

    body = ast.literal_eval(event.get('body'))
    logger.info(body)

    with session_scope() as session:
        receiving_new = ReceivingSchema().load(body, session=session)
        session.add(receiving_new)
        session.commit()
        # Serialize the queryset
        result = ReceivingSchema().dump(receiving_new)
    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }

def get_all(event, context):
    with session_scope() as session:
        receivings = session.query(Receiving).order_by(Receiving.receiving_date)

        # Serialize the queryset
        result = ReceivingSchema().dump(receivings, many=True)
    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }
