import datetime
import logging
import random
import sys
import uuid

import mimesis
from mimesis.enums import Locale
from sqlalchemy import create_engine, String, Column, DateTime, Integer, Float, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, Session, session
from sqlalchemy.sql.expression import func, select
from sqlalchemy_utils import database_exists, create_database

from helpers import ConfigReader, SessionHelper

Base = declarative_base()


class Person(Base):
    __tablename__ = "People"
    cards = relationship("Card")
    customer_id = Column(String(90), primary_key=True, nullable=False, unique=True)
    id_code = Column(Integer, nullable=False, unique=True)
    name = Column(String(90), nullable=False)
    second_name = Column(String(90))
    surname = Column(String(30), nullable=False)
    language = Column(String(90), nullable=False)
    city = Column(String(90), nullable=False)
    phone = Column(String(90), nullable=False)
    birth_date = Column(DateTime, nullable=False)

    def __repr__(self):
        return f"[{self.customer_id}] {self.name} {self.surname} {self.second_name} {self.id_code} {self.birth_date}"

    def __init__(self):
        min_year_birthday = 1960
        self.customer_id = uuid.uuid4()
        self.id_code = int(mimesis.Person(Locale.EN).identifier(mask='########'))
        self.name = mimesis.Person(Locale.EN).name()
        self.surname = mimesis.Person(Locale.EN).surname()
        self.second_name = mimesis.Person(Locale.EN).last_name()
        self.language = mimesis.Person(Locale.EN).language()
        self.phone = mimesis.Person(Locale.EN).telephone()
        self.birth_date = mimesis.Datetime().datetime(min_year_birthday, datetime.datetime.now().year)
        self.city = mimesis.Address().city()


class Card(Base):
    __tablename__ = "Cards"
    transactions = relationship("Transaction")
    card_id = Column(String(90), primary_key=True, nullable=False, unique=True)
    holder_id = Column(String(90), ForeignKey("People.customer_id"), nullable=False)
    card_no = Column(String(30), nullable=False, unique=True)
    currency = Column(String(30), nullable=False)
    amount = Column(Float(), nullable=False, default=0)
    valid_until = Column(DateTime, nullable=False)
    created_on = Column(DateTime, nullable=False)
    last_used_on = Column(DateTime, default=None)

    def __repr__(self):
        return f"{self.card_no} {self.created_on} {self.valid_until}"

    def isvalid(self) -> bool:
        current_date = datetime.datetime.utcnow()
        return self.valid_until > current_date

    def __init__(self, holder_id: int):
        self.card_id = uuid.uuid4()
        self.holder_id = holder_id
        self.currency = mimesis.Finance().currency_iso_code()
        self.card_no = mimesis.Payment().credit_card_number()
        self.valid_until = datetime.datetime.strptime(mimesis.Payment().credit_card_expiration_date(),
                                                      '%m/%y')
        self.created_on = datetime.datetime.utcnow()


class Transaction(Base):
    __tablename__ = "Transaction"
    transaction_id = Column(String(90), primary_key=True, nullable=False, unique=True)
    card_no = Column(String(30), ForeignKey("Cards.card_no"), nullable=False)
    transaction_time = Column(DateTime, nullable=False)
    comment = Column(String(256))
    value = Column(Float(30))

    def __repr__(self):
        return f"{self.transaction_id} {self.card_no} {self.transaction_time} {self.value}"

    def __init__(self, card_no: int, last_session: str):
        self.transaction_id = uuid.uuid4()
        self.comment = 'who: ' + mimesis.Person(Locale.EN).name()
        self.value = random.randint(-1 * sys.maxsize, sys.maxsize)
        self.card_no = card_no

        current_session = datetime.datetime.utcnow()
        last_session = datetime.datetime.strptime(last_session, '%Y-%m-%d %H:%M:%S.%f')

        self.transaction_time = last_session + datetime.timedelta(
            seconds=random.randint(0, int((current_session - last_session).total_seconds())),
        )


class DbServer:
    def __init__(self):
        self.engine = None
        self.session = None
        self.config_helper = ConfigReader()
        self.connection_string = self.config_helper.get_value_from_s3_bucket('task3-files',
                                                                             'configs/dbconf.ini',
                                                                             'DbInfo',
                                                                             'connection_string_common')

    def open_connection(self):
        """
        Tries to open connection to existed db by connection string from config file
        :return:
        """
        logging.info('Trying to create engine')
        try:
            self.engine = create_engine(self.connection_string, echo=True, future=True)
            if not database_exists(self.engine.url):
                logging.info('Such database doesnt exist, creating new')
                create_database(self.engine.url)
                logging.info('Database created successfully')

            logging.info('Trying to create migration')
            Base.metadata.create_all(self.engine)
            logging.info('Migration created successfully')

            self.session = Session(self.engine)

        except Exception:
            logging.error('Cant connect or migration fails')
            raise ValueError('Invalid db connection')

    def load_records(self, lst: list):
        """
        Tries to load all the records to existing database
        :param lst: extended list of each model
        :return:
        """
        logging.info('Starting new session')
        logging.info(f'Adding models to db size: {len(lst)}')
        self.session.add_all(lst)
        logging.info('Committing')
        self.session.commit()
        logging.info(f'Committed successfully, size: {len(lst)}')
        logging.info('Working with session finished successfully')

    def get_people(self):
        """
        Gets all the people from database
        :return:
        """
        logging.info('Trying to get people from database')
        res = self.session.query(Person).all()
        logging.info(f'Records from people table retrieved')
        return res

    def get_cards(self):
        """
        Gets all the cards from database
        :return:
        """
        logging.info('Trying to get cards from database')
        res = self.session.query(Card).all()
        logging.info(f'Records from cards table retrieved')
        return res

    def __del__(self):
        self.session.close()
        self.engine.dispose()
