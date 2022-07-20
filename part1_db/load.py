import datetime
import logging
import random

from entities import Person, Card, DbServer, Transaction
from helpers import SessionHelper


def create_db():
    ds = DbServer()
    ds.open_connection()


def load_people_cards(n: int):
    """
    Generates n people and loads them to db.
    Generates 1 or 0 cards for each person existing in db
    :param n: number of new people
    :return:
    """
    people = []
    cards = []
    server = DbServer()
    server.open_connection()
    existing_people = server.get_people()
    for i in range(n):
        person = Person()
        people.append(person)

    logging.info(f'People created. Size: {len(people)}')

    for person in existing_people:
        new_cards = random.randint(0, 1)
        if new_cards:
            card = Card(holder_id=person.customer_id)
            cards.append(card)

    logging.info(f'Cards for people from db created. Size: {len(cards)}')

    session_helper = SessionHelper()
    session_helper.save_max_created_on_card(cards[0].created_on if len(cards) > 0 else None)
    server.load_records(people + cards)


def load_transactions():
    """
    Creates 1-5 transaction for each valid card from db.
    Valid card means that valid_until > datetime.now()
    :return:
    """
    server = DbServer()

    session_helper = SessionHelper()
    last_session = session_helper.get_last_session()

    server.open_connection()
    transactions = []
    existing_cards = server.get_cards()

    for card in existing_cards:
        if card.isvalid():
            for i in range(random.randint(0, 1)):
                transaction = Transaction(card_no=card.card_no, last_session=last_session)
                card.amount += transaction.value
                card.last_used_on = datetime.datetime.now()
                transactions.append(transaction)

    logging.info(f'Transaction created, size: {len(transactions)}')

    logging.info('Sorting created transactions in descending order')
    transactions.sort(key=lambda x: x.transaction_time)

    session_helper = SessionHelper()

    session_helper.save_max_transaction_time(transactions[0].transaction_time if len(transactions) > 0 else None)
    logging.info(f'Max last transaction time saved to ../appconfig.ini, values={transactions[0].transaction_time if len(transactions) > 0 else None}')

    logging.info(f'Loading transactions to the server. Size {len(transactions)}')
    server.load_records(transactions)
