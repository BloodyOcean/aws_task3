import logging

import load
from entities import DbServer
from helpers import arguments_parse

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT, filename='app.log')


def main():
    vals = arguments_parse()

    if vals.create:
        load.create_db()

    if vals.peoplecards is not None:
        load.load_people_cards(vals.peoplecards)

    if vals.transactions:
        load.load_transactions()


if __name__ == '__main__':
    main()
