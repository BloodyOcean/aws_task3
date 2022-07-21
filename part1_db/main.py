import logging
from pathlib import Path
import load
from entities import DbServer
from helpers import arguments_parse

FORMAT = '%(asctime)s %(message)s'
working_path = str(Path(__file__).resolve().parent)
logging.basicConfig(format=FORMAT, filename=working_path+'app.log')


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
