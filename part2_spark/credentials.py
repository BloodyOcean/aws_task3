import csv
from pathlib import Path


def read_credentials() -> list:
    """
    Reads access key and password
    for amazon from .csv file
    :return:
    """

    working_path = str(Path(__file__).resolve().parent.parent)
    file = open(working_path + '/Administrator_accessKeys.csv')
    csvreader = csv.reader(file)
    header = []
    header = next(csvreader)
    rows = []
    for row in csvreader:
        rows.append(row)
    return rows[0]
