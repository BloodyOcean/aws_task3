import csv


def read_credentials() -> list:
    """
    Reads access key and password
    for amazon from .csv file
    :return:
    """
    file = open('~/aws_task3/Administrator_accessKeys.csv')
    csvreader = csv.reader(file)
    header = []
    header = next(csvreader)
    rows = []
    for row in csvreader:
        rows.append(row)
    return rows[0]
