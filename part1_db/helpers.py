import configparser
import argparse
import datetime
import os

from manager import BotoManager


class SessionHelper:

    def __del__(self):
        self.save_last_session()

    def __init__(self, bucket_name: str = 'task3-files', key: str = 'configs/appconfig.ini', section: str = 'Session'):
        self.bucket_name = bucket_name
        self.key = key
        self.section = section
        self.config_helper = ConfigReader()
        last_session = self.config_helper.get_value_from_s3_bucket(bucket_name, key, self.section, 'last_session')
        if last_session == '':
            self.save_last_session()

    def save_max_created_on_card(self, max_created, val: str = 'max_created_on'):
        if max_created is not None:
            self.config_helper.set_value_to_s3_bucket(self.bucket_name,
                                                      self.key,
                                                      self.section,
                                                      val,
                                                      str(max_created))

    def save_max_transaction_time(self, max_time, val: str = 'max_transaction_time'):
        if max_time is not None:
            self.config_helper.set_value_to_s3_bucket(self.bucket_name,
                                                      self.key,
                                                      self.section,
                                                      val,
                                                      str(max_time))

    def save_last_session(self):
        self.config_helper.set_value_to_s3_bucket(self.bucket_name,
                                                  self.key,
                                                  self.section,
                                                  'last_session',
                                                  str(datetime.datetime.utcnow()))

    def get_last_session(self):
        last_session = self.config_helper.get_value_from_s3_bucket(self.bucket_name,
                                                                   self.key,
                                                                   self.section,
                                                                   'last_session')
        if last_session == '':
            self.save_last_session()
            return datetime.datetime.utcnow()
        else:
            return last_session


class ConfigReader:
    def __init__(self):
        self.old_text = None
        self.boto = BotoManager()
        self.config = configparser.ConfigParser()

    def get_value_from_s3_bucket(self, bucket_name: str, key: str, section: str, value: str) -> str:
        self.config.read_string(self.boto.get_s3_file_text(bucket_name, key))
        return self.config[section][value]

    def set_value_to_s3_bucket(self, bucket_name: str, key: str, section: str, value: str, text: str) -> None:
        self.old_text = self.boto.get_s3_file_text(bucket_name, key)
        self.config.read_string(self.old_text)
        self.config.set(section, value, text)
        res = ""
        for section in self.config.sections():
            res += f"[{section}]" + os.linesep
            res += str.join(os.linesep, [f"{x}={y}" for x, y in list(self.config[section].items())])
        self.boto.rewrite_s3_file(bucket_name, key, res)


def arguments_parse():
    parser = argparse.ArgumentParser(description='Process working with database.')

    parser.add_argument('--create',
                        action='store_true',
                        help='Creates database instance if it doesnt exist and all the necessary tables')

    parser.add_argument('--peoplecards',
                        type=int,
                        help='Creates a number of people with cards')

    parser.add_argument('--transactions',
                        action='store_true',
                        help='Creates transactions for each valid cart from table')

    return parser.parse_args()
