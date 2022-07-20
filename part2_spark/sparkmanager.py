import logging
from datetime import datetime
import credentials
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

from helpers import ConfigReader


class SparkManager:
    def __init__(self):
        self.config_helper = ConfigReader()
        config_helper_db = ConfigReader()

        self.jdbc_connection = config_helper_db.get_value_from_s3_bucket('task3-files',
                                                                         './configs/dbconf.ini',
                                                                         'DbInfo',
                                                                         'connection_string_jdbc')

        self.connector = "aws_task3/part2_spark/mariadb-java-client-3.0.6.jar," \
                         "aws_task3/part2_spark/aws-java-sdk-bundle-1.11.1026.jar," \
                         "aws_task3/part2_spark/hadoop-aws-3.3.3.jar," \
                         "aws_task3/part2_spark/hadoop-common-3.3.3.jar "

        logging.info('Creating Spark configuration')
        self.conf = SparkConf()
        self.conf.set("spark.jars", self.connector)

        logging.info('Creating Spark session')
        self.spark = SparkSession.builder \
            .config(conf=self.conf) \
            .master("local[*]") \
            .appName("Python Spark SQL basic example") \
            .getOrCreate()

        self.spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", credentials.read_credentials()[0])
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", credentials.read_credentials()[1])
        self.spark._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3-eu-central-1.amazonaws.com')
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a"
                                                                                     ".SimpleAWSCredentialsProvider")

        print(f"Hadoop version = {self.spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")

    def read_transactions(self):
        """
        Reads transactions table from database then converts it to dataframe, filters and
        writes partitioned dataframe to parquet storage
        :return:
        """
        max_transaction_date = datetime \
            .strptime(self.config_helper
                      .get_value_from_s3_bucket('task3-files',
                                                'configs/appconfig.ini',
                                                'Session',
                                                'max_transaction_time'),
                      '%Y-%m-%d %H:%M:%S.%f') \
            .replace(microsecond=0)
        logging.info('Reading transactions from database into dataframe')
        transactions_df = self.spark.read.format('jdbc') \
            .option('url', self.jdbc_connection) \
            .option('dbtable',
                    "(SELECT transaction_id, card_no, transaction_time, comment, value FROM Transaction) AS Result") \
            .option('driver', 'org.mariadb.jdbc.Driver') \
            .load()
        logging.info(f'Transactions dataframe loaded, size: {transactions_df.count()}')
        logging.info('Filtering transactions dataframe by transaction time')
        transactions_df_filtered = transactions_df \
            .where(transactions_df.transaction_time >= max_transaction_date)
        logging.info(f'Transactions dataframe filtered, size: {transactions_df_filtered.count()}')
        logging.info('Adding partition_date column to transactions dataframe.')
        transactions_modified_df = transactions_df_filtered \
            .withColumn('partition_date', to_date(transactions_df_filtered.transaction_time)
                        .cast('string'))
        logging.info('Writing partitioned transactions dataframe into separated parquet storage')
        transactions_modified_df.write \
            .partitionBy('partition_date') \
            .format("parquet") \
            .option('fs.s3a.committer.name', 'partitioned') \
            .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
            .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
            .mode('overwrite') \
            .save('s3a://task3-data/transactions/res')

    def read_cards(self):
        """
        Reads cards table from database then converts it to dataframe, filters and
        writes partitioned dataframe to parquet storage
        :return: #hadoop 3.3.1 aws java sdk bundle 1.11.375 hadoop common 3.3.1
        """
        max_creation_date = datetime \
            .strptime(self.config_helper
                      .get_value_from_s3_bucket('task3-files',
                                                'configs/appconfig.ini',
                                                'Session',
                                                'max_created_on'),
                      '%Y-%m-%d %H:%M:%S.%f') \
            .replace(microsecond=0)
        cards_df = self.spark.read.format("jdbc") \
            .option('url', self.jdbc_connection) \
            .option('dbtable',
                    f"(SELECT card_id, holder_id, card_no, currency, amount, valid_until, created_on, last_used_on "
                    f"FROM Cards) AS Result") \
            .option('driver', 'org.mariadb.jdbc.Driver') \
            .load()
        logging.info(f'Cards dataframe loaded, size: {cards_df.count()}')
        filtered_cards_df = cards_df.where(cards_df.created_on >= max_creation_date)
        logging.info(f'Cards dataframe filtered, new size: {filtered_cards_df.count()}')
        filtered_cards_date_df = filtered_cards_df \
            .withColumn("created_date", to_date(filtered_cards_df.created_on)
                        .cast('string'))

        filtered_cards_date_df.write \
            .partitionBy('created_date') \
            .format("parquet") \
            .mode('overwrite') \
            .option('fs.s3a.committer.name', 'partitioned') \
            .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
            .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
            .save('s3a://task3-data/cards/res')
