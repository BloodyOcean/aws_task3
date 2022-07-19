from sparkmanager import SparkManager


def main():
    spark_manager = SparkManager()
    spark_manager.read_cards()
    spark_manager.read_transactions()


if __name__ == '__main__':
    main()
