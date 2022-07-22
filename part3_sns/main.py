import client
import argparse


def read_args():
    parser = argparse.ArgumentParser(description='Process some messages from AWS SNS.')
    parser.add_argument('--mes', dest='msg', help='Sends corresponding message to all subscribers')
    args = parser.parse_args()
    return args


def main():
    arguments = read_args()
    snsclient = client.Client()
    snsclient.subscribe(['cchugaister@gmail.com', 'bataricet@gmail.com'])

    if arguments.msg is not None:
        snsclient.notificate_all(str(arguments.msg))


if __name__ == '__main__':
    main()
