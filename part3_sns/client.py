import boto3


class Client:
    def __init__(self):
        self.client = boto3.client('sns')
        self.subscribersArn = []
        self.topic = 'arn:aws:sns:eu-north-1:555123318795:airflow'

    def subscribe(self, mails: list) -> list:
        """
        Subscribes all the users from list to topic
        for retrieving messages and notifactions
        :param mails: string list of emails
        :return: list of dictionaries arn`s of all subscribers
        """
        for mail in mails:
            response = self.client.subscribe(
                TopicArn=self.topic,
                Protocol='email',
                Endpoint=mail,
                ReturnSubscriptionArn=True
            )
            self.subscribersArn.append(response)
        return self.subscribersArn

    def notificate_all(self, msg: str) -> dict:
        """
        Adds message to  topic, thus every sibscriber gets this message to mail
        :param msg: string with message in mail
        :return: dictionary with message status
        """
        response = self.client.publish(
            TopicArn=self.topic,
            Message=msg,
            Subject='AWS SOMETHING WENT WRONG',
        )

        return response
