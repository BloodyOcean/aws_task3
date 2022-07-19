import boto3


class BotoManager:
    def __init__(self):
        self.client = boto3.client('rds', region_name='eu-north-1')
        self.s3 = boto3.client('s3', region_name='eu-north-1')

    def rewrite_s3_file(self, bucket_name: str, key: str, text: str) -> None:
        self.s3.put_object(Body=text, Bucket=bucket_name, Key=key)

    def get_s3_file_text(self, bucket_name: str, key: str):
        file = self.s3.get_object(Bucket=bucket_name, Key=key)
        inner_text = file['Body'].read().decode()
        return inner_text
