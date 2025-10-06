import boto3


class S3:
    def __init__(self, bucket_name):
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.bucket_name = bucket_name

    def upload_file(self, data, key):
        response = self.s3_client.put_object(
            Body=data, Key=key, Bucket=self.bucket_name, ContentType="application/json"
        )
        return response
