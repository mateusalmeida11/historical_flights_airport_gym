import os

import boto3
from botocore.exceptions import ClientError


class S3UploadError(Exception):
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class S3GetError(Exception):
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class S3EmptyFile(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class S3:
    def __init__(self):
        self.s3_client = self.create_client()

    def create_client(self):
        access_key = os.environ.get("ACCESS_KEY")
        secret_access_key = os.environ.get("SECRET_ACCESS_KEY")
        region = os.environ.get("REGION", "us-east-1")

        if access_key and secret_access_key:
            return boto3.client(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_access_key,
                region_name=region,
            )
        else:
            return boto3.client("s3", region_name=region)

    def upload_file(self, bucket, data, key):
        try:
            response = self.s3_client.put_object(
                Body=data,
                Key=key,
                Bucket=bucket,
                ContentType="application/json",
            )
            return response
        except ClientError as e:
            raise S3UploadError(
                e.response["Error"]["Message"],
                status_code=e.response["ResponseMetadata"]["HTTPStatusCode"],
            ) from e

    def get_file(self, bucket_name, key):
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        except ClientError as e:
            raise S3GetError(
                e.response["Error"]["Message"],
                status_code=e.response["ResponseMetadata"]["HTTPStatusCode"],
            ) from e

        lenght = int(response["ResponseMetadata"]["HTTPHeaders"]["content-length"])
        if lenght <= 0:
            raise S3EmptyFile("Empty File")
        return response["Body"]
