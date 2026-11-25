import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKET = "cinema"


def s3_client():
    return boto3.client(
        "s3", endpoint_url="http://minio:9000",
        aws_access_key_id="cinema", aws_secret_access_key="cinema123",
        region_name="us-east-1", config=Config(s3={"addressing_style": "path"})
    )


def create_bucket(s3):
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            raise
        s3.create_bucket(Bucket=BUCKET)


def keys(s3, prefix):
    result = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            result.append(obj["Key"])
    return sorted(result)
