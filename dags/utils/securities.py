from dags.utils.cfg.configs import Config as cfg
from botocore.client import BaseClient
from boto3.s3.transfer import TransferConfig
import boto3
import requests


def create_s3():
    s3 = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=cfg.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=cfg.get("AWS_SECRET_KEY")
    )
    return s3


def load_data(url: str, bucket: str, key: str, s3: BaseClient):
    config = TransferConfig(multipart_threshold=50 * 1024 * 1024)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        s3.upload_fileobj(r.raw, bucket, key, Config=config)
