import os
import datetime
from urllib.parse import urlparse
from pathlib import Path
import logging

import pytest

from moto import mock_aws

import boto3

from sqlalchemy import create_engine, text

from restorinator import restorinator


S3_URL = 's3://foo-backup/foo-aurora3-2024-06-26-060001/snake_db'
DB_URL = 'mysql://bob:hunter2@localhost/snake_db'
REGION = 'eu-west-1'

def test_path():
    return Path(__file__).parent.absolute()

@pytest.fixture(scope='function')
def aws_credentials():
    os.environ['AWS_ACCESS_KEY_ID'] = 'mock'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'mock'
    os.environ['AWS_SECURITY_TOKEN'] = 'mock'
    os.environ['AWS_SESSION_TOKEN'] = 'mock'
    os.environ['AWS_DEFAULT_REGION'] = REGION
    os.environ['MOTO_S3_DEFAULT_MAX_KEYS'] = '10'

    moto_credentials_file_path = test_path()/'dummy_aws_credentials'
    os.environ['AWS_SHARED_CREDENTIALS_FILE'] = str(moto_credentials_file_path)

@pytest.fixture(scope='function')
def aws_s3(aws_credentials):
    with mock_aws():
        yield boto3.client('s3', region_name=REGION)


@pytest.fixture
def fake_infra(aws_s3):
    _parsed = urlparse(S3_URL, allow_fragments=False)
    bucket_name = _parsed.netloc

    s3_client = boto3.client("s3")

    s3_client.create_bucket(
        ACL = 'private',
        Bucket = bucket_name,
        CreateBucketConfiguration = {
            'LocationConstraint': REGION
        }
    )

    # Upload S3 fixtures
    for root, dirs, files in os.walk(test_path()/'s3'):
        if files:
            s3_prefix = os.path.relpath(root, test_path()/'s3')
            for file in files:
                fs_name = str(Path(root)/file)
                s3_object_name = str(Path(s3_prefix)/file)
                s3_client.upload_file(fs_name, bucket_name, s3_object_name)

    for fi in range(100):
        s3_client.put_object(Bucket=bucket_name, Key=f'testpath/file{fi}', Body=bytes(str(fi), 'ascii'))


@mock_aws
def test_restore(fake_infra, tmp_path):
    tmp_db_file = tmp_path/'test.db'
    engine = create_engine(f'sqlite://')

    restorinator.get_db_engine = lambda *a: engine

    restorinator.restore(REGION, S3_URL, DB_URL)

    with engine.connect() as con:
        rs = con.execute(text('SELECT * FROM snakes'))
        rows = list(rs)

        assert (1, 'Python') in rows

        assert len(rows) == 9

@mock_aws
def test_pagination(fake_infra):
    _parsed = urlparse(S3_URL, allow_fragments=False)
    bucket_name = _parsed.netloc
    objs = restorinator.list_all_objects(REGION, bucket_name, 'testpath')

    assert len(objs) == 100
