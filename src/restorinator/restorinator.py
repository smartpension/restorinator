"""
Restorinator:
    Restores a parquet backup from S3 to an RDS MySQL instance

Created by:
    Jasim Basheer <jasim.basheer@smartpension.co.uk>
    Pete Brar <pete.brar@smartpension.co.uk>
"""

import csv
import json
import logging

from urllib.parse import urlparse
from io import BytesIO

import boto3
import pandas as pd
import pyarrow.parquet as pq

from sqlalchemy import create_engine


def get_client(region_name):
    return boto3.client('s3', region_name=region_name)

def list_all_objects(region, s3_bucket, s3_prefix):
    """
    List all objects in the bucket with the base prefix
    """
    client = get_client(region)

    objects = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    all_objects = objects.get('Contents', [])

    while objects.get('IsTruncated', None):
        continuation_token = objects['NextContinuationToken']
        objects = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix, ContinuationToken=continuation_token)

        logging.debug(f'Response: {objects}')

        all_objects.extend(objects.get('Contents', []))

    return all_objects

def load_parquet_data(region, s3_bucket, file_keys):
    """
    Load Parquet files from S3 into a Pandas DataFrame
    """
    dfs = []

    client = get_client(region)

    for file_key in file_keys:
        logging.info(f'Loading file: {file_key}')
        file_obj = client.get_object(Bucket=s3_bucket, Key=file_key)
        parquet_file = pq.ParquetFile(BytesIO(file_obj['Body'].read()))
        df = parquet_file.read().to_pandas()

        logging.info(f'Loaded file: {file_key}, rows: {len(df)}')
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def get_db_engine(constr, ssl_verify=False, args={}):
    """
    Constructs the SQLAlchemy engine
    """
    scheme = urlparse(constr).scheme
    if scheme == 'mysql':
        constr = constr.replace('mysql', 'mysql+pymysql', 1)

    connect_args = args
    if not ssl_verify:
            connect_args['ssl_verify_cert'] = "false"
            connect_args['ssl_verify_identity'] = "false"

    print(constr)

    return create_engine(
        constr, connect_args=connect_args
    )

def load_data(table_name, session, region, bucket_name, file_keys):
    """
    Load parquet frame from S3
    """
    frame = load_parquet_data(region, bucket_name, file_keys)
    frame.rename(columns={'content_type': 'content_type_dr_import'}, inplace=True)

    q = frame.to_sql(table_name, con=session, if_exists='append', index=False)

    return frame

def restore(region, s3_url, db_url, ssl_verify=False, engine_args={}):
    """
    Restores Parquet data to an RDS instance
    """
    # Parse s3 url
    _parsed = urlparse(s3_url, allow_fragments=False)
    bucket_name, base_prefix = _parsed.netloc, _parsed.path.lstrip('/')

    # List all objects in the S3 bucket with the base prefix
    all_objects = list_all_objects(region, bucket_name, base_prefix)
    logging.info(f'Total objects found: {len(all_objects)}')

    # Create a mapping of table names to Parquet file keys
    table_files = {}
    for obj in all_objects:
        file_key = obj['Key']

        if file_key.endswith('.parquet'):
            parts = file_key.split('/')

            # Extract full table name from the directory structure
            full_table_name = parts[-3] 

            # Remove the "production." prefix if it exists
            table_name = full_table_name.split('.')[-1]
            logging.debug(f'Table Name: {table_name}')

            if table_name not in table_files:
                table_files[table_name] = []

            table_files[table_name].append(file_key)

    # Connect to RDS
    engine = get_db_engine(db_url, ssl_verify, engine_args)

    for table_count, e in enumerate(table_files.items()):
        table_name, file_keys = e

        logging.info(f'Table No: {table_count+1}')
        logging.info(f'\nProcessing table: {table_name}')

        # Load Parquet data from S3 into a Pandas DataFrame
        max_no_of_dataframes = 20

        if len(file_keys) <= max_no_of_dataframes:
            frame = load_data(table_name, engine, region, bucket_name, file_keys)

        else:
            logging.warn(f'Too many files for one dataframe found for table {table_name}, chunking')
            chunked_file_keys = [
                file_keys[i:i+max_no_of_dataframes]
                for i in range(0, len(file_keys), max_no_of_dataframes)
            ]

            total_rows=0

            for chunk_no, partial_file_key_list in enumerate(chunked_file_keys):
                frame = load_data(table_name, engine, region, bucket_name, partial_file_key_list)
                total_rows += frame.shape[0]

            logging.info(f'Total rows for large table {table_name} - {total_rows}')

        if not frame.empty:
            # Print the first few rows of the DataFrame for verification
            logging.info(f'Data for table {table_name}:\n')
        else:
            logging.info(f'No data found for table {table_name}')

    logging.info('\nData Import to RDS from S3 completed successfully!')
