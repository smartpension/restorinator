import argparse

import logging

from .restorinator import restore

def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Restore a Parquet backup to SQL')

    parser.add_argument('S3_URL',
        help='S3 URL including prefix of DB to restore, eg. s3://my-backups/foo-cluster/foo-db')

    parser.add_argument('DATABASE_URL',
        help='Database connection string, eg. mysql://user:password@localhost/dbname')

    parser.add_argument('--region', '-r', required=False, default='eu-west-1',
        help='S3 region (default eu-west-1)')

    parser.add_argument('--tlsverify', action='store_true', required=False, default=False,
        help='Strict TLS verification (default is no)')

    parser.add_argument('--ssl-ca', required=False, help='Path to SSL CA')

    args = parser.parse_args()

    engine_args = {}

    if args.ssl_ca:
        engine_args['ssl'] = {'ca': args.ssl_ca}

    restore(
        args.region,
        args.S3_URL,
        args.DATABASE_URL,
        ssl_verify=args.tlsverify,
        engine_args=engine_args,
    )

