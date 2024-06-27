# restorinator

A tool for restoring AWS RDS Parquet S3 exports to any SQL database (although only really tested with MySQL and SQLite)

## Installation

You can pip install directly from git

    $ pip install "git+https://github.com/smartpension/restorinator@main#egg=restorinator"

Or clone this repository, or download a zip and from the `restorinator` directory run

    $ pip install -e .

It is wise to do this in a virtualenv, eg.

    $ python3 -m venv ve
    $ . ./ve/bin/activate
    $ pip install -e .


## Usage

Ensure you are logged into AWS and export the correct environment variables, or use an instance with the relevant IAM permissions in place

Help output:

    usage: restorinator [-h] [--region REGION] [--tlsverify] [--ssl-ca SSL_CA] S3_URL DATABASE_URL

    Restore a Parquet backup to SQL

    positional arguments:
      S3_URL                S3 URL including prefix of DB to restore, eg. s3://my-backups/foo-cluster/foo-db
      DATABASE_URL          Database connection string, eg. mysql://user:password@localhost/dbname

    options:
      -h, --help            show this help message and exit
      --region REGION, -r REGION
			    S3 region (default eu-west-1)
      --tlsverify           Strict TLS verification (default is no)
      --ssl-ca SSL_CA       Path to SSL CA

Example:

    $ restorinator -b s3://some-backups-bucket/foo-cluster-01-02-22-060000/production \
          mysql://writer:t0ps33krit@mydbcluster.cluster-c7tj4example.us-east-1.rds.amazonaws.com/production
