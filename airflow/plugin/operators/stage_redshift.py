from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook

import datetime
import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credential_id,
                 s3_bucket,
                 s3_key,
                 schema,
                 table,
                 file_format,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credential_id = aws_credential_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.schema = schema
        self.table = table
        self.file_format = file_format
        self.autocommit = True
        self.region = 'us-west-2'

    def execute(self, context):
        """
        Extract data from S3 buckets into staging tables in Redshift cluster .
        
        Parameters
        -----------
        redshift_conn_id: str
            The Redshift cluster connection
        aws_credentials_id: str
            The AWS connection credentials
        table: str
             The name of the table in the Redshift cluster 
        s3_bucket: str
            The name of the S3 bucket holding the source data
        s3_key: str
            The S3 key files of the source data
        file_format: str
            The format of the source file e.g. JSON, CSV, PARQUET
        autocommit : bool
            The autocommit mode
        region : str 
            The AWS region
        """
        self.log.info('Initializing COPY command...')        
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        
        file_copy_format = '\n\t\t\t'.join(self.file_format)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        self.log.info(f"Select the staging files for the {self.table} table from the location : {s3_path}")

        copy_query = """
            COPY {}.{} \
            FROM '{}' \
            ACCESS_KEY_ID '{}' \
            SECRET_ACCESS_KEY '{}' \
            {}; \
        """.format(self.schema,
                   self.table,
                   s3_path,
                   credentials.access_key,
                   credentials.secret_key,
                   self.file_format)

        self.log.info(f"Running query : {copy_query}")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        redshift_hook.run(copy_query, self.autocommit)
        self.log.info(f"Successfully staged: {self.table} from S3 bucket to Redshift.")
