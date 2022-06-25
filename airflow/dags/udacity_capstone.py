from datetime import datetime, timedelta
import os
from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateTablesOperator,StageToRedshiftOperator, DataQualityOperator)
#from helpers import SqlQueries

default_args = {
    'owner': 'Spyroula Masiala',
    'depends_on_past': False, 
    'start_date': datetime(2016, 1, 1),
    'retries': 3, 
    'retry_delay': timedelta(minutes=5), 
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG('data_engineering_capstone_project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly',
           max_active_runs = 1
         )

s3_bucket = 'udacity-data-engineer-capstone'
immigration_s3_key = 'immigration.parquet'
country_s3_key = 'country.parquet'
states_s3_key = 'states.parquet'
date_s3_key = 'date.parquet'

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_in_redshift = CreateTablesOperator(
    task_id = 'create_tables_in_redshift',
    redshift_conn_id = 'redshift',
    dag = dag
)

stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id='Stage_immigration',
    aws_credential_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    s3_bucket = s3_bucket,
    s3_key = immigration_s3_key,
    schema = 'public',
    table = 'immigration',
    file_format = "FORMAT AS PARQUET",
    dag=dag
)

stage_country_to_redshift = StageToRedshiftOperator(
    task_id='Stage_country',
    aws_credential_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    s3_bucket = s3_bucket,
    s3_key = country_s3_key,
    schema = 'public',
    table = 'country',
    file_format = "FORMAT AS PARQUET",
    dag=dag)

stage_states_to_redshift = StageToRedshiftOperator(
    task_id='Stage_states',
    aws_credential_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    s3_bucket = s3_bucket,
    s3_key = states_s3_key,
    schema = 'public',
    table = 'states',
    file_format = "FORMAT AS PARQUET",
    dag=dag)

stage_date_to_redshift = StageToRedshiftOperator(
    task_id='Stage_date',
    aws_credential_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    s3_bucket = s3_bucket,
    s3_key = date_s3_key,
    schema = 'public',
    table = 'date',
    file_format = "FORMAT AS PARQUET",
    dag=dag
)

query_checks=[{'check_sql': "SELECT COUNT(*) FROM immigration WHERE cicid is null", 'expected_result':0}, 
              {'check_sql': "SELECT COUNT(*) FROM country WHERE Code is null", 'expected_result':0}, 
              {'check_sql': "SELECT COUNT(*) FROM states WHERE Code is null", 'expected_result':0}, 
              {'check_sql': "SELECT COUNT(*) FROM date WHERE date is null", 'expected_result':0}]


run_quality_checks = DataQualityOperator(
    task_id='Run_Data_Quality_Checks',
    redshift_conn_id = 'redshift',
    dq_checks=query_checks,
    tables_names=['immigration', 'country', 'states', 'date'],
    dag=dag
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_in_redshift
create_tables_in_redshift >> stage_immigration_to_redshift
stage_immigration_to_redshift >> stage_country_to_redshift
stage_immigration_to_redshift >> stage_states_to_redshift
stage_immigration_to_redshift >> stage_date_to_redshift
stage_country_to_redshift >> run_quality_checks
stage_states_to_redshift >> run_quality_checks
stage_date_to_redshift >> run_quality_checks
run_quality_checks >> end_operator
