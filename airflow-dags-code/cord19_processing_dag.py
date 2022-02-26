
import datetime
import pandas as pd
import io
import os
import boto3
from io import BytesIO

from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.redshift_to_s3_operator import RedshiftToS3Transfer
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.models import Variable


from transfer_utils import *
from cleanup_utils import *
from language_utils import *
from spacy_utils import *

dt = datetime.datetime.today()
s3 = boto3.resource('s3')

# 
# 
# 
# 
# DAG works
# 
# 
# 

with DAG(
    dag_id="CORD19_DATA_PRE_PROCESSING",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval=None,
    catchup=False,
    tags=['data-engeneering'],
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    get_version = RedshiftSQLOperator(
        task_id='get_version',
        redshift_conn_id="redshift_db",
        sql="""
            select version();
        """,
    )
    drop_external_schema_if_exists = RedshiftSQLOperator(
        task_id='drop_schema',
        redshift_conn_id="redshift_db",
        sql="drop schema if exists "+ Variable.get('redshift_destination_external_schema_name') +" ;",
    )
    create_external_schema_pointing_to_datalake = RedshiftSQLOperator(
        task_id='create_external_schema_pointing_to_datalake',
        redshift_conn_id="redshift_db",
        sql=f"create external schema {Variable.get('redshift_destination_external_schema_name')} \
            from data catalog database  '{Variable.get('redshift_destination_glue_database_name')}' \
            iam_role '{Variable.get('redshift_role_arn')}' \
            create external database if not exists;",
    )
    drop_redshift_native_table_if_exists = RedshiftSQLOperator(
        task_id='drop_redshift_native_table_if_exists',
        redshift_conn_id="redshift_db",
        sql=f"drop table if exists public.{Variable.get('redshift_destination_table_name')};"
    )
    create_redshift_native_table = RedshiftSQLOperator(
        task_id='create_redshift_native_table',
        redshift_conn_id="redshift_db",
        sql=f"create table public.{Variable.get('redshift_destination_table_name')} \
            as select * from \
            {Variable.get('redshift_destination_external_schema_name')}.{Variable.get('redshift_destination_table_name')} ;"
    )
    unload_raw_data_to_s3 = RedshiftToS3Operator(
        task_id='unload_raw_data_to_s3',
        schema='public',
        table=Variable.get('redshift_destination_table_name'),
        s3_bucket=Variable.get('s3_staging_bucket'),
        s3_key=Variable.get('unload_raw_data_to_s3_key'),
        redshift_conn_id='redshift_db',
        unload_options = ['CSV','parallel off', 'ALLOWOVERWRITE', 'HEADER']
    )
    
    load_raw_data_from_s3_and_save_it_locally = PythonOperator(
        task_id='load_raw_data_from_s3_and_save_it_locally',  python_callable=load_raw_data_from_s3_and_save_it_locally
    )
    eliminate_empty_columns = PythonOperator(
        task_id='eliminate_empty_columns',  python_callable=eliminate_empty_columns
    )
    eliminate_papers_older_than_01_01_2020 = PythonOperator(
        task_id='eliminate_papers_older_than_01_01_2020',  python_callable=eliminate_papers_older_than_01_01_2020
    )
    put_preprocessed_data_into_s3 = PythonOperator(
        task_id='put_preprocessed_data_into_s3',  python_callable=put_preprocessed_data_into_s3
    )
    
    eliminate_non_english_languages = PythonOperator(
        task_id='eliminate_non_english_languages',  python_callable=eliminate_non_english_languages
    )
    preprocess_with_spacy = PythonOperator(
        task_id='preprocess_with_spacy',  python_callable=preprocess_with_spacy
    )
    put_spacy_preprocessed_data_into_s3 = PythonOperator(
        task_id='put_spacy_preprocessed_data_into_s3',  python_callable=put_spacy_preprocessed_data_into_s3
    )
    
    

    start >> get_version >> drop_external_schema_if_exists 
    drop_external_schema_if_exists >> create_external_schema_pointing_to_datalake 
    create_external_schema_pointing_to_datalake >> drop_redshift_native_table_if_exists
    drop_redshift_native_table_if_exists >> create_redshift_native_table
    create_redshift_native_table >> unload_raw_data_to_s3
    unload_raw_data_to_s3 >> load_raw_data_from_s3_and_save_it_locally
    load_raw_data_from_s3_and_save_it_locally >> eliminate_empty_columns
    eliminate_empty_columns >> eliminate_papers_older_than_01_01_2020 >> put_preprocessed_data_into_s3
    put_preprocessed_data_into_s3 >> eliminate_non_english_languages
    eliminate_non_english_languages >> preprocess_with_spacy
    preprocess_with_spacy >> put_spacy_preprocessed_data_into_s3
    put_spacy_preprocessed_data_into_s3 >> end