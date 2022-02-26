
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

dt = datetime.datetime.today()
s3 = boto3.resource('s3')


def eliminate_papers_older_than_01_01_2020():
    dataframe = pd.read_csv(Variable.get('raw_data_file_empty_columns_eliminated'))
    dataframe.publish_time = pd.to_datetime(dataframe.publish_time)
    print(f'::::::: eliminate_papers_older_than_01_01_2020 {dataframe.publish_time.value_counts()[0:10]}')
    print(f'::::::: PRIOR {dataframe.shape}')
    dataframe = dataframe[(dataframe['publish_time']>'2020-01-01')]
    print(f'::::::: AFTER {dataframe.shape}')
    dataframe.to_csv(Variable.get('eliminated_papers_older_than_01_01_2020'))
    return True


# 
# eliminates the known empty columns
def eliminate_empty_columns():
    cord_metadata_raw = pd.read_csv(Variable.get('raw_data_file'))
    print(f':::::::cord_metadata_raw {cord_metadata_raw.shape}')
    cord_metadata_raw.pop('microsoft academic paper id')
    cord_metadata_raw.pop('who #covidence')
    cord_metadata_raw.pop('has_full_text')
    cord_metadata_raw.pop('full_text_file')
    print(f':::::::cord_metadata_raw {cord_metadata_raw.shape}')
    cord_metadata_raw.to_csv(Variable.get('raw_data_file_empty_columns_eliminated'))
    return True




