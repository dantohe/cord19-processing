
import datetime
import pandas as pd
import io
import os
import boto3
from io import BytesIO
from io import StringIO

from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.redshift_to_s3_operator import RedshiftToS3Transfer
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.models import Variable

dt = datetime.datetime.today()
s3 = boto3.resource('s3')
s3_c = boto3.client("s3")

# 
# 
def load_raw_data_from_s3_and_save_it_locally():
    obj = s3.Object(Variable.get('s3_staging_bucket'), Variable.get('unload_raw_data_to_s3_key')+'/'+Variable.get('unload_raw_data_to_s3_filename'))
    with BytesIO(obj.get()['Body'].read()) as bio:
        df = pd.read_csv(bio)
    print(f':::::::dataframe:\n{df.info()}')
    df.to_csv(Variable.get('raw_data_file'))
    print(f':::::::Dataframe was saved locally')
    return True


def put_preprocessed_data_into_s3():
    dataframe = pd.read_csv(Variable.get('eliminated_papers_older_than_01_01_2020'))
    print(f':::::::dataframe:\n{dataframe.info()}')
    csv_buf = StringIO()
    dataframe.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3_c.put_object(Bucket=Variable.get('s3_staging_bucket'), Body=csv_buf.getvalue(), Key=Variable.get('intermediate_preprocessed_s3_key'))
    print(f':::::::Dataframe was saved to s3')
    return True


def put_spacy_preprocessed_data_into_s3():
    dataframe = pd.read_csv(Variable.get('spacy_preprocessed'))
    print(f':::::::dataframe:\n{dataframe.info()}')
    csv_buf = StringIO()
    dataframe.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3_c.put_object(Bucket=Variable.get('s3_staging_bucket'), Body=csv_buf.getvalue(), Key=Variable.get('spacy_preprocessed_s3_key'))
    print(f':::::::Dataframe was saved to s3')
    return True

def load_preprocessed_data_from_s3_and_save_it_locally():
    print(f":::::::object located at {Variable.get('spacy_preprocessed_s3_key')}")
    obj = s3.Object(Variable.get('s3_staging_bucket'), Variable.get('spacy_preprocessed_s3_key'))
    with BytesIO(obj.get()['Body'].read()) as bio:
        df = pd.read_csv(bio)
    print(f':::::::dataframe:\n{df.info()}')
    df.to_csv(Variable.get('spacy_preprocessed'))
    print(f":::::::Dataframe was saved locally at {Variable.get('spacy_preprocessed')}")
    return True
