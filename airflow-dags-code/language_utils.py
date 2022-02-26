
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

from tqdm import tqdm
from langdetect import detect
from langdetect import DetectorFactory
from pprint import pprint
DetectorFactory.seed = 3


def eliminate_non_english_languages():
    dataframe = pd.read_csv(Variable.get('eliminated_papers_older_than_01_01_2020'))
    dataframe.publish_time = pd.to_datetime(dataframe.publish_time)
    
    
    dataframe[["cord_uid", "sha","source_x","title","abstract","authors","journal"]] = dataframe[["cord_uid", "sha","source_x","title","abstract","authors","journal"]].astype(str) 
    
    # hold label - language
    languages_in_abstratcs = []

    # go through each text
    for x in tqdm(range(0, len(dataframe))):
        # split by space into list, take the first x intex, join with space
        abstract = dataframe.iloc[x]['abstract'].split(" ")

        language = "en"
        try:
            if len(abstract) > 50:
                language = detect(" ".join(abstract[:50]))
            elif len(abstract) > 0:
                language = detect(" ".join(abstract[:len(abstract)]))

        except Exception as e:
            all_words = set(abstract)
            try:
                language = detect(" ".join(all_words))
            # finding the title text
            except Exception as e:
                try:
                    language = detect(dataframe.iloc[x]['title'])
                except Exception as e:
                    language = "unknown"
                    pass

        # get the language
        languages_in_abstratcs.append(language)
    
    

    languages_dict = {}
    for lang in set(languages_in_abstratcs):
        languages_dict[lang] = languages_in_abstratcs.count(lang)

    print("Total: {}\n".format(len(languages_in_abstratcs)))
    pprint(languages_dict)
    
    dataframe['language'] = languages_in_abstratcs
    
    print(f'::::::: PRIOR {dataframe.shape}')
    dataframe = dataframe[dataframe['language'] == 'en'] 
    print(f'::::::: AFTER {dataframe.shape}')
    dataframe.to_csv(Variable.get('eliminated_non_english_languages'))
    return True




