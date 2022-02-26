
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

import string
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
import en_core_sci_sm
import en_core_sci_md
pd.options.mode.chained_assignment = None 
from tqdm import tqdm
import logging
logger = logging.getLogger("spacy")
logger.setLevel(logging.ERROR)
import en_core_sci_lg
import en_core_sci_sm
import en_core_sci_md    

def preprocess_with_spacy():
    dataframe = pd.read_csv(Variable.get('eliminated_non_english_languages'))
    print(f'::::::: PRIOR type {type(dataframe)} and shape {dataframe.shape}')
    
    punctuations = string.punctuation
    stopwords = list(STOP_WORDS)
    
    additional_stop_words = [
        'doi', 'preprint', 'copyright', 'peer', 'reviewed', 'org', 'https', 'et', 'al', 'author', 'figure', 
        'rights', 'reserved', 'permission', 'used', 'using', 'biorxiv', 'medrxiv', 'license', 'fig', 'fig.', 
        'al.', 'Elsevier', 'PMC', 'CZI', 'p', '2', '1', '3', 'l', 
        'common','review','describes','abstract','retrospective','chart','patients','study','may',
        'associated','results','including','high''found','one','well','among','Abstract','provide',
        'objective','objective:','background','range','features','participates','doi', 'preprint', 
        'permission', 'use', 'used', 'using', 'biorxiv', 'medrxiv', 'license', 'fig', 'fig.', 'al.', '):'
    ]
    for w in additional_stop_words:
        if w not in stopwords:
            stopwords.append(w)
    
    processor = en_core_sci_md.load(disable=["tagger", "ner"])
    processor.max_length = 7000000

    def spacy_processor(sentence):
        my_string = processor(sentence)
        my_string = [ word.lemma_.lower().strip() if word.lemma_ != "-PRON-" else word.lower_ for word in my_string ]
        my_string = [ word for word in my_string if word not in stopwords and word not in punctuations ]
        my_string = " ".join([i for i in my_string])
        return my_string
    dataframe = dataframe[(dataframe['publish_time']>'2020-01-01')]
    tqdm.pandas()
    dataframe["title_processed"] = dataframe["title"].progress_apply(spacy_processor)
    dataframe["abstract_processed"] = dataframe["abstract"].progress_apply(spacy_processor)
    print(f'::::::: AFTER {dataframe.shape}')
    print(f'::::::: {dataframe.info()}')
    dataframe.to_csv(Variable.get('spacy_preprocessed'))
    return True



