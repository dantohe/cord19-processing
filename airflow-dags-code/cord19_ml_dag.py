
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
from ml_utils_vectorization import *


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
    dag_id="CORD19_MACHINE_LEARNING",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval=None,
    catchup=False,
    tags=['ML-engineering'],
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    load_preprocessed_data_from_s3_and_save_it_locally = PythonOperator(
        task_id='load_preprocessed_data_from_s3_and_save_it_locally',  python_callable=load_preprocessed_data_from_s3_and_save_it_locally
    )
    
    verify_data = PythonOperator(
        task_id='verify_data',  python_callable=verify_data
    )
    
    
    vectorization_compute_sparse_matrix = PythonOperator(
        task_id='vectorization_compute_sparse_matrix',  python_callable=vectorization_compute_sparse_matrix
    )
    
    vectorization_reduce_dimensionality_with_PCA = PythonOperator(
        task_id='vectorization_reduce_dimensionality_with_PCA',  python_callable=vectorization_reduce_dimensionality_with_PCA
    )
    
    clustering_v01 = PythonOperator(
        task_id='clustering_v01',  python_callable=clustering_v01
    )
    
    tsne_v01 = PythonOperator(
        task_id='tsne_v01',  python_callable=tsne_v01
    )
    
    tsne_cluster_images = DummyOperator(task_id='tsne_cluster_images')
    
    latent_dirichlet_allocation_v01 = PythonOperator(
        task_id='latent_dirichlet_allocation_v01',  python_callable=latent_dirichlet_allocation_v01
    )
    
    doc2vec_transformation_v01 = PythonOperator(
        task_id='doc2vec_transformation_v01',  python_callable=doc2vec_transformation_v01
    )
    further_processing_using_doc2vec_model = DummyOperator(task_id='further_processing_using_doc2vec_model')
    
   
    

    start >> load_preprocessed_data_from_s3_and_save_it_locally >> verify_data >> vectorization_compute_sparse_matrix
    vectorization_compute_sparse_matrix >> vectorization_reduce_dimensionality_with_PCA >> clustering_v01
    clustering_v01 >> tsne_v01 >> tsne_cluster_images >> end
    clustering_v01 >> latent_dirichlet_allocation_v01 >> end
    verify_data >> doc2vec_transformation_v01 >> further_processing_using_doc2vec_model >> end
