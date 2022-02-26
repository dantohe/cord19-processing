
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
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
from sklearn.cluster import MiniBatchKMeans
from sklearn.cluster import KMeans
from sklearn import metrics
from scipy.spatial.distance import cdist
from sklearn.manifold import TSNE
from matplotlib import pyplot as plt
import seaborn as sns
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
import nltk as nltk 
from nltk.tokenize import word_tokenize
nltk.download('punkt')
from nltk import word_tokenize,sent_tokenize


dt = datetime.datetime.today()
s3 = boto3.resource('s3')

from tqdm import tqdm
from langdetect import detect
from langdetect import DetectorFactory
from pprint import pprint
DetectorFactory.seed = 3


def verify_data():
    dataframe = pd.read_csv(Variable.get('spacy_preprocessed'))
    print(f'::::::: PRIOR \n type {type(dataframe)} ')
    print(f'::::::: shape {dataframe.shape}')
    print(f'::::::: info {dataframe.info()}')
    print(f'::::::: AFTER ')
    dataframe.to_csv(Variable.get('spacy_preprocessed'))
    return True


def collect_small_subset():
    dataframe = pd.read_csv(Variable.get('spacy_preprocessed'))
    print(f'::::::: PRIOR \n type {type(dataframe)} \n shape {dataframe.shape} \n info {dataframe.info()}')
    journals = ['PLoS One','bioRxiv','Virology','Viruses','The Journal of general virology']
    dataframe_filtered = dataframe[(dataframe['publish_time']>'2020-01-01') & (dataframe['journal'].isin(journals))]
    
    print(f'::::::: AFTER ')
    dataframe_filtered.to_csv(Variable.get('ml_cord19_small_subset'))
    return True



def compute_sparse_matrix(input):
    vectorizer = TfidfVectorizer()
    return vectorizer.fit_transform(input.astype('U'))

def compute_sparse_matrix_with_max(input, max_features=1500):
    vectorizer = TfidfVectorizer(max_features=max_features)
    return vectorizer.fit_transform(input.astype('U'), max_features)

def vectorization_compute_sparse_matrix(df=Variable.get('spacy_preprocessed')):
    dataframe = pd.read_csv(df)
    print(f'::::::: PRIOR \n type {type(dataframe)} \n shape {dataframe.shape} \n info {dataframe.info()}')
    unique_number_of_tokens = dataframe['abstract_processed'].nunique() 
    tfidf_matrix_with_max = compute_sparse_matrix_with_max(dataframe['abstract_processed'].values, unique_number_of_tokens)
#     tfidf_matrix = compute_sparse_matrix(dataframe['abstract_processed'].values)
    print(f':::::::The matrix {type(tfidf_matrix_with_max)} size: {tfidf_matrix_with_max}')
    print(f'::::::: AFTER ')
#     TODO: save matrix to disk
#     dataframe.to_csv(Variable.get('ml_cord19_small_subset'))
    return True


def vectorization_compute_sparse_matrix_subset():
    vectorization_compute_sparse_matrix(Variable.get('ml_cord19_small_subset'))
    return True

def vectorization_reduce_dimensionality_with_PCA(df=Variable.get('spacy_preprocessed')):
    dataframe = pd.read_csv(df)
    print(f'::::::: PRIOR \n type {type(dataframe)} \n shape {dataframe.shape} \n info {dataframe.info()}')
    unique_number_of_tokens = dataframe['abstract_processed'].nunique() 
    print(f'::::::: 1 {str(unique_number_of_tokens)}')
#     tfidf_matrix_with_max = compute_sparse_matrix_with_max(dataframe['abstract_processed'].values, unique_number_of_tokens)
    tfidf_matrix_with_max = compute_sparse_matrix_with_max(dataframe['abstract_processed'].values)
    print(f'::::::: 2 {str(tfidf_matrix_with_max)}')
#     tfidf_matrix = compute_sparse_matrix(dataframe['abstract_processed'].values)
    pca = PCA(n_components=0.95, random_state=3)
    print(f'::::::: 3 {pca}')
    tfidf_matrix_pcaed= pca.fit_transform(tfidf_matrix_with_max.toarray())
    print(f'::::::: 4 {str(tfidf_matrix_pcaed)}')
    print(f':::::::The matrix {type(tfidf_matrix_pcaed)} size: {tfidf_matrix_pcaed} \n {tfidf_matrix_pcaed.shape}')
    print(f'::::::: AFTER ')
#     TODO: save matrix to disk
#     dataframe.to_csv(Variable.get('ml_cord19_small_subset'))
    return True


def vectorization_reduce_dimensionality_with_PCA_subset():
    vectorization_reduce_dimensionality_with_PCA(Variable.get('ml_cord19_small_subset'))
    return True



def clustering_v01(df=Variable.get('spacy_preprocessed')):
    dataframe = pd.read_csv(df)
    print(f'::::::: PRIOR \n type {type(dataframe)} \n shape {dataframe.shape} \n info {dataframe.info()}')
    unique_number_of_tokens = dataframe['abstract_processed'].nunique() 
#     tfidf_matrix_with_max = compute_sparse_matrix_with_max(dataframe['abstract_processed'].values, unique_number_of_tokens)
    tfidf_matrix_with_max = compute_sparse_matrix_with_max(dataframe['abstract_processed'].values)
    #     tfidf_matrix = compute_sparse_matrix(dataframe['abstract_processed'].values)

    # PCA
    pca = PCA(n_components=0.95, random_state=3)
    tfidf_matrix_pcaed= pca.fit_transform(tfidf_matrix_with_max.toarray())
    
    #     clustering 
    k = int(Variable.get('ml_kmeans_number_of_clusters'))
    k_means = KMeans(n_clusters=k, random_state=3)
    k_means = k_means.fit_predict(tfidf_matrix_pcaed)
    dataframe['y'] = k_means
#     print(f':::::::The matrix {type(tfidf_matrix_pcaed)} size: {tfidf_matrix_pcaed} \n {tfidf_matrix_pcaed.shape}')
    print(f'::::::: AFTER ')
#     TODO: save matrix to disk
    dataframe.to_csv(Variable.get('ml_data_with_kmeans_applied'))
    return True

def clustering_v01_subset():
    clustering_v01(Variable.get('ml_cord19_small_subset'))
    return True

def tsne_v01(df=Variable.get('ml_data_with_kmeans_applied')):
    dataframe = pd.read_csv(df)
    k = int(Variable.get('ml_kmeans_number_of_clusters'))
    print(f'::::::: PRIOR \n type {type(dataframe)} \n shape {dataframe.shape} \n info {dataframe.info()}')
    unique_number_of_tokens = dataframe['abstract_processed'].nunique() 
    print(f'::::: unique_number_of_tokens {str(unique_number_of_tokens)} ')
#     tfidf_matrix_with_max = compute_sparse_matrix_with_max(dataframe['abstract_processed'].values, unique_number_of_tokens)
    tfidf_matrix_with_max = compute_sparse_matrix_with_max(dataframe['abstract_processed'].values)
    
    
    tsne = TSNE(verbose=1, perplexity=50)
    X_embedded = tsne.fit_transform(tfidf_matrix_with_max.toarray())
    sns.set(rc={'figure.figsize':(15,15)})
    palette = sns.color_palette("bright", 1)
    sns.scatterplot(X_embedded[:,0], X_embedded[:,1], palette=palette)
    plt.title('t-SNE')
    plt.savefig(Variable.get('ml_simple_tsne_visualization_output'))
    
    
    sns.set(rc={'figure.figsize':(15,15)})
    palette = sns.hls_palette(k, l=.4, s=.9)
    sns.scatterplot(X_embedded[:,0], X_embedded[:,1], hue=dataframe['y'], legend='full', palette=palette)
    plt.title('t-SNE k-means clusters')
    plt.savefig(Variable.get('ml_improved_tsne_visualization_output'))
    
    print(f'::::::: DONE ')
    return True

def tsne_v01_subset():
    tsne_v01(Variable.get('ml_data_with_kmeans_applied'))
    return True


# Functions for printing keywords for each topic
def selected_topics(model, vectorizer, top_n=3):
    current_words = []
    keywords = []
    
    for idx, topic in enumerate(model.components_):
        words = [(vectorizer.get_feature_names()[i], topic[i]) for i in topic.argsort()[:-top_n - 1:-1]]
        for word in words:
            if word[0] not in current_words:
                keywords.append(word)
                current_words.append(word[0])
                
    keywords.sort(key = lambda x: x[1])  
    keywords.reverse()
    return_values = []
    for ii in keywords:
        return_values.append(ii[0])
    return return_values

def latent_dirichlet_allocation_v01(df=Variable.get('spacy_preprocessed')):
    dataframe = pd.read_csv(df)
    k = int(Variable.get('ml_kmeans_number_of_clusters'))
    
    vectorizers = []
    for x in range(0, k):
        # Creating a vectorizer
        vectorizers.append(CountVectorizer(min_df=1, max_df=0.9, stop_words='english', lowercase=True, token_pattern='[a-zA-Z\-][a-zA-Z\-]{2,}'))
    vectorized_data = []

    for current_cluster, cvec in enumerate(vectorizers):
        try:
            vectorized_data.append(cvec.fit_transform(dataframe.loc[dataframe['y'] == current_cluster, 'abstract_processed']))
        except Exception as e:
            print(e)
            vectorized_data.append(None)
    
    lda_models = []

    for ii in range(0, k):
        # Latent Dirichlet Allocation Model
        lda = LatentDirichletAllocation(n_components=int(Variable.get('ml_number_of_topics_per_cluster')), max_iter=10, learning_method='online',verbose=False, random_state=42)
        lda_models.append(lda)
    
    clusters_lda_data = []

    for current_cluster, lda in enumerate(lda_models):
        print("::::::: Current Cluster: " + str(current_cluster))

        if vectorized_data[current_cluster] != None:
            clusters_lda_data.append((lda.fit_transform(vectorized_data[current_cluster])))
        
    all_keywords = []
    for current_vectorizer, lda in enumerate(lda_models):
        print("Current Cluster: " + str(current_vectorizer))

        if vectorized_data[current_vectorizer] != None:
            all_keywords.append(selected_topics(lda, vectorizers[current_vectorizer]))
    
    f=open(Variable.get('ml_topics_output'),'w')

    count = 0

    for x in all_keywords:
        if vectorized_data[count] != None:
            f.write(', '.join(x) + "\n")
        else:
            f.write("Not enough instances to be determined. \n")
            f.write(', '.join(x) + "\n")
        count += 1

    f.close()    
    
    print(f'::::::: DONE ')
    return True


def latent_dirichlet_allocation_v01_subset():
    latent_dirichlet_allocation_v01(Variable.get('ml_data_with_kmeans_applied'))
    return True



def doc2vec_transformation_v01(df=Variable.get('ml_cord19_small_subset')):
    dataframe = pd.read_csv(df)
    print(f'::::::: PRIOR \n type {type(dataframe)} \n shape {dataframe.shape} \n info {dataframe.info()}')
    list_id = list(dataframe["cord_uid"])
    list_def = list(dataframe['abstract_processed'])

    tagged_data = [TaggedDocument(words=word_tokenize(term_def.lower()), tags=[list_id[i]]) for i, term_def in enumerate(list_def)]
    
    max_epochs = int(Variable.get('ml_model_doc2vec_max_epoch'))
    vec_size = int(Variable.get('ml_model_doc2vec_vec_size'))
    alpha = float(Variable.get('ml_model_doc2vec_alpha'))

    model = Doc2Vec(vector_size=vec_size,
                    alpha=alpha, 
                    min_alpha=0.00025,
                    min_count=1,
                    dm=1)

    model.build_vocab(tagged_data)
    
    
    for epoch in range(max_epochs):
        if epoch % 100 == 0:
            print(':::::::iteration {0}'.format(epoch))
        model.train(tagged_data,
                    total_examples=model.corpus_count,
                    epochs=model.epochs)
        model.alpha -= 0.0002
        model.min_alpha = model.alpha

    model.save(Variable.get('ml_model_doc2vec_output'))
    
    print(f"::::::: {model.wv.most_similar('virus')}")
    print(f'::::::: AFTER ')
    return True

def doc2vec_transformation_v01_subset():
    doc2vec_transformation_v01(Variable.get('ml_cord19_small_subset'))
    return True

