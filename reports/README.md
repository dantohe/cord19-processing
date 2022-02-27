# Processing Workflow
This page provides some insight into the processing steps.  
The code for the dags and operators is in the following folder
[airflow-dags-code](https://github.com/dantohe/cord19-processing/tree/main/airflow-dags-code)      
Currently, there are three stages for processing (each phase with its own Airfloe DAG):
- a data preparation and pre-processing phase
- a data processing pipeline using a small data set - this phase proves the viability of the pipeline and also gives some estimations regarding the time necessary to process the entire data set
- the ML pipeline per se - providing the run of the pipeline using the entire data set

![](../images/all-dags.png)    

### Data pre-processing   
The following graph (derived from Airflow) explains the  steps taken for pre-processing the corpus:   
![](../images/preprocessing-01.png)    

The following image presents the Airflow tree of this processing
![](../images/preprocessing-02.png)    

The following image presents the the time it took for each task to finish the pre processing.
![](../images/preprocessing-03.png)    

### Data processing using a small subset of papers 
The following graph (derived from Airflow) explains the  steps taken for processing a small subset of the corpus (around 1500 papers)
![](../images/processing-small-01.png)    

The following image presents the Airflow tree of this small set processing
![](../images/processing-small-02.png)    

The following image presents the the time it took for each task to finish this small set processing.
![](../images/processing-small-03.png)    

### Data processing using the entire set of papers 
The following graph (derived from Airflow) explains the  steps taken for processing the corpus
![](../images/processing-01.png)    

The following image presents the Airflow tree of processing
![](../images/processing-02.png)    

The following image presents the the time it took for each task to finish the processing.
![](../images/processing-03.png)    
