# cord19-processing
NLP on CORD19 dataset using AWS infrastructure and Airflow as orchestrator

### Objectives
-	Learning! 
-	Build a Machine Learning data pipeline for NLP processing 
-	The pipeline should be easy scalable for small and large data sets
-	The pipeline should allow for adding new processing steps and also new algorithms 
-	The pipeline should be 100% cloud based 
-	The pipeline should be deployable as code using Infrastructure-as-Code paradigm 

### Constraints
-	This project is an educational project deployed in AWS using my personal account (I need to keep balance between the number, volume of resources AND also cost)
-	The structure of the project needs to be easy to understand (for example Infrastructure as Code might be better served as a CloudFormation template or a CDK stack but I choose to use Jupyter notebooks and boto3 because it is easier to convey the steps)
-	Some of the resource’s deployment might seem redundant or unnecessary (for instance the Redshift cluster can be replaced by directly accessing the s3 data lake) but the goal is learning and adding a more complex infrastructure is a good way to achieve this 
-	Some of the resource’s configuration can be made much more scalable (for instance Airflow should be configured using maybe the MWAA service or with a MySQL and airflow kubernetes executor) but I needed to find the proper balance between time allocated, cost and the final goal. 
-	Further iterations on this project might ameliorate some of these issues.

### The code for this project
-	Infrastructure deployment – folder infrastructure-deployment 
-	Exploratory data analysis – folder exploratory-data-analysis
-	Evaluating ML algorithms – folder evaluating-ml-algorithms
-	Airflow DAGs code – folder airflow-dags-code 
-	Reports – folder reports
-	Data – folder data
-	Images – folder images
