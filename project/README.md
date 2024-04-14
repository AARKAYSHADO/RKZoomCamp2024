## Data Engineering Zoomcamp 2024 

## Course Project
American Stock Exchange - Stock Market Data Pipeline

## Objective
Build a data pipeline that can perform ETL to data warehouse hosted on Cloud platform and generate insights on large cap companies' common stocks traded on American Stock Exchange.

### Tools used
Terraform - To deploy infrastructure on GCS and Bigquery
Docker - To containerize data pipeline scripts
Spark - To perform data transformations 
GCS - Data lake used for processing
Bigquery - Data warehouse to store data
Looker Studio - Buils reports and derive insights form the data loaded

### Pre-requisites
Create a GCP account
Create a project in GCP
Create a service account for the project that has IAM access set up for GCS, Bigquery, Dataproc
Create keys and download the JSON file to your work folder 
Copy all the scripts listed below your work folder
  main.tf
  variables.tf
  yf_file_extraction.py
  yf_web_to_gcs.py
  yf_csv_to_parquet.py
  yf_parquet_to_bigquery.py
  Dockerfile_file_extraction
  Dockerfile_web_to_gcs

### Step 1 - Deploy infrastructure
Deploy google cloud services such as GCS and Bigquery which will be used in this project. 
Amend credentials, project, region to align to your GCP account in variables.tf file.

File location - /workspaces/RKZoomCamp2024
tf files - main.tf, variables.tf

Execute commands to deploy the infrastructure
terraform init
terraform plan
terraform apply

### Step 2 - Data extraction
YahooFinance API is used to extract common stocks traded on ASE. 
This data pipeline takes time period and company name as arguments. 

File location - /workspaces/RKZoomCamp2024
Docker image - Dockerfile_file_extraction
Data pipeline script - yf_file_extraction.py

Sample Docker image build & run commands (rename Dockerfile_file_extraction to Dockerfile)
docker build -t yf:pandas .
docker run -it yf:pandas --year=2023 --file=AMD
docker run -it yf:pandas --year=2023 --file=AMZN
docker run -it yf:pandas --year=2023 --file=GOOGL
docker run -it yf:pandas --year=2023 --file=META

Alternate (if not using Docker)
python yf_file_extraction.py \
    --year=2023 \
    --file=META

### Step 3 - Upload to GCS
Upload csv files to Google Cloud Storage for further processing. 
This data pipeline takes time period and company name as arguments. 

File location - /workspaces/RKZoomCamp2024
Docker image - Dockerfile_web_to_gcs
Data pipeline script - yf_web_to_gcs.py

Sample Docker image build & run commands (rename Dockerfile_web_to_gcs to Dockerfile)
docker build -t yf:gcs .
docker run -it yf:gcs --year=2023 --file=AMD
docker run -it yf:gcs --year=2023 --file=AMZN
docker run -it yf:gcs --year=2023 --file=GOOGL
docker run -it yf:gcs --year=2023 --file=META

Alternate (if not using Docker)
python yf_web_to_gcs.py \
    --year=2023 \
    --file=META

### Step 4 - csv to parquet
Convert csv files to parquet format in GCS for optimized data processing

File location - /workspaces/RKZoomCamp2024
Data pipeline script - yf_csv_to_parquet.py

Execute
gsutil -m cp yf_csv_to_parquet.py gs://dataenggzoomcamp_proj_yf/code/yf_csv_to_parquet.py

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster-yf \
    --region=us-west1 \
    gs://dataenggzoomcamp_proj_yf/code/yf_csv_to_parquet.py \
    -- \
        --year=2023 \
        --file=META

(**repeat for AMD, AMZN, GOOGL)

### Step 5 - parquet to Bigquery
Perform data transformations in spark and load the results to Bigquery table 

File location - /workspaces/RKZoomCamp2024
Data pipeline script - yf_parquet_to_bigquery.py

Execute
gsutil -m cp yf_parquet_to_bigquery.py gs://dataenggzoomcamp_proj_yf/code/yf_parquet_to_bigquery.py

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster-yf \
    --region=us-west1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dataenggzoomcamp_proj_yf/code/yf_parquet_to_bigquery.py \
    -- \
        --year=2023 \
        --input=gs://dataenggzoomcamp_proj_yf/pq \
        --output=yf.report2023

create or replace table `dltdemo.yf.report2023_prt` 
partition by Date
cluster by Org as 
(select * from dltdemo.yf.report2023);

### Step 6 - Build dashboard using LookerStudio

![Screenshot of the Stock report](https://lookerstudio.google.com/s/nPRg5eRlYAE)



