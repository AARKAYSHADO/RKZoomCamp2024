#!/usr/bin/env python
# coding: utf-8

# In[11]:


#!/usr/bin/env python
# coding: utf-8

# in order to specify start date and 
# end date we need datetime package
import datetime
import os
import argparse
import pandas as pd
import yfinance as yahooFinance
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from google.cloud import storage

credentials_location = '/home/codespace/.config/gcloud/application_default_credentials.json'
bucket = os.environ.get("GCP_GCS_BUCKET", "dataenggzoomcamp_proj_yf")

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)


# In[3]:


sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")


# In[4]:


#year = 2023
parser = argparse.ArgumentParser()
parser.add_argument('--year', required=True)
args = parser.parse_args()
year = int(args.year)

# startDate , as per our convenience we can modify
startDate = datetime.datetime(year, 1, 1)
 
# endDate , as per our convenience we can modify
endDate = datetime.datetime(year, 12, 31)


# In[7]:


spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()


# In[8]:


# Read and print the stock tickers that make up S&P500
tickers = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
tickers.to_csv("list_500_companies.csv")

files = ['META', 'AMZN', 'GOOGL', 'AMD']
schema = types.StructType([
    types.StructField('Date', types.DateType(), True),
    types.StructField('Open', types.FloatType(), True),
    types.StructField('High', types.FloatType(), True),
    types.StructField('Low', types.FloatType(), True),
    types.StructField('Close', types.FloatType(), True),
    types.StructField('Volume', types.IntegerType(), True),
    types.StructField('Dividends', types.FloatType(), True),
    types.StructField('Stock Splits', types.FloatType(), True)
])


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


for f in files:
    stock = yahooFinance.Ticker(f)
    # pass the parameters as the taken dates for start and end
    hist = stock.history(start=startDate,end=endDate)
    filename = f'{f}_{year}.csv'
    print(f'Writing to csv file - {filename}')
    hist.to_csv(f'{f}_{year}.csv')
    print(filename)
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(f'{f}_{year}.csv')
    df = df.repartition(24)
    pq_path = f'yf/pq/{f}/{year}/'
    print(f"Writing to pq file - {pq_path}")
    df.write.parquet(pq_path, mode='overwrite')
    # upload it to gcs 
    print(f"Writing to GCS - pq/{f}/{year}")
    upload_to_gcs(bucket, f"pq/{f}/{year}", {pq_path})
    df_pq = spark.read.parquet(f'gs://dataenggzoomcamp_proj_yf/pq/{f}/*')
    df_pq.registerTempTable(f'yf_{f}')

df_result = spark.sql("""
SELECT
    'META', Date, Open, High, Low, Close, Volume, Dividends, Stock Splits
FROM
    yf_META

union

SELECT
    'AMZN', Date, Open, High, Low, Close, Volume, Dividends, Stock Splits
FROM
    yf_AMZN

union

SELECT
    'GOOGL', Date, Open, High, Low, Close, Volume, Dividends, Stock Splits
FROM
    yf_GOOGL

union

SELECT
    'AMD', Date, Open, High, Low, Close, Volume, Dividends, Stock Splits
FROM
    yf_AMD

""")






