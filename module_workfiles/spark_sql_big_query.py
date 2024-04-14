#!/usr/bin/env python
# coding: utf-8

# In[11]:


#!/usr/bin/env python
# coding: utf-8

# in order to specify start date and 
# end date we need datetime package
#import datetime
#import argparse
#import pyspark
#from pyspark.sql import SparkSession

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

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

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

parser = argparse.ArgumentParser()

parser.add_argument('--year', required=True)
parser.add_argument('--input', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

year = int(args.year)
output = args.output
input_loc = args.input

# startDate , as per our convenience we can modify
startDate = datetime.datetime(year, 1, 1)
 
# endDate , as per our convenience we can modify
endDate = datetime.datetime(year, 12, 31)

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-west1-322862734435-1zqnkr0s')

def upload_to_gcs(bucket, gcs_folder, local_folder):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    #blob = bucket.blob(object_name)
    #blob.upload_from_filename(local_file)

    from os import listdir
    from os.path import isfile, join
    files = [f for f in listdir(local_folder) if isfile(join(local_folder, f))]
    #files = ['part-00000-f0df2b0f-4b5b-4d41-8675-dad7e879e5a8-c000.snappy.parquet', 'part-00001-f0df2b0f-4b5b-4d41-8675-dad7e879e5a8-c000.snappy.parquet']
    for file in files:
        print(file)
        local_file = local_folder + file
        print(local_file)
        gcs_file = gcs_folder + file
        print(gcs_file)
        blob = bucket.blob(gcs_file)
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
    gcs_path = f'pq/{f}/{year}/'
    print(f"Writing to pq file - {pq_path}")
    df.write.parquet(pq_path, mode='overwrite')
    # upload it to gcs 
    print(f"Writing to GCS - {gcs_path}")
    upload_to_gcs(bucket, gcs_path, pq_path)
    df_pq = spark.read.parquet(f'gs://dataenggzoomcamp_proj_yf/pq/{f}/*')
    df_pq.registerTempTable(f'yf_{f}')

df_result = spark.sql("""
SELECT
    'META', Date, Open, High, Low, Close, Volume
FROM
    yf_META

union

SELECT
    'AMZN', Date, Open, High, Low, Close, Volume
FROM
    yf_AMZN

union

SELECT
    'GOOGL', Date, Open, High, Low, Close, Volume
FROM
    yf_GOOGL

union

SELECT
    'AMD', Date, Open, High, Low, Close, Volume
FROM
    yf_AMD
    
""")

#df_pq = spark.read.parquet(input_loc)
#df_pq.registerTempTable('yf_all')

#df_result = spark.sql("""
#SELECT
#    Date, Volume, High
#FROM
#    yf_all
#WHERE 
#    High > 40.0
#""")

df_result.write.format('bigquery') \
    .option('table', output) \
    .save()

