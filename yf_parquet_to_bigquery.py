#!/usr/bin/env python
# coding: utf-8

import argparse
import pyspark
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()

parser.add_argument('--year', required=True)
parser.add_argument('--input', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

year = int(args.year)
output = args.output
input_loc = args.input
files = ['META', 'AMD', 'AMZN', 'GOOGL']

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-west1-322862734435-1zqnkr0s')


for f in files:
    df_pq = spark.read.parquet(f'{input_loc}/{f}/')
    df_pq.registerTempTable(f'yf_{f}')

df_result = spark.sql("""
SELECT
    'META' as Org, Date, Open, High, Low, Close, Volume
FROM
    yf_META

union

SELECT
    'AMD' as Org, Date, Open, High, Low, Close, Volume
FROM
    yf_AMD

union

SELECT
    'AMZN' as Org, Date, Open, High, Low, Close, Volume
FROM
    yf_AMZN

union

SELECT
    'GOOGL' as Org, Date, Open, High, Low, Close, Volume
FROM
    yf_GOOGL
    
""")


df_result.write.format('bigquery') \
    .option('table', output) \
    .save()

