#!/usr/bin/env python
# coding: utf-8

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

parser = argparse.ArgumentParser()

parser.add_argument('--year', required=True)
parser.add_argument('--file', required=True)

args = parser.parse_args()

year = int(args.year)
file = args.file

schema = types.StructType([
    types.StructField('Date', types.DateType(), True),
    types.StructField('Open', types.FloatType(), True),
    types.StructField('High', types.FloatType(), True),
    types.StructField('Low', types.FloatType(), True),
    types.StructField('Close', types.FloatType(), True),
    types.StructField('Volume', types.IntegerType(), True),
    types.StructField('Dividends', types.FloatType(), True),
    types.StructField('StockSplits', types.FloatType(), True)
])

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(f'gs://dataenggzoomcamp_proj_yf/raw/{file}/*')
df = df.repartition(24)
pq_path = f'gs://dataenggzoomcamp_proj_yf/pq/{file}/'
print(f"Writing to pq file - {pq_path}")
df.write.parquet(pq_path, mode='overwrite')
