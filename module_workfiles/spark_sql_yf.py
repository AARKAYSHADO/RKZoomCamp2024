#!/usr/bin/env python
# coding: utf-8

# in order to specify start date and 
# end date we need datetime package
import datetime
import argparse
import pandas as pd
import yfinance as yahooFinance
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

year = 2023

# startDate , as per our convenience we can modify
startDate = datetime.datetime(year, 1, 1)
 
# endDate , as per our convenience we can modify
endDate = datetime.datetime(year, 12, 31)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

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

for f in files:
    stock = yahooFinance.Ticker(f)
    # pass the parameters as the taken dates for start and end
    hist = stock.history(start=startDate,end=endDate)
    hist.to_csv(f'{f}_{year}.csv')
    file_name = f'{f}_{year}.csv'
    print(file_name)
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(file_name)
    df = df.repartition(24)
    path = f'yf/{f}/{year}/'
    df.write.parquet(path, mode='overwrite')
    df = spark.read.parquet(path)
    df.registerTempTable(f'yf_{f}_{year}')
        
df_result = spark.sql("""
SELECT
    'META', Date, Volume
FROM
    yf_META_2023
WHERE 
    High > 40.0

UNION

SELECT
    'AMZN', Date, Volume
FROM
    yf_AMZN_2023
WHERE 
    High > 40.0

UNION 

SELECT
    'GOOGL', Date, Volume
FROM
    yf_GOOGL_2023
WHERE 
    High > 40.0

UNION 

SELECT
    'AMD', Date, Volume
FROM
    yf_AMD_2023
WHERE 
    High > 40.0

""")
        
df_result.coalesce(1).write.parquet('yf/out/', mode='overwrite')
