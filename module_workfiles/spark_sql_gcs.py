#!/usr/bin/env python
# coding: utf-8

# In[11]:


#!/usr/bin/env python
# coding: utf-8

# in order to specify start date and 
# end date we need datetime package
import datetime
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

# startDate , as per our convenience we can modify
startDate = datetime.datetime(year, 1, 1)
 
# endDate , as per our convenience we can modify
endDate = datetime.datetime(year, 12, 31)

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-europe-west6-828225226997-fckhkym8')

df_pq = spark.read.parquet(input_loc)
df_pq.registerTempTable('yf_all')

df_result = spark.sql("""
SELECT
    Date, Volume, High
FROM
    yf_all
WHERE 
    High > 40.0
""")

df_result.coalesce(1).write.parquet(output, mode='overwrite')

