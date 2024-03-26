#!/usr/bin/env python
# coding: utf-8

# In[12]:


import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import dlt
import duckdb
import os


# In[6]:


url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet"
file_name = "output.parquet"
os.system(f"wget {url} -O {file_name}")


# In[7]:


df = pa.parquet.read_pandas(file_name, columns=None)


# In[13]:


parquet_file = pq.ParquetFile('output.parquet')


# In[14]:


parquet_file.read()


# In[ ]:




