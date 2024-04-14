#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[4]:


import os


# In[5]:


import gzip


# In[6]:


os.system("wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz -O output.csv.gz")


# In[7]:


with gzip.open('output.csv.gz', 'rt', newline='') as csv_file:
        csv_data = csv_file.read()
        with open('output.csv', 'wt') as out_file:
            out_file.write(csv_data)


# In[8]:


pd.__version__


# In[10]:


df = pd.read_csv('output.csv', nrows=100)


# In[11]:


df


# In[5]:


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


# In[11]:


from sqlalchemy import create_engine


# In[12]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[13]:


engine.connect()


# In[14]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[16]:


df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)


# In[17]:


df = next(df_iter)


# In[18]:


len(df)


# In[19]:


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


# In[23]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[24]:


get_ipython().run_line_magic('time', "df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')")


# In[26]:


while True:
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
    print('inserted another chunk..., took %.3f second')


# In[ ]:




