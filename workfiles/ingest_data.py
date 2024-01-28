#!/usr/bin/env python
# coding: utf-8

import gzip
import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
#from time import time

def main(params):
    user = params.user
    pwd = params.pwd
    host = params.host
    port = params.port
    db = params.db
    tblnm = params.tblnm
    url = params.url
    csv_name_gz = 'output.csv.gz'
    csv_name = 'output.csv'
    #for taxi zone lookup csv
    os.system(f"wget {url} -O {csv_name}")
    #for yello and freen taxi daata csv.gz
    #download csv
    #os.system(f"wget {url} -O {csv_name_gz}")
    #unzip .gz file
    #with gzip.open(csv_name_gz, 'rt', newline='') as csv_file:
    #    csv_data = csv_file.read()
    #    with open(csv_name, 'wt') as out_file:
    #        out_file.write(csv_data)

    engine = create_engine(f'postgresql://{user}:{pwd}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    # yellow taxi trip
    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    # green taxi trip
    # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # Create table
    df.head(n=0).to_sql(name=tblnm, con=engine, if_exists='replace')

    df.to_sql(name=tblnm, con=engine, if_exists='append')

    while True:
        df = next(df_iter)
        # yellow taxi trip
        # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        # green taxi trip
        # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        # df.to_sql(name=tblnm, con=engine, if_exists='append')
        print('inserted another chunk...')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest csv data to postgres')
    
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--pwd', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--tblnm', help='table name where the csv data is copied to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
