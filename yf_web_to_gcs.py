#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from google.cloud import storage

credentials_location = '/home/codespace/.config/gcloud/application_default_credentials.json'
#credentials_location = '/config/application_default_credentials.json'
#GOOGLE_APPLICATION_CREDENTIALS = '/config/application_default_credentials.json'
bucket = os.environ.get("GCP_GCS_BUCKET", "dataenggzoomcamp_proj_yf")

parser = argparse.ArgumentParser()

parser.add_argument('--year', required=True)
parser.add_argument('--file', required=True)

args = parser.parse_args()

year = int(args.year)
file = args.file

def upload_to_gcs(bucket, gcs_file, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(gcs_file)
    blob.upload_from_filename(local_file)

local_file = f'{file}_{year}.csv'
gcs_file = f'raw/{file}/{file}_{year}.csv'
# upload it to gcs 
print(f"Writing to GCS - {gcs_file}")
upload_to_gcs(bucket, gcs_file, local_file)

