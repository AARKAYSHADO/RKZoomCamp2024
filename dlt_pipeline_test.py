import pyarrow as pa
import pandas as pd
import dlt
import duckdb

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet"



df = pa.parquet.read_pandas(url, columns=None)

df.show()

#ny_taxi_pipeline = dlt.pipeline(destination='duckdb', dataset_name='green_taxi')

#info = ny_taxi_pipeline.run(some_source(), loader_file_format="parquet")
#print('Job completed successfully for day')

