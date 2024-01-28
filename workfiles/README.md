# RKZoomCamp2024

# to show all containers including stopped
sudo docker ps -a

# to remove container
docker rm <containerid>


docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
  

 CREATE TABLE "yellow_taxi_data" (
"VendorID" INTEGER,
  "tpep_pickup_datetime" TIMESTAMP,
  "tpep_dropoff_datetime" TIMESTAMP,
  "passenger_count" INTEGER,
  "trip_distance" REAL,
  "RatecodeID" INTEGER,
  "store_and_fwd_flag" TEXT,
  "PULocationID" INTEGER,
  "DOLocationID" INTEGER,
  "payment_type" INTEGER,
  "fare_amount" REAL,
  "extra" REAL,
  "mta_tax" REAL,
  "tip_amount" REAL,
  "tolls_amount" REAL,
  "improvement_surcharge" REAL,
  "total_amount" REAL,
  "congestion_surcharge" REAL
)




pgcli -h localhost -p 5432 -u root -d ny_taxi
\dt
select 1
select count(1) from yellow_taxi_data

wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
gzip -d yellow_tripdata_2021-01.csv.gz
pip install sqlalchemy
sudo pip install psycopg2

jupyter notebook

# pgadmin block

docker pull dpage/pgadmin4

docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4

  # network

  docker network create pg-network

  docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name=pg-database \
  postgres:13

  docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name=pg-admin \
  dpage/pgadmin4

  # convert to py script

  jupyter nbconvert --to=script upload-data.ipynb

  # data pipeline

  URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
  
  # data ingestion pipeline - standalone 

  python ingest_data.py \
  --user=root \
  --pwd=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --tblnm=yellow_taxi_trips \
  --url=${URL}

# builds the image as specified in Dockerfile
docker build -t taxi_ingest:v001 .

# runs the dockerized version of data ingestion script (remember to define URL as required)

docker run -it \
--network=pg-network \
taxi_ingest:v001 \
--user=root \
--pwd=root \
--host=pg-database \
--port=5432 \
--db=ny_taxi \
--tblnm=yellow_taxi_trips \
--url=${URL}

# docker compose 
# start running below commands once the yaml file config is done
docker-compose up
# run in detach mode so the terminal is release for use
docker-compose up -d 
# exit docker compose
docker-compose down


