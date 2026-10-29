import pandas as pd 
from datetime import datetime 
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Declare data types because pandas find it hard to automatically detect the data type in the raw csv file
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

# Date fields in the dataset to be parsed
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# Declare root url
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

# IRead sample data
df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)

# Create engine to connect to postgres
engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')

# Get DDL Schema
print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

# Create empty table
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

# # Create an iterative dataframe to ingest data into table in batches
df_iter = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)

for df_chunk in tqdm(df_iter):
    # Insert chunk
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append",
        index=False
    )

    print("Inserted:", len(df_chunk))

# df.head(5)
# df.info()
# df.dtypes