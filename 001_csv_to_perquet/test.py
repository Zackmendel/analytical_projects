import pandas as pd 
from sqlalchemy import create_engine
from tqdm.auto import tqdm 

dtypes = {
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

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

df = pd.read_csv(
    f'{prefix}yellow_tripdata_2021-01.csv.gz',
    dtype=dtypes,
    parse_dates=parse_dates,
    nrows=10
)


engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')

df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


df_iter = pd.read_csv(
    f'{prefix}yellow_tripdata_2021-01.csv.gz',
    dtype=dtypes,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)

for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(
        name='yellow_taxi_data',
        con=engine,
        if_exists='append',
        index=False
    )

    print("Inserted:", len(df_chunk))