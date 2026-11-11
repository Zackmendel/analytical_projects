import pandas as pd 
from sqlalchemy import create_engine

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/'

df = pd.read_csv(
    f'{prefix}taxi_zone_lookup.csv'
    )

engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')

df.to_sql(
    name='taxi_zone_lookup', 
    con=engine, 
    if_exists='append'
)

print(f'Inserted {len(df)} zone lookup data')