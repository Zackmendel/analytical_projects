#!/usr/bin/env python
"""Combined ingestion script for NYC yellow taxi, green taxi, and zone lookup datasets."""

import argparse
import getpass
import os
import re
import sys

import pandas as pd
from sqlalchemy import URL, create_engine
from tqdm.auto import tqdm

YELLOW_URL_TEMPLATE = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    "yellow_tripdata_{year}-{month:02d}.csv.gz"
)
GREEN_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "green_tripdata_{year}-{month:02d}.parquet"
)
ZONE_LOOKUP_URL = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/"
    "taxi_zone_lookup.csv"
)

YELLOW_DTYPE = {
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
    "congestion_surcharge": "float64",
}

YELLOW_PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_table_name(name: str) -> str:
    """Validate that a table name is a safe SQL identifier."""
    if not IDENTIFIER_RE.fullmatch(name):
        raise argparse.ArgumentTypeError(
            f"Invalid table name {name!r}: must start with a letter/underscore "
            "and contain only letters, digits, and underscores."
        )
    return name


def validate_month(value: str) -> int:
    """Validate month is between 1 and 12."""
    month = int(value)
    if not 1 <= month <= 12:
        raise argparse.ArgumentTypeError("Month must be between 1 and 12.")
    return month


def validate_positive_int(value: str) -> int:
    """Validate integer is strictly positive."""
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Value must be a positive integer.")
    return parsed


def ingest_yellow_taxi(engine, year: int, month: int, table_name: str, chunksize: int) -> None:
    """Download and ingest NYC yellow taxi CSV.gz data in chunks."""
    url = YELLOW_URL_TEMPLATE.format(year=year, month=month)
    print(f"Ingesting yellow taxi data ({year}-{month:02d}) into '{table_name}'...")

    df_iter = pd.read_csv(
        url,
        dtype=YELLOW_DTYPE,
        parse_dates=YELLOW_PARSE_DATES,
        iterator=True,
        chunksize=chunksize,
    )

    total_rows = 0
    first = True
    for df_chunk in tqdm(df_iter, desc=table_name, unit="chunk"):
        if first:
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False,
            )
            first = False

        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )
        total_rows += len(df_chunk)

    print(f"Inserted {total_rows:,} rows into '{table_name}'.")


def ingest_green_taxi(engine, year: int, month: int, table_name: str, chunksize: int) -> None:
    """Download and ingest NYC green taxi Parquet data in chunks."""
    url = GREEN_URL_TEMPLATE.format(year=year, month=month)
    print(f"Ingesting green taxi data ({year}-{month:02d}) into '{table_name}'...")

    df = pd.read_parquet(url)
    df.head(0).to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
    )

    for start in tqdm(range(0, len(df), chunksize), desc=table_name, unit="chunk"):
        df.iloc[start : start + chunksize].to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )

    print(f"Inserted {len(df):,} rows into '{table_name}'.")


def ingest_zones(engine, table_name: str) -> None:
    """Download and ingest the NYC taxi zone lookup table."""
    print(f"Ingesting taxi zone lookup data into '{table_name}'...")
    df_zone = pd.read_csv(ZONE_LOOKUP_URL)
    df_zone.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
    )
    print(f"Inserted {len(df_zone):,} rows into '{table_name}'.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest NYC yellow taxi, green taxi, and zone lookup datasets into PostgreSQL."
    )
    parser.add_argument(
        "--pg-user",
        default=os.getenv("PGUSER", "root"),
        help="PostgreSQL user (default: $PGUSER or 'root')",
    )
    parser.add_argument(
        "--pg-pass",
        default=os.getenv("PGPASSWORD") or os.getenv("POSTGRES_PASSWORD"),
        help="PostgreSQL password (defaults to $PGPASSWORD/$POSTGRES_PASSWORD, or prompts)",
    )
    parser.add_argument(
        "--pg-host",
        default=os.getenv("PGHOST", "localhost"),
        help="PostgreSQL host (default: $PGHOST or 'localhost')",
    )
    parser.add_argument(
        "--pg-port",
        type=int,
        default=int(os.getenv("PGPORT", "5432")),
        help="PostgreSQL port (default: $PGPORT or 5432)",
    )
    parser.add_argument(
        "--pg-db",
        default=os.getenv("PGDATABASE", "ny_taxi"),
        help="PostgreSQL database name (default: $PGDATABASE or 'ny_taxi')",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=["all", "yellow", "green", "zones"],
        default=["all"],
        help="Which datasets to ingest (default: all)",
    )
    parser.add_argument("--yellow-year", type=int, default=2021, help="Yellow taxi year (default: 2021)")
    parser.add_argument("--yellow-month", type=validate_month, default=1, help="Yellow taxi month (default: 1)")
    parser.add_argument(
        "--yellow-table",
        type=validate_table_name,
        default="yellow_taxi_data",
        help="Target table for yellow taxi data (default: yellow_taxi_data)",
    )
    parser.add_argument("--green-year", type=int, default=2025, help="Green taxi year (default: 2025)")
    parser.add_argument("--green-month", type=validate_month, default=11, help="Green taxi month (default: 11)")
    parser.add_argument(
        "--green-table",
        type=validate_table_name,
        default="green_tripdata",
        help="Target table for green taxi data (default: green_tripdata)",
    )
    parser.add_argument(
        "--zone-table",
        type=validate_table_name,
        default="taxi_zone_lookup",
        help="Target table for zone lookup data (default: taxi_zone_lookup)",
    )
    parser.add_argument(
        "--chunksize",
        type=validate_positive_int,
        default=100_000,
        help="Rows per insert batch (default: 100000)",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    pg_pass = args.pg_pass
    if pg_pass is None:
        pg_pass = getpass.getpass(f"PostgreSQL password for {args.pg_user}@{args.pg_host}: ")

    # TODO(security): Local Docker Compose dev setup connects without TLS and uses the
    # container's root user. In production, use a least-privilege DB role and require TLS/mTLS.
    connection_url = URL.create(
        drivername="postgresql+psycopg",
        username=args.pg_user,
        password=pg_pass,
        host=args.pg_host,
        port=args.pg_port,
        database=args.pg_db,
    )
    engine = create_engine(connection_url)

    selected = {"yellow", "green", "zones"} if "all" in args.datasets else set(args.datasets)

    try:
        if "zones" in selected:
            ingest_zones(engine, args.zone_table)
        if "yellow" in selected:
            ingest_yellow_taxi(
                engine,
                year=args.yellow_year,
                month=args.yellow_month,
                table_name=args.yellow_table,
                chunksize=args.chunksize,
            )
        if "green" in selected:
            ingest_green_taxi(
                engine,
                year=args.green_year,
                month=args.green_month,
                table_name=args.green_table,
                chunksize=args.chunksize,
            )
    finally:
        engine.dispose()

    print("All selected datasets ingested successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
