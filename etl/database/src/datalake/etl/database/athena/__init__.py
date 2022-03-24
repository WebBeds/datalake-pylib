#!/usr/bin/env python3

import time
import pandas as pd
from pyathena import connect

# Support for older versions
try:
    from pyathena.pandas_cursor import PandasCursor
except ModuleNotFoundError:
    from pyathena.pandas.cursor import PandasCursor

def add_partition(db: str, table: str, region: str, tmp_results: str, partitions: str, verbose: bool):
    # Add partition if not exists
    query = f"ALTER TABLE {db}.{table} ADD IF NOT EXISTS PARTITION ({partitions});"

    if verbose:
        print(f"Query add partition: {query}")

    # Send query to Athena.
    conn = connect(
        s3_staging_dir="s3://{}".format(tmp_results),
        region_name=region
    )
    
    pd.read_sql(query, conn)

def cursor_as_pandas_retry(cursor, query, retries=3, sleep=3):
    while retries > 0:
        try:
            return cursor.execute(query).as_pandas()
        except Exception as e:
            time.sleep(sleep)
            retries -= 1
            print(f"Retrying {retries} query {query}: {str(e)}")

def get_df(query: str, region: str, tmp_results: str, verbose: bool = False) -> pd.DataFrame:

    if verbose:
        print(query)

    cursor = connect(
        s3_staging_dir="s3://{}".format(tmp_results),
        region_name=region
    ).cursor(PandasCursor)

    df = cursor.execute(query).as_pandas()

    return df