#!/usr/bin/env python3

import pandas as pd
import psycopg2 as pg

def get_df(query: str, credentials: str, verbose: bool = False) -> pd.DataFrame:
    """
    Returns dataframe with the data that returns the given query.
    """
    conn = pg.connect(credentials)
    cursor = conn.cursor()

    df = pd.read_sql(query, conn)

    if verbose:
        print(f"Got {len(df)} rows.")

    cursor.close()
    conn.close()

    return df

def get_columns(db, schema, table, credentials: str, verbose: bool = False):
    query = f"""SELECT column_name FROM information_schema.columns WHERE table_catalog='{db}' AND table_schema='{schema}' AND table_name='{table}';"""

    df = get_df(
        query=query,
        credentials=credentials,
        verbose=verbose
    )

    columns = list(df['column_name'])

    if verbose:
        print(f"Postgres fields ({len(columns)}): {columns}.")

    return columns