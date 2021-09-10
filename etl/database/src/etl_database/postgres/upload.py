#!/usr/bin/env python3

import csv
import numpy as np
import pandas as pd
import psycopg2 as pg
from io import StringIO
from psycopg2.extras import NamedTupleCursor


def upload_df(df: pd.DataFrame, table, filters, credentials: str, verbose: bool = False) -> None:
    
    sep = '|'

    df = df.replace(['\n', '\r', '\r\n', f'\\{sep}'], ' ', regex=True)

    conn = pg.connect(credentials)
    cursor = conn.cursor()

    where = ''
    if filters:
        where = f' WHERE {filters} '

    truncate_query = f"""DELETE FROM {table} {where};"""
    if verbose:
        print(truncate_query)
    cursor.execute(truncate_query)

    # Generate a CSV buffer
    csv_buffer = StringIO()
    df.to_csv(
        csv_buffer, sep=sep, na_rep='', header=False, index=False,
        quoting=csv.QUOTE_MINIMAL, quotechar='"', escapechar='\\', doublequote=False
    )
    csv_buffer.seek(0)

    # Insert data using COPY postgres command.
    cursor.copy_from(
        file=csv_buffer,
        table=table,
        columns=list(df.columns),
        sep=sep,
        null=''
    )

    conn.commit()
    cursor.close()
    conn.close()

    if verbose:
        print("DB finished. Inserted {} registers".format(len(df)))

def upsert_df(df: pd.DataFrame, database, schema, table, key,credentials: str, verbose: bool = False, dry: bool = False) -> None:

    df = df.replace({np.nan: None})

    columns = list(df.columns)
    arglist = df.to_dict(orient='records')

    fields = ','.join(columns)
    key_fields = ','.join(key)
    insert_placeholders = ', '.join('%%(%s)s' % f for f in columns)
    udpdate_placeholders = ', '.join('%s=%%(%s)s' % (f, f) for f in columns if f not in key)
    query = f"""INSERT INTO {schema}.{table} ({fields}) VALUES ({insert_placeholders})
        ON CONFLICT ({key_fields}) DO
        UPDATE SET {udpdate_placeholders};"""

    if verbose:
        print(query)

    if dry:
        return

    conn = pg.connect(
        credentials,
        cursor_factory=NamedTupleCursor
    )

    with conn:
        with conn.cursor() as cursor:
            pg.extras.execute_batch(cursor, query, arglist, page_size=1000)
    conn.close()
    
# TODO: Check this method to generify.
# 
# def upsert_df_inner(df: pd.DataFrame, database, schema, table, key, credentials: str, verbose: bool = False, dry: bool = False, delete_field=None):
#     # Only updates the fields included in the df and postgres table.

#     if delete_field:

#         df_to_delete = df[df[delete_field]]

#         if not df_to_delete.empty:
#             # Delete registers in the dataframe
#             df.drop(df[df[delete_field]].index, inplace=True)

#             # Update registers to delete as deleted in PG
#             upsert_df_inner(
#                 df=df_to_delete, 
#                 database=CFG.AWS_POSTGRES_DB, 
#                 schema=CFG.AWS_POSTGRES_SCHEMA, 
#                 table=TABLE_NAME, 
#                 key=TABLE_PRIMARY_KEY, 
#                 verbose=verbose, 
#                 dry=dry
#             )

#             # Delete registers marked as deleted in PG
#             delete_registers(
#                 table=TABLE_NAME, 
#                 filters=f"{delete_field}", 
#                 verbose=verbose
#             )

#     pg_columns = get_columns(
#         database,
#         schema,
#         table,
#         verbose
#     )

#     df_columns = list(df.columns)
#     upsert_columns = list(set.intersection(set(df_columns), set(pg_columns)))

#     df = drop_not_included_columns(df, upsert_columns)

#     upsert_df(
#         df,
#         database,
#         schema,
#         table,
#         key,
#         verbose,
#         dry
#     )