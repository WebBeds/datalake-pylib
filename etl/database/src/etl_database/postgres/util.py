#!/usr/bin/env python3

from pandas import DataFrame

def drop_not_included_columns(df: DataFrame, columns):
    for column in df.columns:
        if column not in columns:
            df.drop(columns=column, inplace=True)
    return df

def get_credentials(host: str,port: str,database: str,user: str,password: str) -> str:
    """
    Return postgres like credentials by given parameters.
    """
    return f'host={host} port={port} dbname={database} user={user} password={password}'