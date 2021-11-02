#!/usr/bin/env python3

# ========================= #
# DATAFRAMES MODULE         #
# ========================= #

import boto3
import io
import pandas as pd
from urllib import parse

def get_parquet_from_s3(bucket, key, verbose=False):
    """
    Get parquet from S3 and return it as a Pandas DataFrame.
    """
    key = parse.unquote(key)

    # Read file and create dataframe
    try:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    except s3.exceptions.NoSuchKey:
        df = pd.DataFrame()

    # Colum names to lowercase
    df.columns = df.columns.str.lower()
    return df

def get_csv_from_s3(bucket, key, verbose=False, compression='gzip', keep_default_na=True, sep=','):
    """
    Get CSV from S3 and return it as a Pandas DataFrame.
    """

    key = parse.unquote(key)

    try:
        # Connect to S3 and get object
        s3client = boto3.client('s3')
        obj = s3client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), dtype=str, compression=compression, keep_default_na=keep_default_na, sep=sep)
    except s3client.exceptions.NoSuchKey:
        df = pd.DataFrame()

    # Colum names to lowercase
    df.columns = df.columns.str.lower()

    return df

def get_json_from_s3(bucket, key, verbose=False) -> pd.DataFrame:
    key = parse.unquote(key)

    # Read file and create dataframe
    url = "s3://" + bucket + "/" + key
    df = pd.read_json(url, lines=True, dtype=False)

    # Colum names to lowercase
    df.columns = df.columns.str.lower()
    return df

def get_excel_from_s3(bucket, key, verbose=False) -> pd.DataFrame:
    
    key = parse.unquote(key)

    # Read file and create dataframe
    url = "s3://" + bucket + "/" + key
    df = pd.read_excel(url, dtype=str, na_filter=False)

    # Colum names to lowercase
    df.columns = df.columns.str.lower()
    return df