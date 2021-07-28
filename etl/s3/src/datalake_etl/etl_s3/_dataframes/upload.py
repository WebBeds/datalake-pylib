#!/usr/bin/env python3

# ========================= #
# SERVICE MODULE            # 
# ========================= #

import boto3
import tempfile
import pandas as pd
import pyarrow.parquet as pq
from .get import get_parquet_from_s3

def upload_file_(file, bucket, key, verbose, dry):
    """
    Upload a file to S3, specifying the file path, the Bucket and key values.
    If dry is enabled, will not upload to S3 the changes.
    """
    if verbose:
        print(f"Uploading file: bucket={bucket}, key={key}")
    
    if dry:
        return

    s3client = boto3.client('s3')
    s3client.upload_file(file, bucket, key)

def upsert_df(df, bucket, key, primary_key, verbose, dry, delete_field=None):

    df_current = get_parquet_from_s3(bucket, key, verbose)
    df = pd.concat([df_current, df], ignore_index=True)
    df = df.drop_duplicates(subset=primary_key, keep='last')

    if delete_field:
        df.drop(df[df[delete_field]].index, inplace=True)

    with tempfile.NamedTemporaryFile() as tmp:
        df.to_parquet(tmp.name, use_dictionary=True, compression='gzip')
        upload_file_(tmp.name, bucket, key, verbose, dry)

    return

def upload_parquet(df: pd.DataFrame, bucket, key, verbose=False, dry=False, s3_client=None):
    if dry:
        return

    # TODO: remove check of error
    if 'error' in df.columns:
        # clean nan and None strings
        df['error'] = df['error'].astype(str).map(lambda x: None if x in ('None', 'nan') else x)
    with tempfile.NamedTemporaryFile() as tmp:
        # TODO: check to use:
        # coerce_timestamps='ms'
        # compression='gzip'
        df.to_parquet(tmp.name, allow_truncated_timestamps=True)
        # PARANOIC check
        pfile = pq.ParquetFile(tmp.name)
        if pfile.metadata.num_rows != df.shape[0]:
            print(df.shape, pfile.metadata)
            raise Exception('Parquet file wrong')
        if verbose:
            print("Uploading key {} to bucket {}".format(key, bucket))
        if not s3_client:
            s3_client = boto3.session.Session().client('s3')
        s3_client.upload_file(tmp.name, bucket, key)