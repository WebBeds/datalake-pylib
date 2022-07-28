#!/usr/bin/env python3

import boto3
import os
import re
import awswrangler as wr

def move_file(origin_bucket: str, origin_key: str, destination_bucket: str, destination_key: str, delete: bool = True, verbose: bool = False, dry: bool = False, s3_client = None) -> None:
    """
    Copy origin bucket and key into the destination bucket and key, then if delete is true will remove the original file.

    :param origin_bucket: The original bucket to copy from.
    :param origin_key: The original key to copy from.
    :param destination_bucket: The destination bucket to make a copy of the origin.
    :param destionation_key: The destination key to make a copy of the origin.
    :param delete: Default enabled, will delete the original file after the copy operation completes (optional). Default: True
    :param verbose: Show a message indicating whether the copy operation will affect (optional). Default: False
    :param dry: Will not make any changes and dont copy anything or delete anything (optional). Default: False
    :param s3_client: Specify a S3 Resource to make the operations (optional). Default: None
    """

    if verbose:
        print(f"Moving CSV from Bucket={origin_bucket}, Key={origin_key} to Bucket={destination_bucket}, Key={destination_key}")
    
    if dry:
        return

    s3 = s3_client
    if s3_client == None:
        s3 = boto3.session.Session().resource('s3')

    # Copy from original
    s3.Object(
        destination_bucket
        ,destination_key
    ).copy_from(
        CopySource=os.path.join(origin_bucket,origin_key)
    )
    
    # Delete original
    if delete:
        s3.Object(origin_bucket,origin_key).delete()

def safe_delete(
    bucket: str,
    prefix: str,
    suffix: str,
    limit: int = 0,
    verbose: bool = False,
    dry: bool = False,
    session = None
):

    if not bucket or len(bucket) < 3:
        raise ValueError("Bucket name is required and must be at least 3 characters long.")
    if not prefix or len(prefix) < 3:
        raise ValueError("Prefix is required and must be at least 3 characters long.")
    if not suffix or len(suffix) < 3:
        raise ValueError("Suffix is required and must be at least 3 characters long.")

    path = f"s3://{bucket}/{prefix}"
    objs = wr.s3.list_objects(
        path=path,
        suffix=suffix,
        boto3_session=session
    )

    if len(objs) == 0:
        return
    if limit > 0 and len(objs) > limit:
        raise Exception(f"Too many objects found at {path}")

    regex = re.compile(f"{re.escape(prefix)}.*{re.escape(suffix)}$")
    for obj in objs:
        if not regex.search(obj):
            raise Exception(f"Object {obj} does not match pattern {prefix}*{suffix}")

    if verbose:
        print(f"Deleting {len(objs)} objects matching {prefix}*{suffix} on {path}, dry={dry}")

    if dry:
        return

    wr.s3.delete_objects(
        path=objs,
        boto3_session=session
    )