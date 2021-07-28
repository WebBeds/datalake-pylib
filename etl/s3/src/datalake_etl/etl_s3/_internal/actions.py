#!/usr/bin/env python3

import boto3
import os

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