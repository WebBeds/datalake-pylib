from datetime import datetime

import argparse

def valid_aws_datetime(s):
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        msg = "Not a valid date: '{}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def valid_epoch(s):
    try:
        return datetime.utcfromtimestamp(int(s))
    except ValueError:
        msg = "Not a valid date: '{}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

def trunc_datetime_to_seconds(dt: datetime):
    return datetime.fromisoformat(dt.strftime("%Y-%m-%dT%H:%M:00"))

def get_now(timestamp):
    if timestamp == 0:
        return datetime.utcnow()
    try:
        return datetime.utcfromtimestamp(timestamp)
    except:
        raise Exception("Timestamp is not an epoch")