# ========================= #
# AWS LAMBDA MODULE         #
# ========================= #

import urllib

def get_s3_trigger_object_bucket(event: dict) -> str:
    """
    Returns the string which represents the bucket name of the object uploaded to s3 that originated the lambda function invokation.
    """
    return event['Records'][0]['s3']['bucket']['name']


def get_s3_trigger_object_key(event: dict) -> str:
    """
    Returns the string which represents the key of the object uploaded to s3 that originated the lambda function invokation.
    """
    return urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

def get_s3_trigger_object_account(event: dict) -> str:
    """
    Returns the string which represents the account id of the lambda function invokation.
    """
    return event['Records'][0]['userIdentity']['principalId']