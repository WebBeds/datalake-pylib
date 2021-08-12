# ========================= #
# SECRET MANAGER LIBRARY    #
# ========================= #

from boto3 import client
from json import loads

def get_secret(name: str, region_name: str,s3_client = None) -> dict:
    """
    Given secret name and region will return the Secrets that contain the service and all the secrets trace
    for access for more information but normally will be ignored.
    """
    
    # If client is None will create a new one.
    if s3_client is None:
        s3_client = client(
            'secretsmanager' # Service
            ,region_name=region_name # Region name
        )

    # Get the secrets by the id, the name.
    values = client.get_secret_value(
        SecretId=name
    )

    # Return the secrets and the all the information of the service.
    return loads(values["SecretString"]), values