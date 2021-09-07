# ========================= #
# SECRET MANAGER LIBRARY    #
# ========================= #

from boto3 import client
from json import loads

def get_secret(name: str, region_name: str,aws_client = None) -> dict:
    """
    Given secret name and region will return the Secrets that contain the service and all the secrets trace
    for access for more information but normally will be ignored.
    """
    
    # If client is None will create a new one.
    if aws_client is None:
        aws_client = client(
            'secretsmanager' # Service
            ,region_name=region_name # Region name
        )

    # Get the secrets by the id, the name.
    values = aws_client.get_secret_value(
        SecretId=name
    )

    # Return the secrets and the all the information of the service.
    return loads(values["SecretString"]), values