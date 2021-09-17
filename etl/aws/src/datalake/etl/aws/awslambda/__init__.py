# ========================= #
# AWS LAMBDA MODULE         #
# ========================= #

import os

def is_lambda() -> bool:
    return os.getenv("AWS_LAMBDA_FUNCTION_NAME") != None