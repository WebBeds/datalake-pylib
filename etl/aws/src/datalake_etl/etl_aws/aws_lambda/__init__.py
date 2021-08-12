# ========================= #
# AWS LAMBDA MODULE         #
# ========================= #

import os

def isLambda() -> bool:
    return os.getenv("AWS_LAMBDA_FUNCTION_NAME") != None