from datalake.sdk.exception import SDKException

class Session:
    _environment: str = None
    _auth_token: str = None

    def __init__(self, environment, auth_token):
        self._environment = environment.upper()
        self._auth_token = auth_token

    def get_environment(self):
        return self._environment

    def get_hostname(self):
        if self._environment == "PROD":
            return "https://api.wbcaspian.net"
        elif self._environment == "DEV":
            return "https://api.dev.wbcaspian.net"
        else:
            raise SDKException("invalid environment")
    
    def __token__(self):
        return self._auth_token