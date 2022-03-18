class Session:
    _environment = None
    _auth_token = None
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
            raise Exception("Invalid environment")
    def get_auth_token(self):
        return self._auth_token