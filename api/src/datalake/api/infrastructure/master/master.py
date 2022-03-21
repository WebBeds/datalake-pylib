from datalake.sdk import Client
from .hotel import Hotels

class MasterFetcher:

    _client: Client = None
    _fetchers: dict = None

    def __init__(self, client: Client):
        self._client = client
        self._fetchers = {}

    def hotels(self) -> Hotels:
        if 'hotels' not in self._fetchers:
            self._fetchers['hotels'] = Hotels(self._client._session)
        return self._fetchers['hotels']
