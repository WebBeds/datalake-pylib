from datalake.sdk import Client
from .hotel import HotelFetcher

class MasterFetcher:

    _client: Client = None
    _fetchers: dict = None

    def __init__(self, client: Client):
        self._client = client
        self._fetchers = {}

    def hotels(self) -> HotelFetcher:
        if 'hotels' not in self._fetchers:
            self._fetchers['hotels'] = HotelFetcher(self._client._session)
        return self._fetchers['hotels']
