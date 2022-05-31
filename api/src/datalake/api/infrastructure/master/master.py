from .. import Fetcher
from .hotel import Hotels

class MasterFetcher(Fetcher):

    def hotels(self) -> Hotels:
        if 'hotels' not in self._fetchers:
            self._fetchers['hotels'] = Hotels(self._client._session)
        return self._fetchers['hotels']
