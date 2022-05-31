from ... import Fetcher
from .executions import Executions

class MPAFetcher(Fetcher):
    
    def executions(self) -> Executions:
        if 'executions' not in self._fetchers:
            self._fetchers['executions'] = Executions(self._client._session)
        return self._fetchers['executions']