from datalake.sdk import Client

from .infrastructure.master import MasterFetcher

class APIClient(Client):
    
    def __register__(self) -> None:
        # Master Fetcher
        self._services['master'] = MasterFetcher(self)

    def get_resource(self, name: str):
        return super().get_resource(name)