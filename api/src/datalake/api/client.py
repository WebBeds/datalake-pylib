from datalake.sdk import Client

from .infrastructure.master import MasterFetcher

class APIClient(Client):
    
    def __register__(self) -> None:
        # Master Fetcher
        self._services['master'] = MasterFetcher(self)

    def get_resource(self, name: str):
        if name not in self._services:
            raise Exception(f"Resource '{name}' is not registered.")
        return super().get_resource(name)