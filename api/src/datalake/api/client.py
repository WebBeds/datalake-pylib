from datalake.sdk import Client

from .infrastructure.master import MasterFetcher

class APIClient(Client):
    
    def __register__(self) -> None:
        # Master Fetcher
        self._services['master'] = MasterFetcher(self)

    def add_resource(self, name: str, resource: object):
        if name in self._services:
            raise Exception(f"Resource '{name}' is already registered.")
        self._services[name] = resource
        return self

    def remove_resource(self, name: str):
        if name not in self._services:
            raise Exception(f"Resource '{name}' is not registered.")
        self._services.pop(name)
        return self
    
    def get_resource(self, name: str):
        if name not in self._services:
            raise Exception(f"Resource '{name}' is not registered.")
        return super().get_resource(name)