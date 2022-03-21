# SDK Client
from abc import abstractmethod
from .session import Session

class Client:

    _session: Session = None
    _services: dict = {}

    def __init__(self, session: Session) -> None:
        self._session = session
        self.__register__()

    def available_services(self) -> list:
        return list(self._services.keys())

    def get_resource(self, name: str):
        return self._services[name]
    
    @abstractmethod
    def __register__(self) -> None:
        pass

