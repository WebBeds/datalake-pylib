# SDK Client

from .session import Session
from .service import Service

class Client:

    _session: Session = None
    _services: dict = {}

    def __init__(self, session: Session) -> None:
        self._session = session
        self.__register__()

    def __register__(self) -> None:
        # Hotel Service
        from .fetchers.hotel import HotelFetcher
        self._services['hotel'] = HotelFetcher(self._session)

    def get_resource(self, name: str) -> Service:
        return self._services[name]

    def available_services(self) -> list:
        return list(self._services.keys())
