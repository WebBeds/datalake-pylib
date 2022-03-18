from abc import abstractmethod
from ..collection import Collection
from ..session import Session
from ..requests import Request
from ..exception import SDKException

class Service:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._request = Request(session)
    @abstractmethod
    def get_collection(self, content) -> Collection:
        pass
    def fetch(self, method: str, url: str, params: dict = {}, headers: dict = {}):
        resp = self._request.request(
            method=method,
            url=url,
            params=params,
            headers=headers,
        )
        if resp.status_code != 200:
            raise SDKException(
                "Failed to fetch data",
                status_code=resp.status_code,
                content=resp.content,
            )
        return resp