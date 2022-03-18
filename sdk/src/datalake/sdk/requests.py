from .session import Session
from requests import request as rq

class Request:

    _session: Session = None

    def __init__(self, session: Session):
        self._session = session

    def request(self,method: str, url: str, params: dict = {}, headers: dict = {}):
        url = self._session.get_hostname() + url
        # Add bearer token to headers
        headers['Authorization'] = 'Bearer ' + self._session._auth_token
        return rq(
            method=method,
            url=url,
            params=params,
            headers=headers,
        )