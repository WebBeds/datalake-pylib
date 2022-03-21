from abc import abstractmethod
from requests import request as rq

import json
import tempfile

from .collection import Collection
from .session import Session
from .requests import Request
from .exception import SDKException
from .file import File



class Service:

    def __init__(self, session: Session) -> None:
        self._session = session
        self._request = Request(session)

    def __load_json__(self, content: str, result_str: str = None) -> dict:
        result = json.loads(content)
        if result_str is not None and result_str not in result:
            raise SDKException(
                "could not find result_str in response",
            )
        if result_str is not None:
            return result[result_str]
        return result

    def __get_collection__(self, content, result_str: str = None) -> Collection:
        if self.__entity__ is None:
            raise SDKException("entity is not defined for this service")
        if not isinstance(content, dict):
            content = self.__load_json__(content, result_str)
        collection = Collection()
        collection.set_schema(self.__entity__().__schema__())
        for hotel in content:
            collection.append(self.__entity__()(hotel))
        return collection

    def __fetch_file__(self, method: str, url: str, result_str: str, params: dict = {}, headers: dict = {}, delete: bool = True) -> File:
        
        # Make request to obtain file url.
        resp = self.__fetch__(
            method=method,
            url=url,
            params=params,
            headers=headers,
        )

        # Load and parse response
        file_url: str = None
        result = json.loads(resp.content)
        if result_str is not None and result_str not in result:
            raise SDKException(
                "could not find result_str in response",
                status_code=resp.status_code,
                content=resp.content,
            )
        
        file_url = result[result_str]
        file: File = None

        with tempfile.NamedTemporaryFile(delete=delete) as file_name:
            with open(file_name.name, 'wb') as f:
                # Obtain file content
                f.write(rq(method="GET", url=file_url).content)
                f.close()
            # Read file as csv
            file = File(path=file_name.name, read=delete)
            f.close()

        return file

    def __fetch__(self, method: str, url: str, params: dict = {}, headers: dict = {}):
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

    @abstractmethod
    def __entity__(self):
        pass