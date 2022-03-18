import json
import tempfile
import pandas
from requests import request

from .service import Service, Collection
from ..entities.hotel import Hotel
from ..entities.file import File

class HotelService(Service):

    _base = '/master/hotels'
    
    def get_collection(self, content) -> Collection:
        hotels = []
        for hotel in content:
            hotels.append(Hotel(hotel))
        col = Collection(hotels)
        col._schema = Hotel._schema
        return col
    
    def _get_result(self, content):
        resp = json.loads(content)
        return resp['result']

    def response(self, response):
        return self.get_collection(self._get_result(response.content))

    def response_with_file(self, url: str) -> File:
        file: File = None
        with tempfile.NamedTemporaryFile() as file_name:
            with open(file_name.name, 'wb') as f:
                f.write(request('GET', url).content)
                f.close()
            file = File(df=pandas.read_csv(file_name.name))
            f.close()
        return file

    def get_paginated(self, platform: str, page: int = 0, page_size: int = 10):
        return self.response(
            self.fetch(
                method='GET',
                url=self._base,
                params={
                    'platform': platform,
                    'page': page,
                    'pagesize': page_size,
                },
            )
        )
    
    def get_export(self, platform: str, id: str = "", page: int = 0, page_size: int = 10000):
        return self.response_with_file(
            self._get_result(
                self.fetch(
                    method='GET',
                    url=f"{self._base}/export",
                    params={
                        'platform': platform,
                        'id': id,
                        'page': page,
                        'pagesize': page_size,
                    },
                ).content
            )
        )
        
    def from_id(self, platform: str, id):
        return self.response(
            self.fetch(
                method='GET',
                url=self._base,
                params={
                    'platform': platform,
                    'id': id,
                },
            )
        )