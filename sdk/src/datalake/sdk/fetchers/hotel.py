from ..service import Service
from ..models import Hotel

class HotelFetcher(Service):

    _base = '/master/hotels'
    
    def __entity__(self):
        return Hotel
    
    def response(self, response):
        return self.__get_collection__(response.content, result_str="result")

    def get_paginated(self, platform: str, page: int = 0, page_size: int = 10):
        return self.response(
            self.__fetch__(
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
        return self.__fetch_file__(
            method='GET',
            url=f"{self._base}/export",
            result_str="result",
            params={
                'platform': platform,
                'id': id,
                'page': page,
                'pagesize': page_size,
            },
        )
        
    def from_id(self, platform: str, id):
        return self.response(
            self.__fetch__(
                method='GET',
                url=self._base,
                params={
                    'platform': platform,
                    'id': id,
                },
            )
        )