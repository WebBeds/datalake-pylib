import json
from datalake.sdk import Service
from datalake.api.models import MPAExecution

class Executions(Service):

    _base = "/project/mpa/executions"

    def __entity__(self):
        return MPAExecution

    def response(self, response):
        return self.__get_collection__(response.content, result_str="result")
    
    def get_paginated(self,
        executionId: str = "",
        status: str = "",
        application: str = "",
        task: str = "",
        email: str = "",
        page: int = 0,
        pageSize: int = 0,
    ):
        return self.response(
            self.__fetch__(
                method='GET',
                url=self._base,
                params={
                    'executionid': executionId,
                    'status': status,
                    'application': application,
                    'task': task,
                    'email': email,
                    'page': page,
                    'pagesize': pageSize,
                },
            )
        )
    
    def get_export(self,
        executionId: str = "",
        status: str = "",
        application: str = "",
        task: str = "",
        email: str = "",
        page: int = 0,
        pageSize: int = 0,
        delete: bool = True,
    ):
        return self.__fetch_file__(
            method='GET',
            url=f"{self._base}/export",
            result_str="result",
            params={
                'executionid': executionId,
                'status': status,
                'application': application,
                'task': task,
                'email': email,
                'page': page,
                'pagesize': pageSize,
            },
            delete=delete
        )

    def new(self,
        application: str,
        email: str,
        task: str = "",
        extra: str = "",
    ):
        resp = self.__fetch__(
            method='PUT',
            url=self._base,
            params={
                'application': application,
                'email': email,
                'task': task,
                'extra': extra,
            },
        )
        # Load response into an object
        resp_data = json.loads(resp.content)
        # Return the result with ID
        return resp_data['result']
    
    def update(self,
        executionId: str,
        status: str = "",
        extra: str = "",
    ):
        return self.__fetch__(
            method='POST',
            url=self._base,
            params={
                'executionid': executionId,
                'status': status,
                'extra': extra,
            },
        ).status_code

        