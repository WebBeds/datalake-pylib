from tests.library import TestFetcher

from datalake.sdk import Collection
from datalake.api.infrastructure import MPAFetcher
from datalake.api.infrastructure.projects.mpa import Executions

class MPA(TestFetcher):

    resource_name = 'mpa'

    def test_resource(self):
        try:
            self.client.get_resource(self.resource_name)
        except Exception as e:
            self.fail(f'Failed to get resource: {e}')

    def test_execution(self):

        execution_id = "661af9bc-a0a0-41b4-bbf6-08bcc85d1002"
        
        resource: MPAFetcher = None
        try:
            resource = self.client.get_resource(self.resource_name)
        except Exception as e:
            self.fail(f'Failed to get resource: {e}')

        service: Executions = resource.executions()
        
        resp: Collection = service.get_paginated(
            executionId=execution_id,
        )
        
        self.assertEqual(len(resp), 1)
        self.assertIn('executionid', resp[0].keys())
        self.assertEqual(resp[0]['executionid'], execution_id)        

        