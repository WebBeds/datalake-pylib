from tests.library import TestFetcher

from datalake.sdk import Collection
from datalake.api.infrastructure import MasterFetcher
from datalake.api.infrastructure.master import Hotels

class Master(TestFetcher):

    resource_name = 'master'

    def test_resource(self):
        try:
            self.client.get_resource(self.resource_name)
        except Exception as e:
            self.fail(f'Failed to get resource: {e}')

    def test_hotels(self):

        platform = "SH"
        page = 0
        page_size = 150
        
        resource: MasterFetcher = None
        try:
            resource = self.client.get_resource(self.resource_name)
        except Exception as e:
            self.fail(f'Failed to get resource: {e}')

        service: Hotels = resource.hotels()

        resp: Collection = service.get_paginated(
            platform=platform,
            page=page,
            page_size=page_size,
        )

        self.assertEqual(len(resp), page_size)
        self.assertEqual(resp[0]['platform'], platform)