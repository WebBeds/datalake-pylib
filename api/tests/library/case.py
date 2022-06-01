from unittest.case import TestCase, SkipTest
from datalake.sdk import Client

class TestFetcher(TestCase):

    client: Client = None

    def __tests__(self):
        for name in self.__dir__():
            if not name.startswith('test_') or not callable(getattr(self, name)):
                continue
            yield name, getattr(self, name)

    def sequence(self):
        for name, step in self.__tests__():
            try:
                with self.subTest(step=name):
                    step()
            except SkipTest:
                pass
            except Exception as e:
                self.fail("{} Failed ({}: {})".format(step,type(e),e))
