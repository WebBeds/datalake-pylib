
from .einstein import EinsteinEngine
from .legacy import LegacyEngine

class Engines(dict):
    def __init__(self, *args, **kwargs):
        super(Engines, self).__init__(*args, **kwargs)
        self.__dict__ = self

# Register here the engines
ENGINES = Engines(
    legacy=LegacyEngine,
    einstein=EinsteinEngine,
)