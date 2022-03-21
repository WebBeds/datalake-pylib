from abc import abstractmethod

class Entity(dict):
    def __init__(self, *args, **kwargs):
        super(Entity,self).__init__(*args, **kwargs)
    @abstractmethod
    def __schema__() -> dict:
        pass