from abc import abstractmethod

class Entity(dict):

    def __init__(self, data: dict):
        self.__dict__.update(data)    
    
    def __str__(self) -> str:
        return self.__class__.__name__ + f"({hex(id(self))})"
    def __repr__(self) -> str:
        return self.__class__.__name__ + f"({hex(id(self))})"

    @abstractmethod
    def __schema__() -> dict:
        pass