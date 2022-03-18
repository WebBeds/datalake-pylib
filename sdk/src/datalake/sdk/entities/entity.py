class Entity(dict):
    _schema: dict = None
    def __init__(self, data: dict):
        self.__dict__.update(data)
    def get_schema(self) -> dict:
        return self._schema
    def __str__(self) -> str:
        return self.__class__.__name__ + f"({hex(id(self))})"
    def __repr__(self) -> str:
        return self.__class__.__name__ + f"({hex(id(self))})"