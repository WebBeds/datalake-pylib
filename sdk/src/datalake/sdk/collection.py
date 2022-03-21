from pandas import json_normalize

from datalake.etl import (
    schema as sch
)

class Collection(list):
    _schema: dict = None
    def set_schema(self, schema: dict):
        self._schema = schema
    def to_dataframe(self):
        collection = []
        for entity in self:
            collection.append(entity.__dict__)
        if self._schema is None:
            return json_normalize(collection)
        return sch.normalize_df(json_normalize(collection), self._schema)