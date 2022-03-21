from pandas import json_normalize

from datalake.etl import (
    schema as sch
)

class Collection(list):
    _schema: dict = None
    def set_schema(self, schema: dict):
        self._schema = schema
    def get_schema(self) -> dict:
        return self._schema
    def to_dataframe(self):
        if self._schema is None:
            return json_normalize(self)
        return sch.normalize_df(json_normalize(self), self._schema)