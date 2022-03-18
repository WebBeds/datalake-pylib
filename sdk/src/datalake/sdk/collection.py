from typing import List
from .entities.entity import Entity
from pandas import json_normalize

from datalake.etl import (
    schema as sch
)

class Collection(list):
    _schema: dict = None
    def to_dataframe(self):
        collection = []
        schema = None
        for entity in self:
            if schema is None:
                schema = entity.get_schema()
            collection.append(entity.__dict__)
        return sch.normalize_df(json_normalize(collection), schema)