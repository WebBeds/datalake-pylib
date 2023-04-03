from dataclasses import dataclass
from typing import Any


class Schema:
    def parse(self, input):
        return input


@dataclass
class Argument:
    key: str
    search_key: str = None
    schema: Schema = Schema()
    default: Any = None

    def parse(self, input) -> Any:
        try:
            if not input:
                return self.schema.parse(self.default)
            if self.key in input:
                return self.schema.parse(input[self.key])
            if self.search_key and self.search_key in input:
                return self.schema.parse(input[self.search_key])
            return self.schema.parse(self.default)
        except Exception:
            return self.default
