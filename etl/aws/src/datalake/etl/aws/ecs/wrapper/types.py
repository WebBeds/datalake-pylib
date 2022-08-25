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
        if not input or ((input and self.key not in input) or (self.search_key and self.search_key not in input)):
            return self.schema.parse(self.default)
        if self.search_key and self.search_key in input:
            return self.schema.parse(input[self.search_key])
        return self.schema.parse(input[self.key])