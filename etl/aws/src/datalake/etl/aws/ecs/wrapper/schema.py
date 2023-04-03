from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .types import Schema


@dataclass
class ParsedTimeSeconds(Schema):
    format: str = "%Y-%m-%dT%H:%M:%S.%fZ"

    def parse(self, input):
        try:
            if not isinstance(input, str):
                return input
            return str(int(datetime.strptime(input, self.format).timestamp()))
        except Exception:
            return input


@dataclass
class ParsedTimeMilliseconds(Schema):
    format: str = "%Y-%m-%dT%H:%M:%S.%fZ"

    def parse(self, input):
        try:
            if not isinstance(input, str):
                return input
            return str(datetime.strptime(input, self.format).timestamp())
        except Exception:
            return input


class ParsedExecutionIdArn(Schema):
    def parse(self, input):
        try:
            if not isinstance(input, str):
                return input
            return input.split(":")[-1]
        except Exception:
            return input


@dataclass
class ParsedCLIArgument(Schema):
    arg_type: Any = None

    def parse(self, input):
        input_type = type(input)
        if not self.arg_type or (self.arg_type and self.arg_type == input_type):
            return input
        # Encapsulate input in a list
        if input_type == str and self.arg_type == list:
            return [input]
        return self.arg_type(input)
