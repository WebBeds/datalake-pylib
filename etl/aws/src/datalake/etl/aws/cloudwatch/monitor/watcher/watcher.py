from dataclasses import dataclass
from datetime import datetime
from croniter import croniter

from ..outputs import (
    Output,
    CloudWatch,
    Teams,
)
from ..sources import (
    Source,
    Athena,
    Postgres,
)

import logging

DEFAULT_VALUE = 7 * 24 * 60 * 60 # 7 days
DEFAULT_SOURCE = "athena"
DEFAULT_OUTPUT = "cloudwatch"

@dataclass
class Watcher:

    name: str
    sql: str

    source: Source
    output: Output

    default_value: float = DEFAULT_VALUE
    cron_expression: str = None

    def validate_cron(self, now: datetime) -> bool:
        if not self.cron_expression or not now:
            return True        
        return croniter.match(
            self.cron_expression,
            now,
        )

    def run(self, dry: bool = False, now: datetime = None) -> None:

        if not self.validate_cron(now):
            logging.info(f"Watcher `{self.name}` skipped due to cron expression")
            return

        df = self.source.retrieve(
            query=self.sql
        )
        df.columns = df.columns.str.lower()

        if df.empty:
            logging.warning(f"Watcher `{self.name}` returned empty dataframe")
            return
        if not self.name.lower() in df.columns:
            raise ValueError(f"Watcher returned no column named {self.name.lower()}")

        df[self.name.lower()] = df[self.name.lower()].fillna(self.default_value)

        if not self.output:
            raise ValueError("Watcher without output")

        if dry:
            return

        self.output.send(
            df=df,
        )

    @staticmethod
    def parse(data: dict):

        if not "name" in data \
        or not "sql" in data:
            raise ValueError("Watcher must have name, dimensions, unit and sql")

        name: str = data.get("name")
        sql: str = data.get("sql")
        default_value: float = data.get("default_value", DEFAULT_VALUE)
        cron_expression: str = data.get("cron", None)

        source: dict or str = data.get("source", DEFAULT_SOURCE)
        source_opts: dict = data.get("source_opts", {})
        if isinstance(source, str):
            source = {
                "type": source,
                "options": source_opts,
                "src": data,
            }
        elif isinstance(source, dict):
            if not "type" in source:
                raise ValueError("Source must have type")
            source = {
                "type": source.get("type"),
                "options": source.get("options", {}),
                "src": data,
            }
        else:
            raise ValueError("Source must be a string or a dict")

        if source["type"] == "athena":
            source = Athena.parse(source)
        elif source["type"] == "postgres":
            source = Postgres.parse(source)
        else:
            raise ValueError("Source not supported")

        output: dict or str = data.get("output", DEFAULT_OUTPUT)
        output_opts: dict = data.get("output_opts", {})
        if isinstance(output, str):
            output = {
                "type": output,
                "options": output_opts,
                "src": data,
            }
        elif isinstance(output, dict):
            if not "type" in output:
                raise ValueError("Output must have type")
            output = {
                "type": output.get("type"),
                "options": output.get("options", {}),
                "src": data,
            }

        if str(output["type"]).lower() == "cloudwatch":
            output = CloudWatch.parse(output)
        elif str(output["type"]).lower() == "teams":
            output = Teams.parse(output)

        return Watcher(
            name=name,
            sql=sql,
            source=source,
            output=output,
            default_value=default_value,
            cron_expression=cron_expression,
        )