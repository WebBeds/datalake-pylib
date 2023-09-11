from dataclasses import dataclass

import awswrangler as wr
import pg8000

from .source import Source, pd


@dataclass
class Postgres(Source):
    connection: str

    def retrieve(self, query: str, con: pg8000.Connection = None) -> pd.DataFrame:
        if not con:
            con = wr.postgresql.connect(self.connection)
        return wr.postgresql.read_sql_query(sql=query, con=con)

    def parse(data: dict):
        connection: str = data.get("options", {}).get("connection", None)
        if not connection:
            raise ValueError("Source must have connection")

        return Postgres(connection=connection)
