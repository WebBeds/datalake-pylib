from dataclasses import dataclass
from .source import Source, pd

import awswrangler as wr
import pg8000

class Postgres(Source):

    connection: str

    def retrieve(self, query: str) -> pd.DataFrame:
        con: pg8000.Connection = wr.postgresql.connect(self.connection)
        return wr.postgresql.read_sql_query(
            sql=query,
            con=con
        )

    def parse(data: dict):

        connection: str = data.get("options", {}).get("connection", None)
        if not connection:
            raise ValueError("Source must have connection")
        
        return Postgres(
            connection=connection
        )