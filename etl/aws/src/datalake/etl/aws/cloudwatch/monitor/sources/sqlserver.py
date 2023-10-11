from dataclasses import dataclass

import awswrangler as wr

from .source import Source, pd


@dataclass
class SQLServer(Source):
    connection: str

    def retrieve(self, query: str, con=None) -> pd.DataFrame:
        if not con:
            con = wr.sqlserver.connect(self.connection)
        return wr.sqlserver.read_sql_query(sql=query, con=con)

    def parse(data: dict) -> "SQLServer":
        connection: str = data.get("options", {}).get("connection", None)
        if not connection:
            raise ValueError("Source must have connection")

        return SQLServer(connection=connection)
