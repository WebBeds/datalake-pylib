from dataclasses import dataclass

import awswrangler as wr

from .source import Source, pd


@dataclass
class SQLServer(Source):
    connection: str
    driver: int = 18

    def retrieve(self, query: str, con=None) -> pd.DataFrame:
        if not con:
            con = wr.sqlserver.connect(
                self.connection,
                odbc_driver_version=self.driver,
            )
        return wr.sqlserver.read_sql_query(sql=query, con=con)

    def parse(data: dict) -> "SQLServer":
        connection: str = data.get("options", {}).get("connection", None)
        if not connection:
            raise ValueError("Source must have connection")
        driver: int = data.get("options", {}).get("driver", 18)
        if not driver or not isinstance(driver, int):
            raise ValueError("Source must have driver")
        return SQLServer(
            connection=connection,
            driver=driver,
        )
