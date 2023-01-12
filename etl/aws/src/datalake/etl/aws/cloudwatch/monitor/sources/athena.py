from .source import Source, pd

import awswrangler as wr

class Athena(Source):

    DEFAULT_ATHENA_KWARGS: dict = {
        "database": "default",
        "ctas_approach": False
    }

    def __init__(self, 
        athena_kwargs: dict = None
    ) -> None:
        self.athena_kwargs = athena_kwargs

    def retrieve(self, query: str) -> pd.DataFrame:
        return wr.athena.read_sql_query(
            sql=query,
            **self.DEFAULT_ATHENA_KWARGS
        )

    def parse(data: dict):
        options: dict = Athena.DEFAULT_ATHENA_KWARGS
        if data.get("options") and isinstance(data.get("options"), dict):
            options.update(data.get("options"))
        return Athena(
            athena_kwargs=options
        )