from pandas import DataFrame, read_csv

class File:
    _df: DataFrame = None
    def __init__(self, df: DataFrame) -> None:
        self._df = df
    def get_dataframe(self) -> DataFrame:
        return self._df
    def __str__(self) -> str:
        return self.__class__.__name__ + f"({hex(id(self))})"
    def __repr__(self) -> str:
        return self.__class__.__name__ + f"({hex(id(self))})"