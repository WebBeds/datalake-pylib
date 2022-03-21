from pandas import DataFrame, read_csv

class File:
    
    dataframe: DataFrame = None
    path: str = None
    
    def __init__(self, path: str, read: bool = False) -> None:
        self.path = path
        if read:
            self.dataframe = read_csv(path)
    
    def get_dataframe(self) -> DataFrame:
        if self.dataframe is None:
            self.dataframe = read_csv(self.path)
        return self.dataframe

    def get_path(self) -> str:
        return self.path