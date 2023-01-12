import pandas as pd

class Source:
    
    def retrieve(self, query: str) -> pd.DataFrame:
        raise NotImplementedError

    @staticmethod
    def parse(data: dict):
        raise NotImplementedError