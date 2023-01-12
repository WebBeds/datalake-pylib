import pandas as pd

class Output:

    def send(self, df: pd.DataFrame) -> None:
        raise NotImplementedError

    @staticmethod
    def parse(data: dict):
        raise NotImplementedError