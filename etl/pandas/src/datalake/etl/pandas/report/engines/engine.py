
from abc import abstractmethod
from typing import List
from pandas import DataFrame

class ReportingEngine:
    """
    ReportingEngine is an abstract class that defines the interface for
    reporting engines.

    The interface is defined as follows:
    - `report`: takes a set of DataFrames and returns a DataFrame with the reporting results.
    """

    def __init__(self,
        value_names: list = ["value1", "value2"],
        rename_options: dict = {
            "time": "time",
            "message": "message",
            "column": "column"
        }):
        self.value_names = value_names
        self.rename_options = rename_options

    def __normalize_dataframes__(self,
        first_df: dict,
        second_df: dict,
        keyindex: str = None,
        ):

        static_values: list = []
        static: list = [None, None]
        dataframes: list = []
        columns = None

        # Map dataframes
        for idx, d in enumerate([first_df, second_df]):
            dataframes.append(d["dataframe"])

        # Lowercase colums
        for d in dataframes:
            d.columns = d.columns.str.lower()

        # Get static data
        for idx, _ in enumerate(dataframes):
            static[idx] = dataframes[idx].copy()

        # Intersection columns
        for d in dataframes:
            if columns == None:
                columns = set(d.columns)
            columns = columns.intersection(d.columns)

        # Apply the columns to the DataFrames
        for idx, _ in enumerate(dataframes):
            dataframes[idx] = dataframes[idx][list(columns)]

        # Set index if passed to the dataframes.
        if keyindex != None and isinstance(keyindex, str):
            for idx, _ in enumerate(dataframes):
                dataframes[idx] = dataframes[idx].set_index(keyindex)
                static[idx] = static[idx].set_index(keyindex)

        dataframes.extend(static)

        return dataframes, columns

    @abstractmethod
    def report(self,
        first_df: DataFrame,
        second_df: DataFrame, 
        schema: dict = None,
        failfast: bool = True,
        keyindex: str = None,
        keylist: List[str] = None,
        static: List = None
        ) -> DataFrame:
        pass