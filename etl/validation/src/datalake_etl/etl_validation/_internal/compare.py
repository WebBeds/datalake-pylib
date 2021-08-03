#!/usr/bin/env python3

from pandas import DataFrame, concat
from typing import Tuple

class ComparisonException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

def read_parameters(**kwargs):

    schema: dict = None
    failfast: bool = True
    
    failindex = None
    first_df_static = None
    second_df_static = None

    value_names: list = ["value1", "value2"]

    if "schema" in kwargs and isinstance(kwargs["schema"],dict):
        schema = kwargs["schema"]

    if "failfast" in kwargs and isinstance(kwargs["failfast"],bool):
        failfast = kwargs["failfast"]

    if "failindex" in kwargs and isinstance(kwargs["failindex"],str) or isinstance(kwargs["failindex"],list):
        failindex = kwargs["failindex"]

    if "first_df_static" in kwargs and isinstance(kwargs["first_df_static"],str) or isinstance(kwargs["first_df_static"],list):
        first_df_static = kwargs["first_df_static"]

    if "second_df_static" in kwargs and isinstance(kwargs["second_df_static"],str) or isinstance(kwargs["second_df_static"],list):
        second_df_static = kwargs["second_df_static"]

    if "value" in kwargs and isinstance(kwargs["value"],list):
        value_names = kwargs["value_names"]

    return schema, failfast, failindex, first_df_static, second_df_static, value_names

def normalize_dataframes(*args):
    
    static_values = list()
    static = list()
    dataframes = list()
    columns = None

    # Map dataframes
    for idx, d in enumerate(args):
        dataframes.append(d["dataframe"])
        if d["static"] != None:
            static_values.append({
                "id": idx,
                "static": d["static"]
            })

    # Lowercase colums
    for d in dataframes:
        d.columns = d.columns.str.lower()

    # Get static data
    for s in static_values:
        static.append(
            dataframes[s["id"]][s["static"]].copy()
        )

    # Intersection columns
    for d in dataframes:
        if columns == None:
            columns = set(d.columns)
        columns.intersection(set(d.columns))

    return Tuple(dataframes), Tuple(static), columns

def isolate_comparison(values: list, **kwargs):
    pass

def compare_dataframes(first_df: DataFrame,second_df: DataFrame,**kwargs):
    """
    Compare two DataFrames with Row per Row comparison checking columns.

    :param first_df: First dataframe, will be the primary and some options will be based on the primary.
    :param second_df: Second dataframe, will be compared with primary dataframe.
    :param **kwargs: Additional keyword arguments. List:
    
    Functional options:

    - schema: dict = None
        - Schema to be used instead of default comparison functions.
    - failfast: bool = True
        - At first failure, return the report of fails.
    - failindex: str | list = None
        - If specified will take the input column or list of columns and put the values into
        a failindex key at return report.
    - first_df_static: str | list = None
        - If specified will take the input column or list of columns of the primary dataframe and will take from it.
        This option if for columns that are not in the two DataFrames and you need to know when returning the report.
    - second_df_static: str | list = None
        - If specified will take the input column or list of columns of the second dataframe and will take from it.
        Same as first_df_static.
    
    Return options:

    - value_names: list = ["value1", "value2"]
        - Specify the value names that will be returned.
    
    """
    
    # Read parameters
    schema, failfast, failindex, first_static, second_static, value_names = read_parameters(**kwargs)

    # Normalize dataframes and get static data.
    first_df, second_df, first_static_df, second_static_df, columns = normalize_dataframes({"dataframe": first_df, "static": first_static},{"dataframe": second_df, "static": second_static})

    # Fail report
    report = DataFrame()

    for r in range(len(first_df)):
        
        first_values, second_values = first_df.iloc[r], second_df.iloc[r]
        first_static_values, second_static_values = (None,first_static_df.iloc[r])[first_static != None], (None,second_static_df.iloc[r])[second_static != None]

        for c in range(len(first_values)):
            
            first_value, second_value = first_values[c], second_values[c]
            functions = None

            if schema != None and columns[c] in schema:
                functions = schema[columns[c]]
        
            r, fail = isolate_comparison(
                values=[first_value, second_value]
                ,functions=functions
                ,static=[first_static_values,second_static_values]
                ,failindex=failindex
                ,value_names=value_names
            )

            report = concat([report,r],ignore_index=True)

            if failfast and fail:
                return report

    return report