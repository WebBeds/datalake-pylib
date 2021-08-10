#!/usr/bin/env python3

from pandas import DataFrame, concat, Series
from datetime import datetime

from .check_methods import default_check

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
    rename_options: dict = {
        "time": "time",
        "message": "message",
        "column": "column"
    }

    optional_data: dict = None

    if "schema" in kwargs and isinstance(kwargs["schema"],dict):
        schema = kwargs["schema"]

    if "failfast" in kwargs and isinstance(kwargs["failfast"],bool):
        failfast = kwargs["failfast"]

    if "failindex" in kwargs and isinstance(kwargs["failindex"],str) or "failindex" in kwargs and isinstance(kwargs["failindex"],list):
        failindex = kwargs["failindex"]

    if "first_df_static" in kwargs and isinstance(kwargs["first_df_static"],str) or "first_df_static" in kwargs and isinstance(kwargs["first_df_static"],list):
        first_df_static = kwargs["first_df_static"]

    if "second_df_static" in kwargs and isinstance(kwargs["second_df_static"],str) or "second_df_static" in kwargs and isinstance(kwargs["second_df_static"],list):
        second_df_static = kwargs["second_df_static"]

    if "value_names" in kwargs and isinstance(kwargs["value_names"],list):
        value_names = kwargs["value_names"]

    if "rename_options" in kwargs and isinstance(kwargs["rename_options"],dict):
        rename_options = kwargs["rename_options"]

    if "optional_data" in kwargs:
        optional_data = kwargs["optional_data"]

    return schema, failfast, failindex, first_df_static, second_df_static, value_names, rename_options, optional_data

def normalize_dataframes(*args):
    
    static_values: list = []
    static: list = [None, None]
    dataframes: list = []
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
    for idx, s in enumerate(static_values):
        static.pop(idx)
        static.append(
            dataframes[s["id"]][s["static"]].copy()
        )

    # Intersection columns
    for d in dataframes:
        if columns == None:
            columns = set(d.columns)
        columns.intersection(set(d.columns))

    dataframes.extend(static)

    return *tuple(dataframes), columns

def isolate_comparison(values: list, **kwargs) -> DataFrame:
    
    value_1 = values[0]
    value_2 = values[1]

    fails = DataFrame()

    if kwargs["functions"] != None:

        for f in kwargs["functions"]:

            r = f(value_1, value_2,kwargs["optional_data"])

            if not r["success"]:
                
                findex_1 = "None"
                findex_2 = "None"
                
                df_static_data_values = "None"
                df2_static_data_values = "None"

                if kwargs["failindex"] != None and isinstance(kwargs["failindex"],str):
                    findex_1 = kwargs["row"][0][kwargs["failindex"]]

                if kwargs["failindex"] != None and isinstance(kwargs["failindex"],str):
                    findex_2 = kwargs["row"][1][kwargs["failindex"]]

                if kwargs["failindex"] != None and isinstance(kwargs["failindex"],list):
                    findex_1 = list()
                    for i in kwargs["failindex"]:
                        findex_1.append(kwargs["row"][0][i])
                
                if kwargs["failindex"] != None and isinstance(kwargs["failindex"],list):
                    findex_2 = list()
                    for i in kwargs["failindex"]:
                        findex_2.append(kwargs["row"][1][i])

                if kwargs["static"][0] != None and isinstance(kwargs["static"][0],list):
                    df_static_data_values = list()
                    for i in kwargs["static"][0]:
                        df_static_data_values.append(kwargs["static"][0][i])

                if kwargs["static"][1] != None and isinstance(kwargs["static"][1],list):
                    df2_static_data_values = list()
                    for i in kwargs["static"][1]:
                        df2_static_data_values.append(kwargs["static"][1][i])

                fail_schema = {
                    "success": False,
                    "fail": {
                        
                        # RENAME OPTIONS
                        kwargs["rename_options"]["message"]: r["fail"]
                        ,kwargs["rename_options"]["column"]: kwargs["column"]
                        ,kwargs["rename_options"]["time"]: datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                        
                        # NAMES DONT CHANGE
                        ,"failindex_1": findex_1
                        ,"failindex_2": findex_2
                        ,"static_1": df_static_data_values
                        ,"static_2": df2_static_data_values
                        
                        # VALUE NAMES
                        ,kwargs["value_names"][0]: value_1
                        ,f"{kwargs['value_names'][0]}_type": str(type(value_1))

                        ,kwargs["value_names"][1]: value_2
                        ,f"{kwargs['value_names'][1]}_type": str(type(value_2))

                    }
                }

                fails = fails.append(Series(fail_schema["fail"]),ignore_index=True)

                if kwargs["failfast"]:
                    return fails, True

    else:

        r = default_check(value_1, value_2)

        if not r["success"]:
            
            findex_1 = "None"
            findex_2 = "None"
            
            df_static_data_values = "None"
            df2_static_data_values = "None"

            if kwargs["failindex"] != None and isinstance(kwargs["failindex"],str):
                findex_1 = kwargs["row"][0][kwargs["failindex"]]

            if kwargs["failindex"] != None and isinstance(kwargs["failindex"],str):
                findex_2 = kwargs["row"][1][kwargs["failindex"]]

            if kwargs["failindex"] != None and isinstance(kwargs["failindex"],list):
                findex_1 = list()
                for i in kwargs["failindex"]:
                    findex_1.append(kwargs["row"][0][i])
            
            if kwargs["failindex"] != None and isinstance(kwargs["failindex"],list):
                findex_2 = list()
                for i in kwargs["failindex"]:
                    findex_2.append(kwargs["row"][1][i])

            if kwargs["static"][0] != None and isinstance(kwargs["static"][0],list):
                df_static_data_values = list()
                for i in kwargs["static"][0]:
                    df_static_data_values.append(kwargs["static"][0][i])

            if kwargs["static"][1] != None and isinstance(kwargs["static"][1],list):
                df2_static_data_values = list()
                for i in kwargs["static"][1]:
                    df2_static_data_values.append(kwargs["static"][1][i])

            fail_schema = {
                "success": False,
                "fail": {
                    
                    # RENAME OPTIONS
                    kwargs["rename_options"]["message"]: r["fail"]
                    ,kwargs["rename_options"]["column"]: kwargs["column"]
                    ,kwargs["rename_options"]["time"]: datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                    
                    # NAMES DONT CHANGE
                    ,"failindex_1": findex_1
                    ,"failindex_2": findex_2
                    ,"static_1": df_static_data_values
                    ,"static_2": df2_static_data_values
                    
                    # VALUE NAMES
                    ,kwargs["value_names"][0]: value_1
                    ,f"{kwargs['value_names'][0]}_type": str(type(value_1))

                    ,kwargs["value_names"][1]: value_2
                    ,f"{kwargs['value_names'][1]}_type": str(type(value_2))

                }
            }

            fails = fails.append(Series(fail_schema["fail"]),ignore_index=True)

    return fails, False

def compare_dataframes(first_df: DataFrame,second_df: DataFrame, **kwargs) -> DataFrame:
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

    - rename_options: dict = {
        "time": "time",
        "message": "message",
        "column": "column"
    }
        - Rename basic report columns to specified names.
    
    - optional_data: dict = None
        - Will be passed to a schema function as kwargs. User will expect kwargs if need.

    """
    
    # Read parameters
    schema, failfast, failindex, first_static, second_static, value_names, rename_options, optional_data = read_parameters(**kwargs)

    # Normalize dataframes and get static data.
    first_df, second_df, first_static_df, second_static_df, columns = normalize_dataframes({"dataframe": first_df, "static": first_static},{"dataframe": second_df, "static": second_static})
    
    columns = list(columns)

    # Fail report
    report = DataFrame()

    for r in range(len(first_df)):
        
        first_values, second_values = first_df.iloc[r], second_df.iloc[r]
        
        first_static_values = None
        second_static_values = None

        if first_static != None:
            first_static_values = first_static_df.iloc[r]

        if second_static != None:
            second_static_values = second_static_df.iloc[r]
        
        for c in range(len(first_values)):
            
            first_value, second_value = first_values[c], second_values[c]
            functions = None
            column_name = first_df.columns[c]

            if schema != None and column_name in schema:
                functions = schema[column_name]
        
            r, fail = isolate_comparison(
                values=[first_value, second_value]
                ,row=[first_values,second_values]
                ,column=column_name
                ,functions=functions
                ,static=[first_static_values,second_static_values]
                ,failindex=failindex
                ,value_names=value_names
                ,rename_options=rename_options
                ,optional_data=optional_data
                ,failfast=failfast
            )

            report = concat([report,r],ignore_index=True)

            if failfast and fail:
                return report

    return report