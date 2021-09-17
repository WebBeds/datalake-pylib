#!/usr/bin/env python3

from multiprocessing import Pool, cpu_count
from .compare import compare_dataframes, compare_dataframes_new

import pandas as pd
import numpy as np

class Comparator:
    def __init__(self,**kwargs):
        self.kwargs = kwargs
    def compare(self,*args) -> pd.DataFrame:
        if len(args) < 2:
            raise Exception(f"insufficient data, only {len(self.sources)} dataframes")
        if "engine" in self.kwargs and self.kwargs["engine"] == "new":
            return compare_dataframes_new(
                args[0],
                args[1],
                **self.kwargs    
            )
        return compare_dataframes(
            args[0],
            args[1],
            **self.kwargs
        )

def concurrent_comparison(*sources,comparator: Comparator,n_cores: int = None) -> pd.DataFrame:

    if n_cores == None:
        n_cores = cpu_count() // 2

    dataframes = []

    for s in sources:
        dataframes.append(
            np.array_split(s,n_cores)
        )

    data = pd.DataFrame()
    
    with Pool(n_cores) as pool:
        data = pd.concat(pool.starmap(comparator.compare,zip(dataframes[0],dataframes[1])))
    
    return data

# ================================
# VALIDATION CLASS
# ================================

class Validator:
    """
    Allows to compare two dataframes with a custom schema and other options, you can apply
    concurrent comparison by passing a 'comparator' function to apply to the 'concurrent_comparison' method.
    
    - args: list
        - the dataframes to compare.
            - Two dataframes: Normal comparison.
            - Three dataframes: Comparison with check.

    Optional parameters:

    - schema: dict = None
        - The comparison schema, specify some custom functions to columns.
    - failfast: bool = True
        - The comparison will end at the first fail.
    
    - failindex: str | list = None
        - The columns that will be as index and you can deconstruct.
    
    - first_df_static: str | list = None
        - The columns that will be used as static data, dont be compared but will be accessible
        to use in the static_1 column and deconstruct. Dont need to be on the two dataframes to 
        get, it must to be in first, but it could be ignored because the intersection of columns.
    - second_df_static: str | list = None
        - The columns that will be used as static data, dont be compared but will be accessible
        to use in the static_2 column and deconstruct. Dont need to be on the two dataframes to 
        get, it must to be on second, but it could be ignored because the intersection of columns.
    
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

    - engine: str = None ("new")
        - Use the new engine for comparison, using pandas compare built-in methods.

    """
    def __init__(self, *args, **kwargs) -> None:
        
        # Source DataFrames
        self.sources = list(args)
        self.kwargs = kwargs

        # Comparator
        self.comparator = Comparator(**kwargs)

        # Cores
        cores = cpu_count() // 2
        if "cores" in kwargs:
            cores = kwargs["cores"]
        self.cores = cores

        # Sort functionality
        if "sort_by" in kwargs and "index_by" in kwargs:

            common = None
            different = None

            for idx, _ in enumerate(self.sources):
                
                self.sources[idx].columns = self.sources[idx].columns.str.lower()
                self.sources[idx].sort_values(by=kwargs["sort_by"],ignore_index=True,inplace=True)
                self.sources[idx].drop_duplicates(subset=kwargs["index_by"],ignore_index=True,inplace=True)

                if common == None:
                    common = set(self.sources[idx][kwargs["index_by"]].values)
                else:
                    common = common.intersection(set(self.sources[idx][kwargs["index_by"]].values))

                if different == None:
                    different = set(self.sources[idx][kwargs["index_by"]].values)
                else:
                    different = different.difference(set(self.sources[idx][kwargs["index_by"]].values))

            self.common = common
            self.different = different            

            for idx, _ in enumerate(self.sources):
                self.sources[idx] = self.sources[idx][self.sources[idx][kwargs["index_by"]].isin(common)]

    def compare(self):
        if len(self.sources) < 2:
            raise Exception(f"insufficient data, only {len(self.sources)} dataframes")
        if "engine" in self.kwargs and self.kwargs["engine"] == "new":
            return compare_dataframes_new(
                self.sources[0]
                ,self.sources[1]
                ,**self.kwargs
            )
        return compare_dataframes(
            self.sources[0]
            ,self.sources[1]
            ,**self.kwargs
        )

    def concurrent_comparison(self):
        if len(self.sources) < 2:
            raise Exception(f"insufficient data, only {len(self.sources)} dataframes")
        return concurrent_comparison(
            *self.sources,
            comparator=self.comparator,
            n_cores=self.cores
        )