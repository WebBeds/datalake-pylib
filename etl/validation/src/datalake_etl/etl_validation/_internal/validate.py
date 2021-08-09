#!/usr/bin/env python3

from multiprocessing import Pool, cpu_count

from pandas.core.algorithms import diff
from .compare import compare_dataframes

import pandas as pd
import numpy as np

class Comparator:
    def __init__(self,**kwargs):
        self.kwargs = kwargs
    def compare(self,source_1: pd.DataFrame,source_2: pd.DataFrame) -> pd.DataFrame:
        return compare_dataframes(
            first_df=source_1,
            second_df=source_2,
            **self.kwargs
        )

def concurrent_comparison(df: pd.DataFrame,df2: pd.DataFrame,comparator: Comparator,n_cores: int = None) -> pd.DataFrame:

    if n_cores == None:
        n_cores = cpu_count() // 2

    df_split = np.array_split(df,n_cores)
    df2_split = np.array_split(df2,n_cores)
    
    data = pd.DataFrame()
    
    with Pool(n_cores) as pool:
        data = pd.concat(pool.starmap(comparator.compare,zip(df_split,df2_split)))
    
    return data

# ================================
# VALIDATION CLASS
# ================================

class Validator:
    """
    Allows to compare two dataframes with a custom schema and other options, you can apply
    concurrent comparison by passing a 'comparator' function to apply to the 'concurrent_comparison' method.
    """
    def __init__(self, *args, **kwargs) -> None:
        
        # Source DataFrames
        self.sources = args
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
                self.sources[idx] = self.sources[idx].columns.lower()
                self.sources[idx] = self.sources[idx].sort_values(by=kwargs["sort_by"],ignore_index=True,inplace=True)
                self.sources[idx] = self.sources[idx].drop_duplicates(subset=kwargs["index_by"],ignore_index=True,inplace=True)

                if common == None:
                    common = set(self.sources[kwargs["index_by"]].values)
                else:
                    common = common.intersection(set(self.sources[kwargs["index_by"]].values))

                if different == None:
                    different = set(self.sources[kwargs["index_by"]].values)
                else:
                    different = different.difference(set(self.sources[kwargs["indexindex_by"]].values))

            self.common = common
            self.different = different            

            for idx, _ in enumerate(self.sources):
                self.sources = self.sources[self.sources[idx].isin(common)]

    def compare(self):
        if len(self.sources) < 2:
            raise Exception(f"insufficient data, only {len(self.sources)} dataframes")
        if len(self.sources) > 2:
            pass # Future implementation of comparison with check
        return compare_dataframes(
            self.sources[0]
            ,self.sources[1]
            ,**self.kwargs
        )

    def concurrent_comparison(self):
        if len(self.sources) < 2:
            raise Exception(f"insufficient data, only {len(self.sources)} dataframes")
        return concurrent_comparison(
            self.sources[0],
            self.sources[1],
            self.comparator,
            self.cores
        )