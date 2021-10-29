
from typing import List

import numpy as np
from pandas import DataFrame, concat
from .engines.engine import ReportingEngine
from multiprocessing import Pool, cpu_count

class ReportingComparator:
    def __init__(self,
        engine: ReportingEngine,
        schema: dict = None,
        failfast: bool = True,
        keyindex: str = None,
        keylist: List[str] = None,
        static: List = None,
    ):
        self.engine = engine
        self.schema = schema
        self.failfast = failfast
        self.keyindex = keyindex
        self.keylist = keylist
        self.static = static
    def compare(self,
        first_df,
        second_df,
    ):
        """
        Compares two dataframes and returns a report
        """
        return self.engine.report(
            first_df,
            second_df,
            schema=self.schema,
            failfast=self.failfast,
            keyindex=self.keyindex,
            keylist=self.keylist,
            static=self.static,
        )

class ReportingValidator:
    def __init__(self,
        source_1: DataFrame,
        source_2: DataFrame,
        engine: ReportingEngine,
        schema: dict = None,
        failfast: bool = True,
        keyindex: str = None,
        keylist: List[str] = None,
        static: List = [None, None],
        ignore_values: List = None,
        lowercase_columns: bool = False,
        sort_by: List or str = None,
        index_by: List or str = None,
        cores: int = None,
        ):

        # Pack sources
        self.sources = [source_1, source_2]

        # Create engine
        self.engine = ReportingComparator(
            engine=engine,
            schema=schema,
            failfast=failfast,
            keyindex=keyindex,
            keylist=keylist,
            static=static,
        )
        
        self.schema = schema
        self.failfast = failfast
        self.keyindex = keyindex
        self.keylist = keylist
        self.static = static
        self.ignore_values = ignore_values
        self.lowercase_columns = lowercase_columns
        self.sort_by = sort_by
        self.index_by = index_by
        self.cores = cpu_count() // 2 if cores is None else cores

        if self.ignore_values:
            for idx, _ in enumerate(self.sources):
                self.sources[idx] = self.sources[idx][~self.sources[idx][keyindex].isin(self.ignore_values)]

        self.common = None
        self.different = None
        
        for idx, _ in enumerate(self.sources):
            
            if self.lowercase_columns:
                self.sources[idx].columns = self.sources[idx].columns.str.lower()

            if sort_by:
                self.sources[idx].sort_values(by=sort_by,ignore_index=True,inplace=True)

            if index_by:
                
                if common == None:
                    common = set(self.sources[idx][index_by].values)
                else:
                    common = common.intersection(set(self.sources[idx][index_by].values))

                if different == None:
                    different = set(self.sources[idx][index_by].values)
                else:
                    different = different.difference(set(self.sources[idx][index_by].values))

                self.common = common
                self.different = different

        if index_by:
            for idx, _ in enumerate(self.sources):
                self.sources[idx] = self.sources[idx][self.sources[idx][index_by].isin(common)]
            
    def compare(self) -> DataFrame:
        """
        Compares two dataframes and returns a report
        """
        return self.engine.compare(
            self.sources[0],
            self.sources[1]
        )
    
    def concurrent_comparison(self) -> DataFrame:
        
        s0 = np.array_split(self.sources[0], self.cores)  # We split the source
        s1 = [self.sources[1]] * len(s0)  # But keep all in the second file

        report = DataFrame()
        
        with Pool(self.cores) as pool:
            report = concat(pool.starmap(self.engine.compare, zip(s0, s1)))
        
        return report