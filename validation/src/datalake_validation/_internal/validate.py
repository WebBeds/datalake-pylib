#!/usr/bin/env python3

from datetime import datetime
from multiprocessing import Pool,cpu_count

import pandas as pd
import numpy as np

# ================================
# DEFAULT CHECKERS
# ================================

def default_date_check(v: pd.Timestamp, v2: pd.Timestamp) -> dict:
    v_time = v.to_datetime64()
    v2_time = v2.to_datetime64()
    second_treshold = 0
    if pd.Timedelta(v_time - v2_time).total_seconds() != second_treshold:
        return {"success": False, "fail": "Time error, future date detected"}
    return {"success": True}

def default_check(v,v2) -> dict:
    
    # Check if one of the values is Timestamp
    isDate = type(v) == pd.Timestamp or type(v2) == pd.Timestamp
    # Check if one of the values is nan
    isNan = (str(v).lower() == "nan" or str(v).lower() == "nat") or (str(v2).lower() == "nan" or str(v2).lower() == "nat")

    # Transform data by column type
    if type(v) == np.float64 or type(v2) == np.float64:
        v = float(v)
        v2 = float(v2)

    # Timestamp test
    if isDate:
        result = default_date_check(v,v2)
        if not result["success"]:
            return {"success": False, "fail": result["fail"]}
    elif str(v) != str(v2) and not isNan and not isDate:
        return {"success": False, "fail": f"Different value in str comparison."}
    return {"success": True}

def default_ignore_check(v,v1) -> dict:
    return {"success": True}

# ================================
# COMPARISON
# ================================

def compare_two_dataframes(df: pd.DataFrame, df2: pd.DataFrame,schema: dict = None,discard: list = None,failfast = True,failindex = None):

    # Fail dataframe
    fails = pd.DataFrame()

    # Reset index to avoid index errors
    df.reset_index(inplace=True)
    df2.reset_index(inplace=True)

    # NOTE 2nd CHECK: Normalization

    # COLUMNS Lowercase
    df.columns = df.columns.str.lower()
    df2.columns = df2.columns.str.lower()

    # COLUMNS Intersection
    COLUMNS = set(df.columns).intersection(df2.columns)

    if discard == None:
        discard = ["level_0","index"]

    # Discard by list
    for d in discard:
        if d in COLUMNS:
            COLUMNS.discard(d)

    # UPDATE DataFrames to obtain new columns schema
    df = df[COLUMNS]
    df2 = df2[COLUMNS]

    # ROW PER ROW Comparison
    for i in range((df.index.stop,df.index)[type(df.index) == pd.Int64Index]):
        
        # GET EACH ROW VALUES PER DATAFRAME
        df_values = df.iloc[i]
        df2_values = df2.iloc[i]

        # NOTE COMPARE EACH ROW COLUMNS VALUES
        for j in range(len(df_values)):

            # Get the values into variable
            df_value = df_values[j]
            df2_value = df2_values[j]

            # SCHEMA TEST
            if schema != None and list(COLUMNS)[j] in schema:
                
                for f in schema[list(COLUMNS)[j]]:
                    
                    result = f(df_value,df2_value)
                    
                    if not result["success"]:

                        findex = "None"

                        if failindex != None:
                            findex = df_values[failindex]

                        fail_schema = {
                            "success": False,
                            "fail": {
                                "message": result["fail"]
                                ,"column": df.columns[j]
                                ,"failindex": findex
                                ,"value": df_value
                                ,"type": str(type(df_value))
                                ,"value2": df2_value
                                ,"type2": str(type(df2_value))
                                ,"time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                            }
                        }

                        fails = fails.append(pd.Series(fail_schema["fail"]),ignore_index=True)

                        if failfast:
                            return {"success": False, "fail": fail_schema["fail"]}, fails.copy()
            else:

                result = default_check(df_value,df2_value)
                
                if not result["success"]:

                    findex = "None"

                    if failindex != None:
                        findex = df_values[failindex]

                    fail_schema = {
                        "success": False,
                        "fail": {
                            "message": result["fail"]
                            ,"column": df.columns[j]
                            ,"failindex": findex
                            ,"value": df_value
                            ,"type": str(type(df_value))
                            ,"value2": df2_value
                            ,"type2": str(type(df2_value))
                            ,"time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                        }
                    }

                    fails = fails.append(pd.Series(fail_schema["fail"]),ignore_index=True)

                    if failfast:
                        return {"success": False, "fail": fail_schema["fail"]}, fails.copy()
    
    return {"success": True}, fails.copy()

class Comparator:
    def __init__(self,schema: dict = None,failindex: str = None) -> None:
        self.schema=schema
        self.failindex=failindex
    def compare(self,df: pd.DataFrame,df2: pd.DataFrame) -> pd.DataFrame:
        r, f = compare_two_dataframes(df,df2,schema=self.schema,failfast=False,failindex=self.failindex)
        return f

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
# APPLY
# ================================

def concurrent_vectorization(df,func,n_cores = None) -> pd.DataFrame:

    if n_cores == None:
        n_cores = cpu_count() // 2
    
    df_split = np.array_split(df,n_cores)

    data = pd.DataFrame()

    with Pool(n_cores) as pool:
        data = pd.concat(pool.map(func,df_split))
    
    return data

# ================================
# VALIDATION CLASS
# ================================

class Validator:
    """
    Allows to compare two dataframes with a custom schema and other options, you can apply
    concurrent comparison by passing a 'comparator' function to apply to the 'concurrent_comparison' method.
    """
    def __init__(self,df: pd.DataFrame,df2: pd.DataFrame,comparator: Comparator,failfast: bool = False,discard: list = None,n_cores: int = None,sort_by: list = None,index_by: str = None) -> None:
        self.df=df
        self.df2=df2
        self.schema=comparator.schema
        self.discard=discard
        self.failfast=failfast
        self.failindex=comparator.failindex
        self.comparator=comparator
        self.n_cores=n_cores

        # SORT BY FUNCTIONALITY
        if sort_by != None and index_by != None:

            self.df.columns = self.df.columns.str.lower()
            self.df2.columns = self.df2.columns.str.lower()

            self.df.sort_values(by=sort_by,ignore_index=True,inplace=True)
            self.df2.sort_values(by=sort_by,ignore_index=True,inplace=True)

            common = set(self.df[index_by].values).intersection(set(self.df2[index_by].values))

            self.df = self.df[self.df[index_by].isin(common)]
            self.df2 = self.df2[self.df2[index_by].isin(common)]

    def concurrent_comparison(self) -> pd.DataFrame:
        if self.comparator == None:
            print(f"Cannot initialise concurrent comparison, comparator is None.")
            return np.nan
        return concurrent_comparison(self.df,self.df2,self.comparator,self.n_cores)
    def comparison(self):
        return compare_two_dataframes(self.df,self.df2,self.schema,self.discard,self.failfast,self.failindex)