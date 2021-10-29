
from datetime import datetime
from typing import List
from pandas import DataFrame, Series, concat
from .engine import ReportingEngine

from ..methods.check_methods import default_check

class LegacyEngine(ReportingEngine):

    def __isolate__(self,
        values: list,
        column: str,
        functions: List = None,
        row: List = None,
        key: str = None,
        keylist: List = None,
        static_data: List = None,
        static_columns: List = None,
        failfast: bool = True
    ):

        value_1 = values[0]
        value_2 = values[1]

        fails = DataFrame()

        if functions != None:

            for f in functions:

                r = f(value_1, value_2)

                if not r["success"]:
                    
                    findex_1 = "None"
                    
                    df_static_data_values = "None"
                    df2_static_data_values = "None"

                    if static_columns[0] != None and isinstance(static_columns[0],list) or isinstance(static_columns[0],str):
                        if isinstance(static_columns[0],str):
                            df_static_data_values = static_data[0][static_columns[0]]
                        else:
                            df_static_data_values = ()
                            for i in static_columns[0]:
                                df_static_data_values.append(static_data[0][i])

                    if static_columns[1] != None and isinstance(static_columns[1],list) or isinstance(static_columns[1],str):
                        if isinstance(static_columns[1],str):
                            df_static_data_values = static_data[1][static_columns[1]]
                        else:
                            df_static_data_values = ()
                            for i in static_columns[1]:
                                df_static_data_values.append(static_data[1][i])

                    if key != None:
                        findex_1 = row[0][key]

                    if keylist != None and isinstance(keylist,list):
                        findex_1 = list()
                        for i in keylist:
                            findex_1.append(row[0][i])
                    
                    fail_schema = {
                        "success": False,
                        "fail": {
                            
                            # RENAME OPTIONS
                            self.rename_options["message"]: r["fail"]
                            ,self.rename_options["column"]: column
                            ,self.rename_options["time"]: datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                            
                            # NAMES DONT CHANGE
                            ,"failindex_1": findex_1
                            ,"static_1": df_static_data_values
                            ,"static_2": df2_static_data_values
                            
                            # VALUE NAMES
                            ,self.value_names[0]: value_1
                            ,f"{self.value_names[0]}_type": str(type(value_1))

                            ,self.value_names[1]: value_2
                            ,f"{self.value_names[1]}_type": str(type(value_2))

                        }
                    }

                    fails = fails.append(Series(fail_schema["fail"]),ignore_index=True)

                    if failfast:
                        return fails, True

        else:

            r = default_check(value_1, value_2)

            if not r["success"]:
                
                findex_1 = "None"
                
                df_static_data_values = "None"
                df2_static_data_values = "None"

                if static_columns[0] != None and isinstance(static_columns[0],list) or isinstance(static_columns[0],str):
                    if isinstance(static_columns[0],str):
                        df_static_data_values = static_data[0][static_columns[0]]
                    else:
                        df_static_data_values = ()
                        for i in static_columns[0]:
                            df_static_data_values.append(static_data[0][i])

                if static_columns[1] != None and isinstance(static_columns[1],list) or isinstance(static_columns[1],str):
                    if isinstance(static_columns[1],str):
                        df_static_data_values = static_data[1][static_columns[1]]
                    else:
                        df_static_data_values = ()
                        for i in static_columns[1]:
                            df_static_data_values.append(static_data[1][i])

                if key != None:
                    findex_1 = row[0][key]

                if keylist != None and isinstance(keylist,list):
                    findex_1 = list()
                    for i in keylist:
                        findex_1.append(row[0][i])

                fail_schema = {
                    "success": False,
                    "fail": {
                        
                        # RENAME OPTIONS
                        self.rename_options["message"]: r["fail"]
                        ,self.rename_options["column"]: column
                        ,self.rename_options["time"]: datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                        
                        # NAMES DONT CHANGE
                        ,"failindex_1": findex_1
                        ,"static_1": df_static_data_values
                        ,"static_2": df2_static_data_values
                        
                        # VALUE NAMES
                        ,self.value_names[0]: value_1
                        ,f"{self.value_names[0]}_type": str(type(value_1))

                        ,self.value_names[1]: value_2
                        ,f"{self.value_names[1]}_type": str(type(value_2))

                    }
                }

                fails = fails.append(Series(fail_schema["fail"]),ignore_index=True)

        return fails, False

    def report(self,
        first_df: DataFrame,
        second_df: DataFrame,
        schema: dict = None,
        failfast: bool = True,
        keyindex: str = None,
        keylist: List[str] = None,
        static: List = None,
        ) -> DataFrame:
        pass
        
        # Normalize dataframes and get static data.
        dataframes, columns = self.__normalize_dataframes__(
            first_df={"dataframe": first_df, "static": static[0]}, 
            second_df={"dataframe": second_df, "static": static[1]},
            keyindex=keyindex)
        first_df, second_df, first_static_df, second_static_df = dataframes[0], dataframes[1], dataframes[2], dataframes[3]
        columns = list(columns)

        # Append keyindex to keylist if keyindex is not None.
        if keyindex != None:
            keylist.append(keyindex)

        # Fail report
        report = DataFrame()

        for r in range(len(first_df)):
        
            first_values, second_values = first_df.iloc[r], second_df.iloc[r]
            
            first_static_values = None
            second_static_values = None

            if static[0] != None:
                first_static_values = first_static_df.iloc[r]

            if static[1] != None:
                second_static_values = second_static_df.iloc[r]
            
            for c in range(len(first_values)):
                
                first_value, second_value = first_values[c], second_values[c]
                functions = None
                column_name = first_df.columns[c]

                if schema != None and column_name in schema:
                    functions = schema[column_name]
            
                r, fail = self.__isolate__(
                    values=[first_value, second_value]
                    ,row=[first_values,second_values]
                    ,column=column_name
                    ,functions=functions
                    ,static_columns=[first_static_df,second_static_df]
                    ,static=[first_static_values,second_static_values]
                    ,key=None
                    ,keylist=keylist
                    ,failfast=failfast
                )

                report = concat([report,r],ignore_index=True)

                if failfast and fail:
                    return report

        return report