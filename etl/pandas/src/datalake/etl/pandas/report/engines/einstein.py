
from typing import List
from pandas.core.frame import DataFrame
from pandas.core.series import Series
from .engine import ReportingEngine

class EinsteinEngine(ReportingEngine):

    def __report_differences__(self,
        key: str,
        differences: DataFrame,
        row: list,
        static_data: List = None,
        static_columns: List = None,
        keylist: List = None,
        error: str = None,
        ) -> list:
        
        # Get static data
        df_static_data_values = "None"
        df2_static_data_values = "None"
        if static_columns != None:
            if static_columns[0] != None and isinstance(static_columns[0], list) or isinstance(static_columns[0], str):
                if isinstance(static_columns[0], str):
                    df_static_data_values = static_data[0][static_columns[0]]
                else:
                    df_static_data_values = ()
                    for i in static_columns[0]:
                        df_static_data_values.append(static_data[0][i])

            if static_columns[1] != None and isinstance(static_columns[1], list) or isinstance(static_columns[1], str):
                if isinstance(static_columns[1], str):
                    df2_static_data_values = static_data[1][static_columns[1]]
                else:
                    df2_static_data_values = ()
                    for i in static_columns[1]:
                        df2_static_data_values.append(static_data[1][i])

        # Get keylist data
        keylist_data = key
        if keylist != None:
            keylist_data = []
            for i in keylist:
                keylist_data.append(row[0][i])

        # Check if differences is None
        if differences is None:
            # Missing second row, return the content in kwargs["error"]
            return [{
                "key": key, self.rename_options["column"]: "missing_second_row", "keylist": keylist, self.value_names[0]: "", self.value_names[1]: error,
            }]

        # Build response data
        response = []
        for name, s, other in differences.itertuples():
            response.append({
                "key": key, self.rename_options["column"]: name, "keylist": key, self.value_names[0]: s, self.value_names[1]: other,
            })

        return response

    def __compare_values__(self,
        key: str,
        first: Series,
        second: Series,
        static_data: List = None,
        static_columns: List = None,
        keylist: list = None,
        ) -> DataFrame:
        
        differences = first.compare(second)
        if differences.empty:
            return []

        return self.__report_differences__(
            key, differences,
            row=[first, second],
            static_data=static_data,
            static_columns=static_columns,
            keylist=keylist,
        )

    def report(self,
        first_df: DataFrame,
        second_df: DataFrame,
        schema: dict = None,
        failfast: bool = True,
        keyindex: str = None,
        keylist: List[str] = None,
        static: List = None,
        ) -> DataFrame:


        # Normalize dataframes and get static data.
        dataframes, columns = self.__normalize_dataframes__(
            first_df={"dataframe": first_df, "static": static[0]}, 
            second_df={"dataframe": second_df, "static": static[1]},
            keyindex=keyindex)
        first_df, second_df, first_static_df, second_static_df = dataframes[0], dataframes[1], dataframes[2], dataframes[3]
        columns = list(columns)

        # Fail report
        report = []

        # for r in range(len(first_df)):
        for k, first_values in first_df.iterrows():
            try:
                second_values = second_df.loc[k]
            except KeyError:
                r = self.__report_differences__(
                    key=k,
                    row=[first_values, None],
                    differences=None,
                    static_data=[first_static_df, second_static_df],
                    keylist=keylist,
                    error="MISSING")
                report.extend(r)
                continue
            if isinstance(second_values, DataFrame):  # we got several rows
                # First report as duplicate
                r = self.__report_differences__(
                    key=k,
                    differences=None,
                    row=[first_values, second_values],
                    static_data=[first_static_df, second_static_df],
                    keylist=keylist,
                    error="DUPLICATE")
                report.extend(r)
                for _, s in second_values.iterrows():
                    r = self.__compare_values__(
                        key=k,
                        first=first_values,
                        second=s,
                    )
                    if r:
                        report.extend(r)
            else:
                r = self.__compare_values__(
                    key=k,
                    first=first_values,
                    second=second_values
                )
                if r:
                    report.extend(r)

            if failfast and len(report) > 0:
                return report

        return DataFrame(report)