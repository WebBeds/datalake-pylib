#!/usr/bin/env python3

# ========================= #
# SCHEMA UTILIES            #
# ========================= #

from pandas import DataFrame

def normalize_df(df: DataFrame, schema, verbose=False) -> DataFrame:
    for c in df.columns:
        t = schema.get(c, None)
        if t is None:
            # print(f"WARNING: {c} not in schema")
            continue
        df[c] = t.format(df[c])

    if verbose:
        print(df.dtypes)
        print(df.head)

    return df