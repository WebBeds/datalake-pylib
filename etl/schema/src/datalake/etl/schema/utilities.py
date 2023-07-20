#!/usr/bin/env python3

# ========================= #
# SCHEMA UTILIES            #
# ========================= #

from pandas import DataFrame


def normalize_df(df: DataFrame, schema, verbose=False, force=False, lower=False) -> DataFrame:
    if lower:
        df.columns = df.columns.str.lower()

    if force:
        available_columns = [c for c in schema.keys() if c in df.columns]
        df = df[available_columns]

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
