#!/usr/bin/env python3

# ========================= #
# SCHEMA TYPES              #
# ========================= #

import pandas as pd
import numpy as np
import re

from datetime import date, datetime
from pandas.api.types import is_datetime64_any_dtype


class Dummy:
    def __init__(self) -> None:
        pass

    def format(self, s: pd.Series) -> pd.Series:
        return s


class Str:
    def __init__(self, length=0, clean=False, lower=False, upper=False) -> None:
        super().__init__()
        self.length = length
        self.clean = clean
        self.lower = lower
        self.upper = upper

    def format(self, s: pd.Series) -> pd.Series:
        s = s.astype(str).fillna('')
        if self.clean:
            s = s.str.replace(re.compile(r"^(None|nan|0| +)$"), "", regex=True)
            # s = s.map(lambda x: None if x in ('None', 'nan', '0') else x)
        if self.length > 0:
            s = s.str.slice(0, self.length)
        if self.lower:
            s = s.str.lower()
        if self.upper:
            s = s.str.upper()
        return s


class Float:
    def __init__(self, round: int = 4) -> None:
        super().__init__()
        self.round = round

    def format(self, s: pd.Series) -> pd.Series:
        return s.fillna(0).astype(np.float64).round(self.round)


class Int:
    def __init__(self) -> None:
        super().__init__()

    def format(self, s: pd.Series) -> pd.Series:
        return s.fillna(0).astype(int)


class Bool:
    def __init__(self, text=False) -> None:
        super().__init__()
        self.text = text

    def format(self, s: pd.Series) -> pd.Series:
        if self.text:
            s = s.fillna(0).apply(lambda x: True if x == 'true' else False)
        else:
            s = s.fillna(0).apply(lambda x: bool(int(x)))

        return s


class ArrayString:
    def __init__(self, text=False) -> None:
        super().__init__()

    def format(self, s: pd.Series) -> pd.Series:
        return s.fillna(np.array(None)).apply(lambda x: np.array(str(x)))


class ArrayInt:
    def __init__(self, text=False) -> None:
        super().__init__()

    def format(self, s: pd.Series) -> pd.Series:
        return s.fillna(np.array(None))


class Time:
    def __init__(self, infer=True, utc=False) -> None:
        super().__init__()
        self.utc = utc
        self.infer = infer

    def format(self, s: pd.Series) -> pd.Series:
        if not is_datetime64_any_dtype(s):
            if self.infer:
                s = pd.to_datetime(s, infer_datetime_format=True, utc=self.utc)
            else:
                s = pd.to_datetime(s, format='%Y-%m-%dT%H:%M:%SZ', utc=self.utc, errors='coerce')
        elif self.utc:
            s = pd.to_datetime(s, infer_datetime_format=True, utc=True)

        if self.utc and str(s.dtypes) != str(pd.DatetimeTZDtype(tz='UTC')):
            print(f"WARNING: time was not in UTC {s.dtypes}")
            s = s.map(lambda x: x if x.tzinfo is not None else pd.to_datetime(x).tz_localize('UTC'))
        return s


class Date:
    def __init__(self, infer=True, utc=False, format=None) -> None:
        super().__init__()
        self.utc = utc
        self.infer = infer
        self.dformat = format

    def format(self, s: pd.Series) -> pd.Series:
        if not is_datetime64_any_dtype(s):
            if self.infer and self.format is None:
                s = pd.to_datetime(s, infer_datetime_format=True, utc=self.utc)
            else:
                if self.dformat is None:
                    f = '%Y-%m-%d'
                else:
                    f = self.dformat
                s = pd.to_datetime(s, format=f, utc=self.utc, errors='coerce')
        elif self.utc:
            s = pd.to_datetime(s, infer_datetime_format=True, utc=True)

        if self.utc and str(s.dtypes) != str(pd.DatetimeTZDtype(tz='UTC')):
            print(f"WARNING: time was not in UTC {s.dtypes}")
            s = s.map(lambda x: x if x.tzinfo is not None else pd.to_datetime(x).tz_localize('UTC'))

        s = s.dt.date

        return s
