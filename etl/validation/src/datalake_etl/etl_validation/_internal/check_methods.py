#!/usr/bin/env python3

# ========================= #
# DEFAULT CHECK METHODS     #
# ========================= #

from pandas import Timestamp, Timedelta
from numpy import float64

def default_date_check(v: Timestamp, v2: Timestamp) -> dict:
    v_time = v.to_datetime64()
    v2_time = v2.to_datetime64()
    second_treshold = 0
    if Timedelta(v_time - v2_time).total_seconds() != second_treshold:
        return {"success": False, "fail": "Time error, future date detected"}
    return {"success": True}

def default_check(v,v2) -> dict:
    
    # Check if one of the values is Timestamp
    isDate = type(v) == Timestamp or type(v2) == Timestamp
    # Check if one of the values is nan
    isNan = (str(v).lower() == "nan" or str(v).lower() == "nat") or (str(v2).lower() == "nan" or str(v2).lower() == "nat")

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