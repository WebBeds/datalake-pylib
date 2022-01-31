# Pandas Package of ETL Library

Make actions with Pandas DataFrames, on this version you can generate a report of the differences between two DataFrames.

## Install (pip3)

Using install command:

```bash
pip3 install "git+ssh://github.com/Webjet/datalake-pylib#egg=datalake-etl-pandas&subdirectory=etl/pandas"
```

Inside requirements:

```bash
git+ssh://github.com/Webjet/datalake-pylib#egg=datalake-etl-pandas&subdirectory=etl/pandas
```

## Installation (gpip) (Deprecated)

Using get command:

```bash
gpip get github.com/Webjet/datalake-pylib/etl/pandas:datalake-etl-pandas
```

Specified on requirements:

```
github.com/Webjet/datalake-pylib/etl/pandas:datalake-etl-pandas
```

## Import

```python
from datalake.etl import pandas
```

### Return schemas

```python
_RETURN_METHOD_SCHEMA = {
    "success": True or False,
    "fail": "Fail message (Customizable)"
}
```

```python
_RETURN_SCHEMA = {
    "success": True or False,
    "fail": {
        "message": "Fail message (Customizable)"
        ,"column": "Column where the fail occurs"
        ,"failindex": ["Array of columns"] | "Column"
        ,"type": ["Type of the column"]
        ,"value": "First value (Comparison)"
        ,"value2": "Second value (Comparison)"
        ,"time": "Datetime where the fail occurs (datetime)"
    }
}
```

```python
_FUNCTIONS_SCHEMA = {
    "column_to_check (case-sensitive)": ["Array of methods"]
}
```
