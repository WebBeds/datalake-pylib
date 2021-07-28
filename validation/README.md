# Validation Package

## Installation

Get command: (EARLY)

```bash
gpip get github.com/Webjet/datalake-pylib@validation#name=datalake-validation\;branch=package.validation
```

On requirements:

github.com/Webjet/datalake-pylib@validation#name=datalake-validation;branch=package.validation

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
