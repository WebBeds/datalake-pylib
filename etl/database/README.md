# Database Package of ETL Library

**Work in progress**

Manage Athena and Postgres DataFrames and queries.

## Install (pip3)

Using install command:

```bash
pip3 install "git+https://www.github.com/Webjet/datalake-pylib#egg=datalake-etl-database&subdirectory=etl/database"
```

Inside requirements:

```bash
git+https://www.github.com/Webjet/datalake-pylib#egg=datalake-etl-database&subdirectory=etl/database
```

## Install (gpip) (Deprecated)

Using get command:

```bash
gpip get github.com/Webjet/datalake-pylib/etl/database:datalake-etl-database
```

Specified on requirements:

```bash
github.com/Webjet/datalake-pylib/etl/database:datalake-etl-database
```

## Import

```python
from datalake.etl import database
```