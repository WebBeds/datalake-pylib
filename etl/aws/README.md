# AWS Package of ETL Library

**Work in progress**

Work with AWS Services like SecretsManager and Others.

## Install (pip3)

Using install command:

```bash
pip3 install "git+ssh://github.com/Webjet/datalake-pylib#egg=datalake-etl-aws&subdirectory=etl/aws"
```

Inside requirements:

```bash
git+ssh://github.com/Webjet/datalake-pylib#egg=datalake-etl-aws&subdirectory=etl/aws
```

## Install (gpip) (Deprecated)

Using get command: (EARLY)

```bash
gpip get github.com/Webjet/datalake-pylib/etl/aws:datalake-etl-aws
```

Specified on requirements:

```bash
github.com/Webjet/datalake-pylib/etl/aws:datalake-etl-aws
```

## Import

```python
from datalake.etl import aws
```

## Modules

### Secrets Manager

```python
from datalake.etl.aws.secretsmanager import get_secret
```

### Lambda

* Check if the executor isLambda or not.

```python
from datalake.etl.aws.awslambda import is_lambda
```

* Extract information of ObjectCreate event of lambda functions.

```python
from datalake.etl.aws.awslambda.events.object_create import get_s3_trigger_object_bucket, get_s3_trigger_object_key, get_s3_trigger_object_account
```
