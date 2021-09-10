# AWS Package of ETL Library

**Work in progress**

Work with AWS Services like SecretsManager and Others.

## Install

Using get command: (EARLY)

```bash
gpip get github.com/Webjet/datalake-pylib/etl/aws:datalake-etl.etl-aws
```

Specified on requirements:

```bash
github.com/Webjet/datalake-pylib/etl/aws:datalake-etl.etl-aws
```

## Import

```python
from datalake_etl import etl_aws
```

## Modules

### Secrets Manager

```python
from datalake_etl.etl_aws.secretsmanager import get_secret
```

### Lambda

* Check if the executor isLambda or not.

```python
from datalake_etl.etl_aws.aws_lambda import isLambda
```

* Extract information of ObjectCreate event of lambda functions.

```python
from datalake_etl.etl_aws.aws_lambda.events.object_create import get_s3_trigger_object_bucket, get_s3_trigger_object_key, get_s3_trigger_object_account
```
