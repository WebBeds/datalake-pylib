from typing import Any
from psycopg2.extras import execute_batch
import pandas as pd

def _does_table_exists(
    cursor: Any,
    table: str,
    schema: str = None,
) -> bool:
    schema_str = f'"{schema}".' if schema else ''
    cursor.execute(
        f"SELECT true WHERE EXISTS ("
        f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE "
        f"{schema_str} TABLE_NAME = '{table}'"
        f");"
    )
    return len(cursor.fetchall()) > 0

def _get_pg_type(
    s: pd.Series
) -> str:
    if s.dtype == "int64":
        return "bigint"
    elif s.dtype == "int32":
        return "integer"
    elif s.dtype == "float32":
        return "float"
    elif s.dtype == "float64":
        return "double"
    elif s.dtype == "bool":
        return "boolean"
    elif s.dtype == "datetime64[ns]":
        return "timestamp"
    elif s.dtype == "date":
        return "date"
    elif s.dtype == "object":
        return "varchar"
    else:
        raise Exception(f"Unknown type: {s.dtype}")

def _get_postgres_types(
    df: pd.DataFrame
) -> dict:
    return {col: _get_pg_type(df[col]) for col in df.columns}

def _create_table(
    cursor: Any,
    df: pd.DataFrame,
    table: str,
    schema: str = "public",
    verbose: bool = False,
) -> None:
    if _does_table_exists(cursor, table, schema):
        return
    pgt = _get_postgres_types(df)
    cols_str: str = "".join([f'"{k}" {v},\n' for k, v in pgt.items()])
    sql = f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{cols_str});"
    if verbose:
        print(sql)
    cursor.execute(sql)

def _get_upsert(
    table: str,
    pk_fields: list,
    fields: list,
    schema: str = None,
    where_clause: str = None,
) -> str:
    assert len(pk_fields) > 0, "At least one primary key field is required."
    assert len(fields) > 0, "At least one field is required."
    if schema:
        rel = '%s.%s' % (schema, table)
    else:
        rel = table
    insert_placeholders = ', '.join('%%(%s)s' % f for f in fields)
    udpdate_placeholders = ', '.join('%s=%%(%s)s' % (f, f) for f in fields if f not in pk_fields)
    return f"""
        INSERT INTO {rel} ({','.join(fields)}) VALUES ({insert_placeholders}) 
        ON CONFLICT ({','.join(pk_fields)}) DO 
        UPDATE SET {udpdate_placeholders}
        {where_clause if where_clause else ''}
    """

def upsert_index(
    cursor: Any,
    df: pd.DataFrame,
    pk_fields: list,
    table: str,
    schema: str,
    where_clause: str = None,
    page_size: int = 1024,
    create_table: bool = True,
    verbose: bool = False,
    dry: bool = False,
) -> None:
 
    upsert_sql = _get_upsert(
        table=table,
        pk_fields=pk_fields,
        fields=list(df.columns),
        schema=schema,
        where_clause=where_clause,
    )

    if verbose:
        print(upsert_sql)

    if dry:
        return
    
    if create_table:
        _create_table(cursor, df, table, schema, verbose)
    iter = (r[1] for r in df.iterrows())

    execute_batch(cursor, upsert_sql, iter, page_size=page_size)