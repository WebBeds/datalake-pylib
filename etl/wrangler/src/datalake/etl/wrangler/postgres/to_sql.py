from io import StringIO
from awswrangler import postgresql
from pandas import DataFrame
from csv import QUOTE_MINIMAL

def upload_df(
    df: DataFrame,
    table: str,
    filters: str,
    schema: str = "public",
    verbose: bool = False,
    dry: bool = False,
    con = None,
    default_con_name: str = "live-db",
    session = None,
) -> None:

    sep = '|'
    df = df.replace(['\n', '\r', '\r\n', f'\\{sep}'], ' ', regex=True)

    if con is None:
        con = postgresql.connect(
            connection=default_con_name,
            boto3_session=session,
        )
    cursor = con.cursor()

    where_clause = ""
    if filters:
        where_clause = f"WHERE {filters}"

    table_with_schema = f"{schema}.{table}"
    
    truncate_query = f"""DELETE FROM {table_with_schema} {where_clause};"""
    if verbose:
        print(truncate_query)
    if not dry:
        cursor.execute(truncate_query)

    # Generate a CSV buffer
    csv_buffer = StringIO()
    df.to_csv(
        csv_buffer, sep=sep, na_rep='', header=False, index=False,
        quoting=QUOTE_MINIMAL, quotechar='"', escapechar='\\', doublequote=False
    )
    csv_buffer.seek(0)

    def copy_from(
        file_buffer: StringIO,
        table: str,
        columns: list,
        sep: str,
        null: str,
    ) -> None:
        copy_query = f"""COPY {table} ({','.join(columns)}) FROM STDIN WITH (FORMAT CSV, NULL '{null}', DELIMITER '{sep}');"""
        cursor.execute(copy_query, stream=file_buffer)

    if not dry:
        copy_from(
            file_buffer=csv_buffer,
            table=table_with_schema,
            columns=list(df.columns),
            sep=sep,
            null=''
        )

    con.commit()
    cursor.close()
    con.close()

    if verbose:
        print("DB finished. Inserted {} registers".format(len(df)))