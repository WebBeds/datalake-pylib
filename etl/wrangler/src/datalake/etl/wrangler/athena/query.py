from awswrangler.athena import read_sql_query

def read_sql_query_with_retry(
    max_retries: int = 3,
    **kwargs,
):
    """
    Read SQL query with python retry.

    :param max_retries: Maximum number of retries.
    :param kwargs: Keyword arguments for read_sql_query.

    :return: Dataframe or Generator of Dataframes.
    """
    if max_retries < 1:
        raise ValueError('max_retries must be greater than 0.')
    for retry in range(max_retries):
        try:
            dfs = read_sql_query(**kwargs)
            return dfs
        except Exception as e:
            if retry == (max_retries - 1):
                raise e