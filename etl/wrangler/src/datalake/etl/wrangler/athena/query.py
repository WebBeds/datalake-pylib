from awswrangler.athena import read_sql_query
import logging

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

    logging.debug(f'Reading SQL query with retry. max_retries: {max_retries}')
    logging.debug(f'kwargs: {kwargs}')

    r = 0
    ex = None
    while r <= max_retries:
        try:
            logging.debug(f'Reading SQL query with retry. Attempt: {r}')
            return read_sql_query(**kwargs)
        except Exception as e:
            logging.debug(f'Reading SQL query with retry. Exception: {e}')
            r += 1
            ex = e    
    raise RuntimeError(f'maximum number of retries ({max_retries}) reached', ex)
            