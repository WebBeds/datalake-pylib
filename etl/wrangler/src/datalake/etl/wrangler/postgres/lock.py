import contextlib
import datetime
import functools
import hashlib

import pg8000
import pg8000.core


def _cast_timeout(timeout):
    """Cast a timeout into a timedelta object."""
    if timeout is None or (timeout is not None and timeout <= 0):
        return timeout
    if isinstance(timeout, (int, float)):
        timeout = datetime.timedelta(seconds=timeout)
    if not isinstance(timeout, datetime.timedelta):
        raise TypeError("Must supply int, float, or timedelta for timeout")
    if timeout < datetime.timedelta(milliseconds=1):
        timeout = datetime.timedelta()
    return timeout


def _cast_lock_id(lock_id):
    """Cast a lock_id into an int."""
    if isinstance(lock_id, str):
        return int.from_bytes(
            hashlib.sha256(lock_id.encode("utf-8")).digest()[:8], "little", signed=True
        )
    elif isinstance(lock_id, int):
        return lock_id
    else:
        raise TypeError(f'Lock ID "{lock_id}" is not a string or int')


class PGLock(contextlib.ContextDecorator):
    """
    A context manager that provides a PostgreSQL advisory lock.

    The lock is acquired when the context is entered and released when the context is exited.

    - lock_id: The lock ID to use. This can be an int or a string. If a string is provided, it will be hashed to an int.
    - conn: A pg8000 connection object.
    - timeout: The number of seconds to wait for the lock. If None, the lock will wait indefinitely. If 0, the lock will not wait at all.
    """

    @contextlib.contextmanager
    def _lock_timeout(self):
        sql = f"""
            SET lock_timeout = {self.timeout.total_seconds() * 1000}
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                yield
        finally:
            sql = f"""
                SET lock_timeout = DEFAULT
            """
            with self.conn.cursor() as cursor:
                cursor.execute(sql)

    def __init__(
        self,
        lock_id: str,
        conn: pg8000.Connection,
        timeout: int = None,
    ) -> None:
        self.lock_id = _cast_lock_id(lock_id)
        self.conn = conn
        self.timeout = _cast_timeout(timeout)
        self.wait = self.timeout is not None

    def __enter__(self):
        sql = f"""
            SELECT pg_advisory_lock($1)
        """
        self.acquired = False
        if not self.wait:
            sql = f"""
                SELECT pg_try_advisory_lock($1)
            """
        try:
            with self.conn.cursor() as cursor:
                with contextlib.ExitStack() as stack:
                    if self.wait:
                        stack.enter_context(self._lock_timeout())
                    cursor.execute(sql, (self.lock_id,))
                    self.acquired = True
                    if not self.wait:
                        self.acquired = cursor.fetchone()[0]
        except pg8000.ProgrammingError:
            raise TimeoutError("Timeout while acquiring lock") from None
        except Exception:
            raise
        return self.acquired

    def __exit__(self, exc_type, exc_value, traceback):
        sql = f"""
            SELECT pg_advisory_unlock($1)
        """
        with self.conn.cursor() as cursor:
            cursor.execute(sql, (self.lock_id,))
            self.adquired = False

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)

        return wrapper
