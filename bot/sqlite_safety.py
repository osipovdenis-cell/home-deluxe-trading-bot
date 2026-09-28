"""Short analytics writes and recovery from transient SQLite contention."""
import sqlite3
import traceback
import time
from functools import wraps


def is_busy(error):
    code = getattr(error, 'sqlite_errorcode', None)
    return isinstance(error, sqlite3.OperationalError) and (
        code & 0xff in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) if code is not None
        else str(error) in ('database is locked', 'database table is locked'))


def retry_initialization(init):
    """Retry idempotent schema/account initialization only, never a trade.

    A failed constructor must close its connection before another attempt. This
    also releases a writer acquired by an earlier step of a partial migration.
    """
    @wraps(init)
    def initialize(self, *args, **kwargs):
        deadline = time.monotonic() + 60
        while True:
            try:
                return init(self, *args, **kwargs)
            except Exception as error:
                connection = getattr(self, 'connection', None)
                if connection is not None:
                    connection.close()
                if not is_busy(error) or time.monotonic() >= deadline:
                    raise
                print('SQLite initialization busy; retrying before starting worker', flush=True)
                time.sleep(.5)
    return initialize


def write_batches(connection, sql, rows, size=100):
    """Compute rows before calling; never hold a write lock during analysis/I/O.

    Each batch is atomic. Callers use idempotent analytical rows, so a later
    failure may leave earlier batches committed without duplicating outcomes.
    This helper is deliberately not used for purchases or account balances.
    """
    for start in range(0, len(rows), size):
        with connection:
            connection.executemany(sql, rows[start:start + size])


def recover_busy(error, *connections):
    """Rollback unfinished work, never replay a possibly committed trade."""
    code = getattr(error, 'sqlite_errorcode', None)
    if not isinstance(error, sqlite3.OperationalError) or (
        (code is not None and code & 0xff not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED))
        or (code is None and str(error) not in ('database is locked', 'database table is locked'))
    ):
        raise error
    for connection in connections:
        if connection is not None:
            connection.rollback()
    # No SQL, parameters, credentials or exception message in diagnostic logs.
    frames = traceback.extract_tb(error.__traceback__)
    location = ' > '.join(f'{frame.name}:{frame.lineno}' for frame in frames[-4:])
    print(f'SQLite busy recovered: OperationalError code={code}; at={location}', flush=True)
