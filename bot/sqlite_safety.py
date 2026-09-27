"""Short analytics writes and recovery from transient SQLite contention."""
import sqlite3
import traceback


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
