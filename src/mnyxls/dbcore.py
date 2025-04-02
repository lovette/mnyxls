from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

    from mysqlstmt import Stmt


######################################################################
# Public functions


def db_execute_stmt(conn: sqlite3.Connection, q: Stmt) -> sqlite3.Cursor:
    """Execute a mysqlstmt query against a SQLite database connection.

    Args:
        conn (sqlite3.Connection): SQLite connection.
        q (Stmt): mysqlstmt

    Returns:
        Cursor
    """
    sql, params = q.sql()

    # > logger.debug(f"SQL> {sql} {params}")

    if params is None:
        params = []  # sqlite doesn't allow None

    return conn.execute(sql, params)
