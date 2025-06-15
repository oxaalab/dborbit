"""
Generic helpers that are reused across sub‑modules.
"""
from __future__ import annotations
import sqlparse
import mysql.connector


def split_sql(sql: str) -> list[str]:
    """
    Split a string containing one or many SQL statements into individual
    statements **safely** (aware of literals, comments, delimiters, etc.).
    """
    return [s.strip() for s in sqlparse.split(sql) if s.strip()]


def exec_multi(cur: mysql.connector.cursor_cext.CMySQLCursor, sql: str) -> None:
    """Execute a multi‑statement SQL script."""
    for stmt in split_sql(sql):
        cur.execute(stmt)
