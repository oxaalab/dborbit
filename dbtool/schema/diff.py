from __future__ import annotations

import difflib
import pathlib
import sqlparse

from dbtool.constants import SCHEMA_HISTORY_TABLE
from dbtool.driver import connection
from dbtool.config import Environment
from dbtool.utils import split_sql

IGNORED_TABLES = {SCHEMA_HISTORY_TABLE}


def _current_schema(cur) -> dict[str, str]:
    """Return the current DB schema as {table_name: DDL‑text}."""
    cur.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
    tables = [r[0] for r in cur.fetchall() if r[0] not in IGNORED_TABLES]
    ddl: dict[str, str] = {}
    for t in tables:
        cur.execute(f"SHOW CREATE TABLE `{t}`")
        ddl[t] = cur.fetchone()[1] + ";"
    return ddl


def _collect_from_sql(sql_text: str, ddl: dict[str, str]) -> None:
    """
    Populate *ddl* mapping with CREATE‑TABLE statements found in *sql_text*.
    """
    for stmt in split_sql(sql_text):
        parsed = sqlparse.parse(stmt)
        if not parsed:
            continue
        ident = parsed[0].get_name()
        if ident and ident not in IGNORED_TABLES:
            ddl[ident] = stmt.rstrip(";") + ";"


def _desired_schema(schema_path: pathlib.Path) -> dict[str, str]:
    """
    Build the “desired” schema mapping.

    • If *schema_path* is a **directory** ⇒ read every *.sql* file inside it.  
    • If it is a **file**               ⇒ parse that file.  
    • If the directory does **not** exist but a sibling ``schema.sql`` file
      does, that file is used transparently.

    Returns {table_name: DDL‑text}.
    """
    ddl: dict[str, str] = {}

    if not schema_path.exists():
        fallback = schema_path.with_suffix(".sql")
        if fallback.exists():
            schema_path = fallback

    if schema_path.is_file():
        _collect_from_sql(schema_path.read_text(encoding="utf-8"), ddl)
        return ddl

    if schema_path.is_dir():
        for file in schema_path.glob("**/*.sql"):
            _collect_from_sql(file.read_text(encoding="utf-8"), ddl)
    return ddl


def diff(
    env: Environment,
    schema_dir: pathlib.Path,
    *,
    allow_destructive: bool = False,
) -> list[str]:
    """
    Return SQL statements that transform *env* so it matches the files in
    *schema_dir* (or a neighbouring *schema.sql*).
    """
    with connection(env) as conn, conn.cursor() as cur:
        current = _current_schema(cur)

    desired = _desired_schema(schema_dir)
    sql_changes: list[str] = []

    for tbl in desired.keys() - current.keys():
        sql_changes.append(desired[tbl])

    for tbl in current.keys() - desired.keys():
        drop_stmt = f"DROP TABLE `{tbl}`;"
        if allow_destructive:
            sql_changes.append(drop_stmt)
        else:
            sql_changes.append(f"-- !!! would execute `{drop_stmt}` (destructive)")

    for tbl in desired.keys() & current.keys():
        if desired[tbl].strip() != current[tbl].strip():
            sql_changes.append(f"-- diff for table `{tbl}`")
            for l in difflib.unified_diff(
                current[tbl].splitlines(),
                desired[tbl].splitlines(),
                fromfile="current",
                tofile="desired",
                lineterm="",
            ):
                sql_changes.append(f"-- {l}")
            sql_changes.append(f"-- manual ALTER required for `{tbl}`")

    return sql_changes
