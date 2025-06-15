from __future__ import annotations
import pathlib, sqlparse, difflib
from dbtool.driver import connection
from dbtool.config import Environment
from dbtool.utils import split_sql


def _current_schema(cur) -> dict[str, str]:
    cur.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
    tables = [row[0] for row in cur.fetchall()]
    ddl = {}
    for t in tables:
        cur.execute(f"SHOW CREATE TABLE `{t}`")
        ddl[t] = cur.fetchone()[1] + ";"
    return ddl


def _desired_schema(schema_dir: pathlib.Path) -> dict[str, str]:
    ddl: dict[str, str] = {}
    for p in schema_dir.glob("**/*.sql"):
        sql = p.read_text(encoding="utf-8")
        tokens = sqlparse.parse(sql)
        if not tokens:
            continue
        ident = tokens[0].get_name()
        if ident:
            ddl[ident] = sql.rstrip(";") + ";"
    return ddl


def diff(
    env: Environment,
    schema_dir: pathlib.Path,
    *,
    allow_destructive: bool = False,
) -> list[str]:
    """
    Return a list of SQL statements (strings) that would bring *env*
    into alignment with *schema_dir*.
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

    common = desired.keys() & current.keys()
    for tbl in common:
        if desired[tbl].strip() != current[tbl].strip():
            sql_changes.append(f"-- diff for table `{tbl}`")
            diff_lines = difflib.unified_diff(
                current[tbl].splitlines(),
                desired[tbl].splitlines(),
                fromfile="current",
                tofile="desired",
                lineterm="",
            )
            for l in diff_lines:
                sql_changes.append("-- " + l)
            sql_changes.append(f"-- manual ALTER required for `{tbl}`")

    return sql_changes
