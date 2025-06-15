"""
`dbtool schema-apply` implementation – pushes the declarative schema
directory to the target database.
"""
from __future__ import annotations
import pathlib, time, datetime as dt
import click
from dbtool.config import Environment
from dbtool.driver import connection
from dbtool.history import ensure_history_table, record_success
from dbtool.schema.diff import diff as calc_diff
from dbtool.utils import exec_multi


def apply_schema(
    env: Environment,
    project_dir: pathlib.Path,
    *,
    auto_approve: bool = False,
    allow_destructive: bool = False,
    dry_run: bool = False,
) -> None:
    schema_dir = project_dir / "db" / "schema"
    stmts = calc_diff(env, schema_dir, allow_destructive=allow_destructive)

    if not stmts:
        click.echo("✅  Database already matches schema directory.")
        return

    click.echo("\n".join(stmts))
    if dry_run:
        click.echo("\n-- DRY‑RUN complete (no changes executed)\n")
        return

    if not auto_approve:
        click.confirm("\nProceed with executing the above statements?", abort=True)

    with connection(env) as conn, conn.cursor() as cur:
        ensure_history_table(cur)
        start = time.perf_counter()
        for stmt in stmts:
            if stmt.startswith("--"):
                continue
            exec_multi(cur, stmt)
        duration = int((time.perf_counter() - start) * 1000)
        ver = f"schema-sync-{dt.datetime.utcnow():%Y%m%d%H%M%S}"
        record_success(
            cur,
            version=ver,
            desc="declarative‑sync",
            typ="Declarative",
            script=f"{ver}.auto",
            checksum="",
            exec_ms=duration,
        )
    click.echo(f"✅  Applied schema sync in {duration} ms.")
