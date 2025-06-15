"""
Auto‑generate a versioned migration from schema diff.
"""
from __future__ import annotations
import datetime as dt, pathlib
from dbtool.schema.diff import diff as calc_diff
from dbtool.config import Environment


def generate(
    env: Environment,
    project_dir: pathlib.Path,
    *,
    description: str,
    allow_destructive: bool = False,
) -> pathlib.Path | None:
    migrations_dir = project_dir / "db" / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)

    stmts = calc_diff(env, project_dir / "db" / "schema", allow_destructive=allow_destructive)
    if not stmts:
        return None

    stamp = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    safe_desc = description.strip().lower().replace(" ", "_")
    fname = f"V{stamp}__{safe_desc}.sql"
    path = migrations_dir / fname
    path.write_text(";\n\n".join(stmts) + ";\n", encoding="utf-8")
    return path
