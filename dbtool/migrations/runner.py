from __future__ import annotations
import pathlib
import time
import re

from dbtool.constants import LOCK_NAME, SCHEMA_HISTORY_TABLE
from dbtool.driver import connection
from dbtool.history import ensure_history_table, record_success
from dbtool.migrations.reader import discover, MigrationFile
from dbtool.utils import exec_multi
from dbtool.config import Environment

_DROP_RE = re.compile(r"\bDROP\b", re.IGNORECASE)


class MigrationRunner:
    """
    Executes versioned / repeatable migrations **inside one service directory**
    (services/<name>/db/migrations).  The caller decides *which* service by
    passing the correct *base_dir*.
    """

    def __init__(self, env: Environment, base_dir: pathlib.Path) -> None:
        self.env: Environment = env
        self.base_dir: pathlib.Path = base_dir
        self.migrations_dir: pathlib.Path = base_dir / "db" / "migrations"

        self.migrations_dir.mkdir(parents=True, exist_ok=True)
        
    def status(self) -> dict:
        with connection(self.env) as conn, conn.cursor(dictionary=True) as cur:
            ensure_history_table(cur)
            cur.execute(
                f"SELECT version, checksum FROM {SCHEMA_HISTORY_TABLE} ORDER BY installed_rank"
            )
            applied = {row["version"]: row for row in cur.fetchall()}

        pending, applied_list, mismatch = [], [], []
        for mf in discover(self.migrations_dir):
            if mf.repeatable:
                continue
            if mf.version not in applied:
                pending.append(mf)
            else:
                db_checksum = applied[mf.version]["checksum"]
                if db_checksum != mf.checksum:
                    mismatch.append((mf, db_checksum))
                applied_list.append(mf)

        return {"applied": applied_list, "pending": pending, "mismatch": mismatch}

    def migrate(
        self,
        *,
        dry_run: bool = False,
        target: str | None = None,
        allow_destructive: bool = False,
    ) -> None:
        files = discover(self.migrations_dir)

        with connection(self.env) as conn, conn.cursor(buffered=True) as cur:
            ensure_history_table(cur)

            cur.execute("SELECT GET_LOCK(%s, 10)", (LOCK_NAME,))
            if cur.fetchone()[0] != 1:
                raise RuntimeError("Could not acquire migration lock – another run in progress.")

            try:
                cur.execute(f"SELECT version FROM {SCHEMA_HISTORY_TABLE}")
                already_applied = {row[0] for row in cur.fetchall() if row[0]}

                for mf in files:
                    if mf.repeatable:
                        continue
                    if target and mf.version > target:
                        break
                    if mf.version in already_applied:
                        continue
                    self._apply_single(cur, mf, dry_run, allow_destructive)

                for mf in files:
                    if not mf.repeatable:
                        continue
                    self._apply_single(cur, mf, dry_run, allow_destructive, repeatable=True)

            finally:
                cur.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))

    def _apply_single(
        self,
        cur,
        mf: MigrationFile,
        dry_run: bool,
        allow_destructive: bool,
        repeatable: bool = False,
    ) -> None:
        print(f"{'(DRY) ' if dry_run else ''}Applying {mf.path.name}")

        if not allow_destructive and _DROP_RE.search(mf.sql):
            raise RuntimeError(
                f"{mf.path.name} contains DROP statements – re‑run with --allow-destructive to proceed."
            )

        if dry_run:
            print(mf.sql)
            return

        start = time.perf_counter()
        exec_multi(cur, mf.sql)
        duration_ms = int((time.perf_counter() - start) * 1000)

        record_success(
            cur,
            version=None if repeatable else mf.version,
            desc=mf.description,
            typ="Repeatable" if repeatable else "SQL",
            script=mf.path.name,
            checksum=mf.checksum,
            exec_ms=duration_ms,
        )
