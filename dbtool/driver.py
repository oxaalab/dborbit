from __future__ import annotations
import mysql.connector
from mysql.connector import errorcode
from contextlib import contextmanager

from dbtool.config import Environment


@contextmanager
def connection(env: Environment):
    """
    Context‑manager that yields a **connection already inside the target
    database**.  If the database does not yet exist (error 1049), it will be
    created automatically for non‑production environments.

    This “just work” behaviour is intentionally enabled only when
    ``env.allow_destructive`` is *True* (typically dev / CI boxes) so that
    production mis‑spells still fail loudly.
    """
    try:
        conn = mysql.connector.connect(**env.dsn(), autocommit=False)

    except mysql.connector.Error as err:
        if (
            err.errno == errorcode.ER_BAD_DB_ERROR
            and env.allow_destructive          # safe guard‑rail
        ):
            # ----------------------------------------------------------------
            # Auto‑create the schema
            # ----------------------------------------------------------------
            bootstrap_dsn = env.dsn().copy()
            bootstrap_dsn.pop("database", None)          # connect to server only
            with mysql.connector.connect(**bootstrap_dsn, autocommit=True) as tmp:
                with tmp.cursor() as cur:
                    print(
                        f"[dbtool] Database {env.database!r} not found – creating ..."
                    )
                    cur.execute(
                        f"CREATE DATABASE `{env.database}` "
                        "DEFAULT CHARACTER SET utf8mb4 "
                        "COLLATE utf8mb4_unicode_ci"
                    )

            # Retry now that DB exists
            conn = mysql.connector.connect(**env.dsn(), autocommit=False)
        else:
            # Bubble up anything else (bad credentials, network failure, etc.)
            raise

    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
