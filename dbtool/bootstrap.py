"""
…
    auth:
      database: auth
      user:     auth_app
      password: anotherS3cret
      path:     backend/auth-service        # NEW – optional
      privileges: SELECT,INSERT,UPDATE,DELETE
"""

from __future__ import annotations

import pathlib
import typing as t

import mysql.connector
import yaml

from dbtool.config import Environment


def _open_server_conn(env: Environment):
    """
    Connect to the MariaDB *server* (no default schema selected).
    """
    dsn = env.dsn().copy()
    dsn.pop("database", None)
    return mysql.connector.connect(**dsn, autocommit=True)


def bootstrap(
    env: Environment,
    services_file: pathlib.Path,
    *,
    host_wildcard: str = "%",
    dry_run: bool = False,
) -> None:
    """
    Create databases / users described in *services_file* for *env*.
    """
    data: dict[str, t.Any]
    try:
        data = yaml.safe_load(services_file.read_text()) or {}
    except FileNotFoundError as exc:
        raise RuntimeError(f"Bootstrap file {services_file} not found") from exc

    services: dict[str, dict[str, str]] = data.get("services") or {}
    if not services:
        raise RuntimeError(f"No `services` section defined in {services_file}")

    with _open_server_conn(env) as conn, conn.cursor() as cur:
        for svc_name, meta in services.items():
            db = meta["database"]
            user = meta["user"]
            pwd = meta["password"]
            priv = meta.get("privileges", "ALL")

            print(f"▶ Provisioning service {svc_name!r}  (db={db}, user={user})")

            stmts = [
                f"CREATE DATABASE IF NOT EXISTS `{db}` "
                "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
                f"CREATE USER IF NOT EXISTS '{user}'@'{host_wildcard}' IDENTIFIED BY %s",
                f"GRANT {priv} ON `{db}`.* TO '{user}'@'{host_wildcard}'",
                "FLUSH PRIVILEGES",
            ]

            if dry_run:
                for s in stmts:
                    printable = s if "%s" not in s else s.replace("%s", "'***'")
                    print("  ", printable, ";")
                continue

            cur.execute(stmts[0])
            cur.execute(stmts[1], (pwd,))
            cur.execute(stmts[2])
            cur.execute(stmts[3])

    if dry_run:
        print("\n-- DRY‑RUN complete (no changes executed)")
    else:
        print("✅  Bootstrap finished.")
