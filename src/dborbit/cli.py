#!/usr/bin/env python3
"""
CLI entry for dborbit MariaDB bootstrap.
"""
from pathlib import Path
import argparse
import getpass
import os

from dborbit.manifest import load_manifest
from dborbit.admin import (
    connect_admin, db_exists, db_has_tables, user_exists, priv_granted,
    create_database, create_user, grant
)
from dborbit.schema import (
    schema_diff, import_schema, compute_schema_file_hash, print_schema_diff_box
)
from dborbit.tracking import (
    get_applied_schema_hash, set_applied_schema_hash
)

def bootstrap(manifest_file: str):
    manifest_path = Path(manifest_file)
    services = load_manifest(manifest_path)["services"]

    admin = os.getenv("DB_ADMIN_USER") or input("MariaDB admin user [root]: ") or "root"
    pwd = os.getenv("DB_ADMIN_PASSWORD") or getpass.getpass(f"Password for '{admin}': ")

    print(f"[INFO] Connecting to MariaDB on localhost:3307 as '{admin}' …")
    cnx = connect_admin(admin, pwd)
    cur = cnx.cursor()

    for svc, cfg in services.items():
        db, user, pw = cfg["database"], cfg["user"], cfg["password"]
        priv = cfg.get("privileges", "ALL")
        schema_path = Path(os.path.expanduser(cfg.get("path", ""))) if "path" in cfg else None

        print(f"[INFO] Provisioning {svc}")

        if not db_exists(cur, db):
            create_database(cur, db)
            print(f"       ├─ created database `{db}`")
        else:
            print(f"       ├─ database `{db}` exists – skipped")

        if not user_exists(cur, user):
            create_user(cur, user, pw)
            print(f"       ├─ created user '{user}'")
        else:
            print(f"       ├─ user '{user}' exists – skipped")

        if not priv_granted(cur, user, db, priv):
            grant(cur, user, db, priv)
            print(f"       ├─ granted '{priv.upper()}'")
        else:
            print(f"       ├─ privileges OK – skipped")

        if not schema_path:
            print(f"       └─ no schema path provided")
            continue
        if not schema_path.is_file():
            print(f"       └─ [WARN] schema file {schema_path} missing – skipped")
            continue

        file_hash = compute_schema_file_hash(schema_path)
        prev_hash = get_applied_schema_hash(cur, db, schema_path)

        if prev_hash == file_hash:
            print(f"       └─ schema already applied (hash matched) – skipped")
            continue

        if not db_has_tables(cur, db):
            resp = input(
                f"       !! Initial schema will be imported into `{db}` from `{schema_path}`. Proceed? [y/N]: "
            ).strip().lower()
            if resp != "y":
                print(f"       └─ skipped by user.")
                continue
            import_schema(cur, db, schema_path)
            set_applied_schema_hash(cur, db, schema_path, file_hash)
            print(f"       └─ imported initial schema from {schema_path}")
            continue

        new, missing, altered = schema_diff(cur, db, schema_path)
        if not (new or missing or altered):
            set_applied_schema_hash(cur, db, schema_path, file_hash)
            print(f"       └─ schema identical – skipped")
            continue

        print("       └─ schema differences detected:")
        print_schema_diff_box(new, missing, altered)

        resp = input(f"           Apply schema file to `{db}`? [y/N]: ").strip().lower()
        if resp != "y":
            print("           → skipped by user.")
            continue

        try:
            import_schema(cur, db, schema_path)
            set_applied_schema_hash(cur, db, schema_path, file_hash)
            print("           → schema applied.")
        except Exception as e:
            print(f"           ! schema apply failed: {e}")

    cur.close()
    cnx.close()
    print("[DONE]")

def cli():
    ap = argparse.ArgumentParser(description="Bootstrap MariaDB & manage schemas.")
    ap.add_argument("manifest", nargs="?", default=None, help="services.yaml|toml")
    ap.add_argument("-f", "--file", dest="manifest_opt", help="same as positional")
    args = ap.parse_args()

    manifest = args.manifest_opt or args.manifest or (
        "services.toml" if Path("services.toml").exists() else "services.yaml"
    )
    bootstrap(manifest)

if __name__ == "__main__":
    cli()
