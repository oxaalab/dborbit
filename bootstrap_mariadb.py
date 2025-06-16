#!/usr/bin/env python3
"""
bootstrap_mariadb.py – v1.6.2
────────────────────────────
Provision MariaDB databases, users and (optionally) schemas for the micro‑
services described in a TOML or YAML manifest.

▪ Always executes **CREATE DATABASE IF NOT EXISTS** (idempotent & reliable)  
▪ Ensures user account + password, re‑grants privileges each run  
▪ Schema import / diff logic unchanged from v1.6.1
"""

from __future__ import annotations

# ––– std‑lib ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
import argparse, datetime as _dt, difflib, getpass, os, re, sys
from hashlib import md5
from pathlib import Path
from typing import Dict, Generator, List, Tuple

# ––– third‑party ––––––––––––––––––––––––––––––––––––––––––––––––––––––––
import mysql.connector                                   # type: ignore

# ╭──────────────────────── Manifest loader ─────────────────────────────╮
try:
    import tomllib as _toml                              # Py ≥3 . 11
except ModuleNotFoundError:                              # pragma: no cover
    import tomli as _toml                                # type: ignore[assignment]


def _load_manifest(path: Path) -> Dict:
    if not path.exists():
        sys.exit(f"[FATAL] Manifest {path} does not exist")
    if path.suffix.lower() == ".toml":
        with path.open("rb") as fh:
            return _toml.load(fh)

    try:
        import yaml
    except ModuleNotFoundError:
        sys.exit("[FATAL] PyYAML is required to read YAML manifests.")
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ╭─────────────────────── Connection helpers ───────────────────────────╮
_SOCKET_CANDIDATES = ["/run/mysqld/mysqld.sock",
                      "/var/run/mysqld/mysqld.sock",
                      "/tmp/mysql.sock"]


def _try_socket(sock: str, user: str, pwd: str):
    try:
        return mysql.connector.connect(unix_socket=sock, user=user,
                                       password=pwd, autocommit=True)
    except mysql.connector.errors.ProgrammingError as err:
        if err.errno != 1045:
            raise
        return mysql.connector.connect(unix_socket=sock, user=user,
                                       autocommit=True)


def _connect_admin(user: str, pwd: str):
    sock = os.getenv("DB_SOCKET")
    if sock and Path(sock).exists():
        print(f"[DEBUG] socket {sock}")
        try:
            return _try_socket(sock, user, pwd)
        except mysql.connector.Error:
            pass
    for cand in _SOCKET_CANDIDATES:
        if Path(cand).exists():
            print(f"[DEBUG] socket {cand}")
            try:
                return _try_socket(cand, user, pwd)
            except mysql.connector.Error:
                pass
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", "3307"))
    print(f"[DEBUG] TCP {host}:{port}")
    return mysql.connector.connect(host=host, port=port, user=user,
                                   password=pwd, autocommit=True,
                                   use_pure=True)


# ╭─────────────────────── Generic helpers ───────────────────────────────╮
def _db_exists(cur, db: str) -> bool:
    cur.execute("SELECT 1 FROM information_schema.schemata "
                "WHERE schema_name=%s", (db,))
    return cur.fetchone() is not None


def _create_db_if_missing(cur, db: str) -> bool:
    """
    CREATE DATABASE IF NOT EXISTS … and return True if it *already* existed,
    False if it has just been created.
    """
    pre = _db_exists(cur, db)
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    # sanity check
    if not _db_exists(cur, db):
        sys.exit(f"[FATAL] Could not create database `{db}`")
    return pre


def _account_exists(cur, user: str) -> bool:
    cur.execute("SELECT 1 FROM mysql.user "
                "WHERE User=%s AND Host='%%'", (user,))
    return cur.fetchone() is not None


def _ensure_account(cur, user: str, pwd: str):
    if _account_exists(cur, user):
        cur.execute("ALTER USER %s@'%%' IDENTIFIED BY %s", (user, pwd))
    else:
        cur.execute("CREATE USER %s@'%%' IDENTIFIED BY %s", (user, pwd))


def _grant_privs(cur, user: str, db: str, priv: str):
    clause = "ALL PRIVILEGES" if priv.upper() == "ALL" else priv
    cur.execute(f"GRANT {clause} ON `{db}`.* TO %s@'%%'", (user,))


def _db_has_real_tables(cur, db: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema=%s AND table_name!='__schema_meta'", (db,))
    return cur.fetchone()[0] > 0


# ╭─────────────────────── SQL parsing helpers ───────────────────────────╮
_SINGLE = re.compile(r"^\s*(--|#|-{4,}).*$")
_DELIM  = re.compile(r"^\s*DELIMITER\s+(.+)", re.I)
_CREATE = re.compile(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?", re.I)


def _strip_comments(sql: str) -> str:
    out, in_ml = [], False
    for line in sql.splitlines():
        if in_ml:
            in_ml = "*/" not in line
            continue
        if line.lstrip().startswith("/*"):
            in_ml = "*/" not in line
            continue
        if _SINGLE.match(line):
            continue
        out.append(line)
    return "\n".join(out)


def _statements(sql: str) -> Generator[str, None, None]:
    sql = _strip_comments(sql)
    delim, buf = ";", []
    for ln in sql.splitlines(keepends=True):
        if (m := _DELIM.match(ln)):
            if buf:
                yield "".join(buf).rsplit(delim, 1)[0].strip()
                buf.clear()
            delim = m.group(1); continue
        buf.append(ln)
        if "".join(buf).rstrip().endswith(delim):
            stmt = "".join(buf).rsplit(delim, 1)[0].strip()
            buf.clear()
            if stmt:
                yield stmt
    if buf:
        yield "".join(buf).strip()


def _normalize(stmt: str) -> str:
    stmt = _strip_comments(stmt)
    stmt = re.sub(r"\bIF\s+NOT\s+EXISTS\b", "", stmt, flags=re.I)
    stmt = re.sub(r"AUTO_INCREMENT=\d+\s*", "", stmt, flags=re.I)
    stmt = re.sub(r"\s+", " ", stmt)
    return stmt.strip().lower()


def _checksum(path: Path) -> str:
    return md5(_normalize(path.read_text("utf-8")).encode()).hexdigest()


# ╭─────────────────────── Schema diff helpers ───────────────────────────╮
def _file_maps(path: Path) -> Tuple[Dict[str, str], Dict[str, str]]:
    cs, stmts = {}, {}
    for s in _statements(path.read_text("utf-8")):
        if (m := _CREATE.search(s)):
            tbl, norm = m.group(1).lower(), _normalize(s)
            cs[tbl] = md5(norm.encode()).hexdigest()
            stmts[tbl] = norm
    return cs, stmts


def _db_maps(cur, db: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    cs, stmts = {}, {}
    cur.execute("SELECT table_name FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name!='__schema_meta'", (db,))
    for (tbl,) in cur.fetchall():
        cur.execute(f"SHOW CREATE TABLE `{db}`.`{tbl}`")
        _, create_stmt = cur.fetchone()
        norm = _normalize(create_stmt)
        tbl = tbl.lower()
        cs[tbl] = md5(norm.encode()).hexdigest()
        stmts[tbl] = norm
    return cs, stmts


def _exec_fix(cur, stmt: str):
    try:
        cur.execute(stmt); return
    except mysql.connector.errors.ProgrammingError as e:
        if e.errno != 1064 or "IF NOT EXISTS" not in e.msg.upper():
            raise
        stmt = re.sub(r"\bADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\b",
                      "ADD COLUMN ", stmt, flags=re.I)
        stmt = re.sub(r"\bADD\s+IF\s+NOT\s+EXISTS\b",
                      "ADD ", stmt, flags=re.I)
        cur.execute(stmt)


def _import_schema(cur, db: str, path: Path):
    cur.execute(f"USE `{db}`")
    for s in _statements(path.read_text("utf-8")):
        _exec_fix(cur, s)


# ╭─────────────────────── meta‑table helpers ────────────────────────────╮
_META_SQL = ("CREATE TABLE IF NOT EXISTS `__schema_meta` ("
             "id TINYINT PRIMARY KEY DEFAULT 1,"
             "checksum CHAR(32) NOT NULL,"
             "file_path VARCHAR(512) NOT NULL,"
             "applied_at TIMESTAMP NOT NULL) ENGINE=InnoDB")


def _meta_get(cur, db: str) -> str | None:
    cur.execute("SELECT 1 FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name='__schema_meta'", (db,))
    if cur.fetchone() is None:
        return None
    cur.execute(f"SELECT checksum FROM `{db}`.__schema_meta WHERE id=1")
    r = cur.fetchone(); return r[0] if r else None


def _meta_set(cur, db: str, cs: str, path: str):
    cur.execute(f"USE `{db}`"); cur.execute(_META_SQL)
    cur.execute("REPLACE INTO __schema_meta VALUES (1,%s,%s,%s)",
                (cs, path, _dt.datetime.utcnow()))


# ╭────────────────────── ANSI colours for diff ──────────────────────────╮
_TTY = sys.stdout.isatty()
_RED, _GRN, _CYN, _RST = (("\033[31m", "\033[32m", "\033[36m", "\033[0m")
                          if _TTY else ("", "", "", ""))


def _col(line: str) -> str:
    if line.startswith("+") and not line.startswith("+++"):
        return _GRN + line + _RST
    if line.startswith("-") and not line.startswith("---"):
        return _RED + line + _RST
    if line.startswith("@@"):
        return _CYN + line + _RST
    return line


# ╭─────────────────────── main bootstrap ────────────────────────────────╮
def bootstrap(manifest: str):
    services = _load_manifest(Path(manifest))["services"]

    admin = os.getenv("DB_ADMIN_USER") or input("MariaDB admin user [root]: ") or "root"
    pw_admin = os.getenv("DB_ADMIN_PASSWORD") or getpass.getpass(f"Password for '{admin}': ")

    cnx = _connect_admin(admin, pw_admin)
    cur = cnx.cursor()

    for name, cfg in services.items():
        db, user, pw = cfg["database"], cfg["user"], cfg["password"]
        priv = cfg.get("privileges", "ALL")
        schema_path = (Path(os.path.expanduser(cfg.get("path", "")))
                       if "path" in cfg else None)

        print(f"[INFO] Provisioning {name}")

        # database
        pre = _create_db_if_missing(cur, db)
        print(f"       ├─ {'created' if not pre else 'DB exists – ok'} `{db}`")

        # account & privileges
        _ensure_account(cur, user, pw)
        print(f"       ├─ account {user}@'%' ensured (password set)")
        _grant_privs(cur, user, db, priv)
        print(f"       ├─ privileges `{priv}` granted")

        # schema handling
        if not schema_path or not schema_path.is_file():
            print("       └─ no schema file – skipped"); continue

        file_cs = _checksum(schema_path)

        if not _db_has_real_tables(cur, db):
            _import_schema(cur, db, schema_path)
            _meta_set(cur, db, file_cs, str(schema_path))
            print(f"       └─ imported initial schema ({schema_path.name})"); continue

        if _meta_get(cur, db) == file_cs:
            print("       └─ schema unchanged – skipped"); continue

        f_cs, f_stmt = _file_maps(schema_path)
        d_cs, d_stmt = _db_maps(cur, db)

        new = sorted(set(f_cs) - set(d_cs))
        missing = sorted(set(d_cs) - set(f_cs))
        altered = sorted(t for t in f_cs if t in d_cs and f_cs[t] != d_cs[t])

        print("       └─ schema differences:")
        if new:     print(f"           • new tables:      {', '.join(new)}")
        if missing: print(f"           • missing tables:  {', '.join(missing)}")
        if altered: print(f"           • altered tables:  {', '.join(altered)}")

        for tbl in altered:
            print(f"           ─ diff for `{tbl}`:")
            for ln in difflib.unified_diff(d_stmt[tbl].split(),
                                           f_stmt[tbl].split(),
                                           fromfile="db", tofile="file",
                                           n=2, lineterm=""):
                print("             " + _col(ln))

        if input("           Apply schema file? [y/N]: ").lower() != "y":
            print("           → skipped by user."); continue

        try:
            _import_schema(cur, db, schema_path)
            _meta_set(cur, db, file_cs, str(schema_path))
            print("           → schema applied & checksum stored.")
        except mysql.connector.Error as err:
            print(f"           ! apply failed: {err}")

    cur.close(); cnx.close(); print("[DONE]")


# ╭───────────────────────── CLI wrapper ─────────────────────────────────╮
def cli():
    p = argparse.ArgumentParser(description="Bootstrap MariaDB databases for micro‑services.")
    p.add_argument("manifest", nargs="?", help="services.yaml | services.toml")
    p.add_argument("-f", "--file", dest="manifest_opt", help="Same as positional")
    a = p.parse_args()
    manifest = (a.manifest_opt or a.manifest or
                ("services.toml" if Path("services.toml").exists() else "services.yaml"))
    bootstrap(manifest)


if __name__ == "__main__":
    cli()
