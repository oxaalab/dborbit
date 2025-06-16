import mysql.connector

def connect_admin(user: str, pwd: str):
    return mysql.connector.connect(
        host="127.0.0.1", port=3307, user=user, password=pwd, autocommit=True, use_pure=True
    )

def db_exists(cur, db):
    cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name=%s", (db,))
    return cur.fetchone() is not None

def db_has_tables(cur, db):
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s", (db,))
    (cnt,) = cur.fetchone()
    return cnt > 0

def user_exists(cur, user):
    cur.execute("SELECT 1 FROM mysql.user WHERE User=%s", (user,))
    return cur.fetchone() is not None

def priv_granted(cur, user, db, priv):
    import re
    cur.execute("SHOW GRANTS FOR %s@'%%'", (user,))
    grants = " ".join(x[0] for x in cur.fetchall())
    if "ALL PRIVILEGES" in grants and f"ON `{db}`.*" in grants:
        return True
    requested = {p.strip().upper() for p in priv.split(",")}
    return requested.issubset(set(re.findall(r"GRANT (.+?) ON", grants, re.I)))

def create_database(cur, db):
    cur.execute(f"CREATE DATABASE `{db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")

def create_user(cur, user, pwd):
    cur.execute("CREATE USER %s@'%%' IDENTIFIED BY %s", (user, pwd))

def grant(cur, user, db, priv):
    clause = "ALL PRIVILEGES" if priv.upper() == "ALL" else priv
    cur.execute(f"GRANT {clause} ON `{db}`.* TO %s@'%%'", (user,))
