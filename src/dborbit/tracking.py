def ensure_tracking_table(cur, db):
    cur.execute(f"USE `{db}`")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS _dborbit_schema_applied (
            id INT AUTO_INCREMENT PRIMARY KEY,
            schema_path VARCHAR(512) NOT NULL,
            schema_hash CHAR(32) NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY (schema_path)
        )
    """)

def get_applied_schema_hash(cur, db, schema_path):
    ensure_tracking_table(cur, db)
    cur.execute(
        "SELECT schema_hash FROM _dborbit_schema_applied WHERE schema_path=%s",
        (str(schema_path),)
    )
    res = cur.fetchone()
    return res[0] if res else None

def set_applied_schema_hash(cur, db, schema_path, schema_hash):
    ensure_tracking_table(cur, db)
    cur.execute(
        """
        INSERT INTO _dborbit_schema_applied (schema_path, schema_hash)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE schema_hash=VALUES(schema_hash), applied_at=CURRENT_TIMESTAMP
        """,
        (str(schema_path), schema_hash)
    )
