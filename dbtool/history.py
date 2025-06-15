from __future__ import annotations
import hashlib
import mysql.connector
from dbtool.constants import SCHEMA_HISTORY_TABLE


DDL_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_HISTORY_TABLE} (
    installed_rank INT AUTO_INCREMENT PRIMARY KEY,
    version        VARCHAR(50),
    description    VARCHAR(200),
    type           VARCHAR(20),
    script         VARCHAR(255),
    checksum       VARCHAR(64),
    installed_by   VARCHAR(100),
    installed_on   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_time INT,
    success        TINYINT
) ENGINE=InnoDB;
"""


def ensure_history_table(cur: mysql.connector.cursor_cext.CMySQLCursor) -> None:
    """Create the schema‑history table if it does not yet exist."""
    cur.execute(DDL_CREATE_TABLE)


def calculate_checksum(sql_text: str) -> str:
    """Return a SHA‑256 hex digest of the given SQL text."""
    return hashlib.sha256(sql_text.encode("utf-8")).hexdigest()


def record_success(
    cur,
    version: str | None,
    desc: str,
    typ: str,
    script: str,
    checksum: str,
    exec_ms: int,
    user: str = "dbtool",
) -> None:
    """Insert a success row into the history table."""
    cur.execute(
        f"""INSERT INTO {SCHEMA_HISTORY_TABLE}
           (version, description, type, script, checksum,
            installed_by, execution_time, success)
           VALUES (%s, %s, %s, %s, %s, %s, %s, 1)""",
        (version, desc, typ, script, checksum, user, exec_ms),
    )
