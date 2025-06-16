import re
from hashlib import md5
from pathlib import Path
from typing import Dict, Generator, List, Tuple

_DELIM_RE = re.compile(r"^\s*DELIMITER\s+(.+)", re.I)
_CREATE_RE = re.compile(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?", re.I)

def statements(sql: str) -> Generator[str, None, None]:
    delim = ";"
    buf: list[str] = []
    for line in sql.splitlines(keepends=True):
        m = _DELIM_RE.match(line)
        if m:
            if buf:
                yield "".join(buf).rsplit(delim, 1)[0].strip()
                buf.clear()
            delim = m.group(1)
            continue
        buf.append(line)
        if "".join(buf).rstrip().endswith(delim):
            stmt = "".join(buf).rsplit(delim, 1)[0].strip()
            if stmt:
                yield stmt
            buf.clear()
    if buf:
        yield "".join(buf).strip()

def normalize(stmt: str) -> str:
    stmt = re.sub(r"\bIF\s+NOT\s+EXISTS\b", "", stmt, flags=re.I)
    stmt = re.sub(r"AUTO_INCREMENT=\d+\s*", "", stmt, flags=re.I)
    stmt = re.sub(r"\s+", " ", stmt)
    return stmt.strip().lower()

def file_schema_map(sql_path: Path) -> Dict[str, str]:
    with sql_path.open("r", encoding="utf-8") as f:
        script = f.read()
    mapping: Dict[str, str] = {}
    for stmt in statements(script):
        m = _CREATE_RE.search(stmt)
        if m:
            mapping[m.group(1).lower()] = md5(normalize(stmt).encode()).hexdigest()
    return mapping

def db_schema_map(cur, db) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s", (db,))
    for (tbl,) in cur.fetchall():
        cur.execute(f"SHOW CREATE TABLE `{db}`.`{tbl}`")
        _, create_stmt = cur.fetchone()
        mapping[tbl.lower()] = md5(normalize(create_stmt).encode()).hexdigest()
    return mapping

def schema_diff(cur, db, sql_path: Path) -> Tuple[List[str], List[str], List[str]]:
    file_map = file_schema_map(sql_path)
    db_map = db_schema_map(cur, db)
    new = sorted(set(file_map) - set(db_map))
    missing = sorted(set(db_map) - set(file_map))
    altered = sorted(t for t in file_map if t in db_map and file_map[t] != db_map[t])
    return new, missing, altered

def import_schema(cur, db, sql_path: Path):
    with sql_path.open("r", encoding="utf-8") as f:
        script = f.read()
    cur.execute(f"USE `{db}`")
    for stmt in statements(script):
        cur.execute(stmt)

def compute_schema_file_hash(sql_path: Path) -> str:
    with sql_path.open("rb") as f:
        return md5(f.read()).hexdigest()

def print_schema_diff_box(new, missing, altered):
    lines = []
    if new:
        lines.append("│  [+] New tables in file:    " + ", ".join(new))
    if missing:
        lines.append("│  [-] Tables missing in file:" + ", ".join(missing))
    if altered:
        lines.append("│  [*] Altered definitions:   " + ", ".join(altered))

    maxlen = max((len(line) for line in lines), default=0)
    border = "┌" + "─" * (maxlen - 1) + "┐"
    bottom = "└" + "─" * (maxlen - 1) + "┘"
    print("       " + border)
    for line in lines:
        print(f"{line}{' ' * (maxlen - len(line))}│")
    print("       " + bottom)
