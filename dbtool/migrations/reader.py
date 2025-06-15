from __future__ import annotations
import re
import pathlib
from dbtool.history import calculate_checksum

_MIGR_RE = re.compile(r"^(V|R)([0-9A-Za-z_.-]*?)__(.+?)\.sql$", re.IGNORECASE)


class MigrationFile:
    """Representation of one migration SQL file on disk."""

    def __init__(self, path: pathlib.Path) -> None:
        m = _MIGR_RE.match(path.name)
        if not m:
            raise ValueError(f"Invalid migration filename: {path}")
        self.prefix, self.version, self.description = m.groups()
        self.repeatable: bool = self.prefix.upper() == "R"
        self.path: pathlib.Path = path
        self.sql: str = path.read_text(encoding="utf-8")
        self.checksum: str = calculate_checksum(self.sql)

    def ordering_key(self) -> tuple[int, str]:
        return (1 if self.repeatable else 0, self.version)


def discover(migrations_dir: pathlib.Path) -> list[MigrationFile]:
    """
    Return **sorted** list of ``MigrationFile`` objects found in *migrations_dir*.

    If the directory does not exist yet (fresh repository) we treat that
    as “no migrations” rather than raising *FileNotFoundError*.
    """
    if not migrations_dir.exists():
        return []

    items: list[MigrationFile] = []
    for p in migrations_dir.iterdir():
        if p.suffix.lower() == ".sql" and _MIGR_RE.match(p.name):
            items.append(MigrationFile(p))

    return sorted(items, key=lambda m: m.ordering_key())
