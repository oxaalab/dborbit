import sys
from pathlib import Path
from typing import Dict

try:
    import tomllib as _toml
except ModuleNotFoundError:
    import tomli as _toml

def load_toml(p: Path) -> Dict:
    with p.open("rb") as f:
        return _toml.load(f)

def load_yaml(p: Path) -> Dict:
    try:
        import yaml
    except ModuleNotFoundError:
        sys.exit("[FATAL] PyYAML required but missing.")
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_manifest(p: Path) -> Dict:
    if not p.exists():
        sys.exit(f"[FATAL] manifest {p} does not exist")
    return (load_toml if p.suffix == ".toml" else load_yaml)(p)
