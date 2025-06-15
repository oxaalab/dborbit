from __future__ import annotations
import os
import pathlib
import typing as t
import yaml

_DEFAULT_PATH = pathlib.Path("dbtool.config.yml")


class ConfigError(RuntimeError):
    """Raised for any user‑visible configuration problem."""


class Environment:
    """
    A thin value‑object holding the attributes required to open a MariaDB
    connection.  Nothing here talks to the database.
    """

    def __init__(self, name: str, d: dict[str, t.Any]) -> None:
        self.name: str = name
        self.host: str = d["host"]
        self.port: int = d.get("port", 3306)          # <‑‑ default back to 3306
        self.database: str = d["database"]
        self.user: str = d["user"]

        # Allow `${ENV_VAR}` syntax for secrets
        raw_pwd: str = str(d["password"])
        self.password: str = (
            os.getenv(raw_pwd[2:-1]) if raw_pwd.startswith("${") else raw_pwd
        )

        # Used by ‘DROP TABLE’ guard‑rails and the auto‑create‑db logic
        self.allow_destructive: bool = d.get("allow_destructive", False)

    # --------------------------------------------------------------------- #
    # Helpers
    # --------------------------------------------------------------------- #
    def dsn(self) -> dict[str, t.Any]:
        """Return kwargs that mysql‑connector understands."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }


def load(path: pathlib.Path | str | None = None, env: str | None = None) -> Environment:
    """
    Parse *path* (or the default YAML) and return an :class:`Environment`.
    """
    cfg_file = pathlib.Path(path) if path else _DEFAULT_PATH
    if not cfg_file.exists():
        raise ConfigError(f"Config file {cfg_file} not found.  Run `dbtool init`.")

    with cfg_file.open() as fh:
        raw = yaml.safe_load(fh) or {}

    env_name = env or raw.get("default_env")
    if not env_name:
        raise ConfigError("No environment specified and no default_env in config")

    try:
        return Environment(env_name, raw["environments"][env_name])
    except KeyError as exc:
        raise ConfigError(f"Environment {env_name!r} not found in config") from exc
