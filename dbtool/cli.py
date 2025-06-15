#!/usr/bin/env python3
"""
dbtool CLI – multi‑service‑aware with optional custom *path* per service.

Layout reference
────────────────
* default layout      → services/<service>/db/{migrations,schema}
* custom layout via   → path: <dir>    (in dbtool.bootstrap.yml)
"""
from __future__ import annotations

import pathlib
import sys
import typing as t

import click
import yaml

from dbtool import __version__
from dbtool.bootstrap import bootstrap
from dbtool.config import Environment, load, ConfigError
from dbtool.generate import generate
from dbtool.migrations.runner import MigrationRunner
from dbtool.schema.apply import apply_schema
from dbtool.schema.diff import diff as schema_diff

# --------------------------------------------------------------------------- #
#  Constants / helpers                                                        #
# --------------------------------------------------------------------------- #

PROJECT_ROOT = pathlib.Path.cwd()
DEFAULT_BOOTSTRAP_FILE = PROJECT_ROOT / "dbtool.bootstrap.yml"


def _load_yaml(path: pathlib.Path) -> dict:
    return yaml.safe_load(path.read_text()) if path.exists() else {}


def _service_meta(service: str, bootstrap_file: pathlib.Path) -> dict[str, t.Any]:
    data = _load_yaml(bootstrap_file)
    try:
        return data["services"][service]
    except KeyError:
        click.echo(
            f"Service {service!r} not found in {bootstrap_file}", err=True
        )
        sys.exit(1)


def _load_env(ctx, _param, value) -> Environment:
    """Server‑level (host/port/root) environment."""
    try:
        return load(ctx.obj["config_path"], value)
    except ConfigError as exc:
        click.echo(f"Config error: {exc}", err=True)
        sys.exit(1)


def _build_service_env(
    base_env: Environment,
    service: str | None,
    bootstrap_file: pathlib.Path = DEFAULT_BOOTSTRAP_FILE,
) -> Environment:
    """Merge server creds with service DB/user creds."""
    if service is None:
        return base_env

    meta = _service_meta(service, bootstrap_file)
    merged: dict[str, t.Any] = {
        "host": base_env.host,
        "port": base_env.port,
        "database": meta["database"],
        "user": meta["user"],
        "password": meta["password"],
        "allow_destructive": base_env.allow_destructive,
    }
    return Environment(service, merged)


def _service_dir(service: str | None, bootstrap_file: pathlib.Path = DEFAULT_BOOTSTRAP_FILE) -> pathlib.Path:
    """
    Return service root directory (may be overridden by *path:*).
    """
    if service is None:
        root = PROJECT_ROOT
    else:
        meta = _service_meta(service, bootstrap_file)
        if "path" in meta:
            root = PROJECT_ROOT / meta["path"] if not pathlib.Path(meta["path"]).is_absolute() else pathlib.Path(meta["path"])
        else:
            root = PROJECT_ROOT / "services" / service

    # ensure sub‑folders exist
    (root / "db" / "migrations").mkdir(parents=True, exist_ok=True)
    (root / "db" / "schema").mkdir(parents=True, exist_ok=True)
    return root


# --------------------------------------------------------------------------- #
#  CLI root                                                                   #
# --------------------------------------------------------------------------- #

@click.group()
@click.option(
    "-c", "--config", "config_path",
    type=click.Path(),
    default=None,
    help="Path to env‑config YAML",
)
@click.pass_context
def main(ctx, config_path):
    """dbtool – hybrid migration & DDL manager for MariaDB (multi‑service‑ready)."""
    ctx.obj = {"config_path": pathlib.Path(config_path) if config_path else None}


# --------------------------------------------------------------------------- #
#  Info                                                                       #
# --------------------------------------------------------------------------- #

@main.command()
def version():
    """Print dbtool version."""
    click.echo(__version__)


# --------------------------------------------------------------------------- #
#  Bootstrap                                                                  #
# --------------------------------------------------------------------------- #

@main.command("bootstrap")
@click.option("-e", "--env", callback=_load_env, expose_value=True)
@click.option(
    "-f", "--file", "bootstrap_file",
    type=click.Path(exists=True, dir_okay=False),
    default=str(DEFAULT_BOOTSTRAP_FILE),
    help="YAML file describing service DB profiles (defaults to dbtool.bootstrap.yml).",
)
@click.option("--dry-run", is_flag=True, help="Show SQL only.")
@click.pass_context
def _bootstrap(ctx, env, bootstrap_file, dry_run):
    """Create databases/users/GRANTs for one or many micro‑services."""
    bootstrap(env, pathlib.Path(bootstrap_file), dry_run=dry_run)


# --------------------------------------------------------------------------- #
#  Status                                                                     #
# --------------------------------------------------------------------------- #

@main.command()
@click.option("-e", "--env",  callback=_load_env, is_eager=True, expose_value=True)
@click.option("-s", "--service", help="Micro‑service name.")
@click.option(
    "-f", "--file", "bootstrap_file",
    type=click.Path(),
    default=str(DEFAULT_BOOTSTRAP_FILE),
    help="Bootstrap file (only needed when using --service).",
)
@click.pass_context
def status(ctx, env, service, bootstrap_file):
    """Show migration status."""
    env = _build_service_env(env, service, pathlib.Path(bootstrap_file))
    srv_dir = _service_dir(service, pathlib.Path(bootstrap_file))

    runner = MigrationRunner(env, srv_dir)
    st = runner.status()
    click.echo(
        f"Applied: {len(st['applied'])} | Pending: {len(st['pending'])} "
        f"| Checksum mismatches: {len(st['mismatch'])}"
    )
    for p in st["pending"]:
        click.echo(f"  PENDING  {p.path.name}")
    for mf, dbsum in st["mismatch"]:
        click.echo(f"  MISMATCH {mf.path.name} (db:{dbsum[:8]}  file:{mf.checksum[:8]})")


# --------------------------------------------------------------------------- #
#  Migrate                                                                    #
# --------------------------------------------------------------------------- #

@main.command()
@click.option("-e", "--env",  callback=_load_env, expose_value=True)
@click.option("-s", "--service", help="Micro‑service name.")
@click.option(
    "-f", "--file", "bootstrap_file",
    type=click.Path(),
    default=str(DEFAULT_BOOTSTRAP_FILE),
    help="Bootstrap file (only needed when using --service).",
)
@click.option("--dry-run", is_flag=True)
@click.option("--allow-destructive", is_flag=True)
@click.pass_context
def migrate(ctx, env, service, bootstrap_file, dry_run, allow_destructive):
    """Apply pending incremental migrations."""
    env = _build_service_env(env, service, pathlib.Path(bootstrap_file))
    srv_dir = _service_dir(service, pathlib.Path(bootstrap_file))

    MigrationRunner(env, srv_dir).migrate(
        dry_run=dry_run,
        allow_destructive=allow_destructive,
    )


# --------------------------------------------------------------------------- #
#  Declarative schema                                                         #
# --------------------------------------------------------------------------- #

@main.command("schema-diff")
@click.option("-e", "--env",  callback=_load_env, expose_value=True)
@click.option("-s", "--service", help="Micro‑service name.")
@click.option(
    "-f", "--file", "bootstrap_file",
    type=click.Path(),
    default=str(DEFAULT_BOOTSTRAP_FILE),
)
@click.option("--allow-destructive", is_flag=True)
@click.pass_context
def _schema_diff(ctx, env, service, bootstrap_file, allow_destructive):
    """Show SQL required so DB matches schema directory on disk."""
    env = _build_service_env(env, service, pathlib.Path(bootstrap_file))
    srv_dir = _service_dir(service, pathlib.Path(bootstrap_file))
    schema_dir = srv_dir / "db" / "schema"

    stmts = schema_diff(env, schema_dir, allow_destructive=allow_destructive)
    click.echo("\n".join(stmts) if stmts else "No differences.")


@main.command("schema-apply")
@click.option("-e", "--env",  callback=_load_env, expose_value=True)
@click.option("-s", "--service", help="Micro‑service name.")
@click.option(
    "-f", "--file", "bootstrap_file",
    type=click.Path(),
    default=str(DEFAULT_BOOTSTRAP_FILE),
)
@click.option("--auto-approve", is_flag=True)
@click.option("--allow-destructive", is_flag=True)
@click.option("--dry-run", is_flag=True)
@click.pass_context
def _schema_apply(ctx, env, service, bootstrap_file, auto_approve, allow_destructive, dry_run):
    """Apply declarative schema directory."""
    env = _build_service_env(env, service, pathlib.Path(bootstrap_file))
    srv_dir = _service_dir(service, pathlib.Path(bootstrap_file))

    apply_schema(
        env,
        srv_dir,
        auto_approve=auto_approve,
        allow_destructive=allow_destructive,
        dry_run=dry_run,
    )


# --------------------------------------------------------------------------- #
#  Generate                                                                   #
# --------------------------------------------------------------------------- #

@main.command("generate")
@click.option("-e", "--env",  callback=_load_env, expose_value=True)
@click.option("-s", "--service", help="Micro‑service name.")
@click.option(
    "-f", "--file", "bootstrap_file",
    type=click.Path(),
    default=str(DEFAULT_BOOTSTRAP_FILE),
)
@click.option("-m", "--message", required=True)
@click.option("--allow-destructive", is_flag=True)
@click.pass_context
def generate_migration(ctx, env, service, bootstrap_file, message, allow_destructive):
    """Create a new migration from schema diff."""
    env = _build_service_env(env, service, pathlib.Path(bootstrap_file))
    srv_dir = _service_dir(service, pathlib.Path(bootstrap_file))

    path = generate(
        env,
        srv_dir,
        description=message,
        allow_destructive=allow_destructive,
    )
    if path:
        click.echo(f"Created {path.relative_to(srv_dir)}")
    else:
        click.echo("Database already matches schema – nothing to generate.")
