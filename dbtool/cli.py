#!/usr/bin/env python3
"""
dbtool – multi‑service‑aware CLI.

• Default layout:        services/<service>/db/{migrations,schema}/  
• Monolithic schema:     services/<service>/db/schema.sql  
• Custom root directory: declare `path:` in dbtool.bootstrap.yml

The tool now gracefully falls back to the project‑root *db/* directory when
the requested service directory does not yet exist but a valid *db/schema*
(or *db/schema.sql*) is present.  This makes first‑run onboarding painless.
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

PROJECT_ROOT = pathlib.Path.cwd()
DEFAULT_BOOTSTRAP_FILE = PROJECT_ROOT / "dbtool.bootstrap.yml"


def _load_yaml(path: pathlib.Path) -> dict:
    return yaml.safe_load(path.read_text()) if path.exists() else {}


def _service_meta(service: str, bootstrap_file: pathlib.Path) -> dict[str, t.Any]:
    data = _load_yaml(bootstrap_file)
    try:
        return data["services"][service]
    except KeyError:
        click.echo(f"Service {service!r} not found in {bootstrap_file}", err=True)
        sys.exit(1)


def _load_env(ctx, _param, value) -> Environment:
    try:
        return load(ctx.obj["config_path"], value)
    except ConfigError as exc:
        click.echo(f"Config error: {exc}", err=True)
        sys.exit(1)


def _build_service_env(
    base: Environment,
    service: str | None,
    bootstrap_file: pathlib.Path,
) -> Environment:
    if service is None:
        return base
    meta = _service_meta(service, bootstrap_file)
    merged = {
        "host": base.host,
        "port": base.port,
        "database": meta["database"],
        "user": meta["user"],
        "password": meta["password"],
        "allow_destructive": base.allow_destructive,
    }
    return Environment(service, merged)


def _resolve_service_root(
    service: str | None,
    bootstrap_file: pathlib.Path,
) -> pathlib.Path:
    """
    Determine the root directory that holds *all* DB artefacts for *service*.

    Fallback logic:

    1. `path:` (if provided in bootstrap)          → absolute / relative dir
    2. services/<service>/                         → default convention
    3. project‑root *db/* directories *when* they already exist
       (facilitates adopting dbtool in an existing mono‑repo)
    """
    if service is None:
        return PROJECT_ROOT

    meta = _service_meta(service, bootstrap_file)

    if "path" in meta:
        root = pathlib.Path(meta["path"]).expanduser()
        if root.is_file():
            click.echo(f"`path:` for {service!r} must be a directory, got a file.", err=True)
            sys.exit(1)
        if not root.is_absolute():
            root = PROJECT_ROOT / root
        root.mkdir(parents=True, exist_ok=True)
        return root

    conv_root = PROJECT_ROOT / "services" / service
    if conv_root.exists():
        return conv_root

    legacy_db = PROJECT_ROOT / "db"
    if (legacy_db / "schema").exists() or (legacy_db / "schema.sql").exists():
        return PROJECT_ROOT

    conv_root.mkdir(parents=True, exist_ok=True)
    return conv_root


def _ensure_subdirs(root: pathlib.Path) -> None:
    (root / "db" / "migrations").mkdir(parents=True, exist_ok=True)
    schema_sql = root / "db" / "schema.sql"
    if not schema_sql.exists():
        (root / "db" / "schema").mkdir(parents=True, exist_ok=True)


@click.group()
@click.option(
    "-c", "--config", "config_path", type=click.Path(), help="env config YAML"
)
@click.pass_context
def main(ctx, config_path):
    ctx.obj = {"config_path": pathlib.Path(config_path) if config_path else None}


@main.command()
def version():
    click.echo(__version__)


@main.command("bootstrap")
@click.option("-e", "--env", callback=_load_env, expose_value=True)
@click.option(
    "-f", "--file", "bootstrap_file",
    type=click.Path(exists=True, dir_okay=False),
    default=str(DEFAULT_BOOTSTRAP_FILE),
)
@click.option("--dry-run", is_flag=True)
def bootstrap_cmd(env, bootstrap_file, dry_run):
    bootstrap(env, pathlib.Path(bootstrap_file), dry_run=dry_run)


def _common_opts(fn):
    opts = [
        click.option("-e", "--env", callback=_load_env, expose_value=True),
        click.option("-s", "--service"),
        click.option(
            "-f", "--file", "bootstrap_file",
            type=click.Path(), default=str(DEFAULT_BOOTSTRAP_FILE)
        ),
    ]
    for opt in reversed(opts):
        fn = opt(fn)
    return fn


@main.command()
@_common_opts
def status(env, service, bootstrap_file):
    root = _resolve_service_root(service, pathlib.Path(bootstrap_file))
    _ensure_subdirs(root)
    runner = MigrationRunner(_build_service_env(env, service, pathlib.Path(bootstrap_file)), root)
    st = runner.status()
    click.echo(f"Applied {len(st['applied'])}  |  Pending {len(st['pending'])}")
    for mf in st["pending"]:
        click.echo(f"  PENDING  {mf.path.name}")


@main.command()
@_common_opts
@click.option("--dry-run", is_flag=True)
@click.option("--allow-destructive", is_flag=True)
def migrate(env, service, bootstrap_file, dry_run, allow_destructive):
    root = _resolve_service_root(service, pathlib.Path(bootstrap_file))
    _ensure_subdirs(root)
    MigrationRunner(_build_service_env(env, service, pathlib.Path(bootstrap_file)), root).migrate(
        dry_run=dry_run,
        allow_destructive=allow_destructive,
    )


@main.command("schema-diff")
@_common_opts
@click.option("--allow-destructive", is_flag=True)
def schema_diff_cmd(env, service, bootstrap_file, allow_destructive):
    root = _resolve_service_root(service, pathlib.Path(bootstrap_file))
    _ensure_subdirs(root)
    schema_root = root / "db" / "schema"
    stmts = schema_diff(
        _build_service_env(env, service, pathlib.Path(bootstrap_file)),
        schema_root,
        allow_destructive=allow_destructive,
    )
    click.echo("\n".join(stmts) if stmts else "No differences.")


@main.command("schema-apply")
@_common_opts
@click.option("--auto-approve", is_flag=True)
@click.option("--allow-destructive", is_flag=True)
@click.option("--dry-run", is_flag=True)
def schema_apply_cmd(env, service, bootstrap_file, auto_approve, allow_destructive, dry_run):
    root = _resolve_service_root(service, pathlib.Path(bootstrap_file))
    _ensure_subdirs(root)
    apply_schema(
        _build_service_env(env, service, pathlib.Path(bootstrap_file)),
        root,
        auto_approve=auto_approve,
        allow_destructive=allow_destructive,
        dry_run=dry_run,
    )


@main.command("generate")
@_common_opts
@click.option("-m", "--message", required=True)
@click.option("--allow-destructive", is_flag=True)
def generate_cmd(env, service, bootstrap_file, message, allow_destructive):
    root = _resolve_service_root(service, pathlib.Path(bootstrap_file))
    _ensure_subdirs(root)
    path = generate(
        _build_service_env(env, service, pathlib.Path(bootstrap_file)),
        root,
        description=message,
        allow_destructive=allow_destructive,
    )
    if path:
        click.echo(f"Created {path.relative_to(root)}")
    else:
        click.echo("Database already matches schema – nothing to generate.")
