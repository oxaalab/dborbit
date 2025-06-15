# dborbit

## Road‑map

| Milestone | Contents                                     |
|-----------|----------------------------------------------|
| v0.1      | MVP – CLI (`init`, `migrate`, `status`, `schema-diff`) |
| v0.2      | `generate`, `rollback`, checksum repair, unit‑tests |
| v0.3      | Declarative `schema apply` & lint rules      |
| v0.4      | macOS GUI (PyQt) beta                        |
| v1.0      | Production GA, docs site, Homebrew formula   |

### Contributing

* Fork → feature branch → PR  
* Run `pre-commit run --all-files` before pushing.  
* PRs must keep `pytest` and `ruff` passing (`make test`).  
* New features need an issue + design brief.
