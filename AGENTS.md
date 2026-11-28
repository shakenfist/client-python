# Agents Guide

## Project Overview

This is the official Python REST API client and CLI (`sf-client`) for
[Shaken Fist](https://github.com/shakenfist/shakenfist), a minimal
cloud platform.

## Quick Start

```bash
# Install with test dependencies
pip install -e ".[test]"

# Install with VDI console support (ryll viewer)
pip install -e ".[vdi]"

# Run unit tests
stestr run

# Run flake8 on changed files
tox -eflake8

# Run all tox environments
tox

# Install the pre-commit hooks (actionlint, shellcheck, skillsaw, tox)
pre-commit install
```

## Key Files

| File | Purpose |
|------|---------|
| `shakenfist_client/apiclient.py` | REST API client library |
| `shakenfist_client/main.py` | CLI entry point, plugin loading |
| `shakenfist_client/util.py` | Shared utilities |
| `shakenfist_client/commandline/*.py` | CLI subcommands (Click-based) |
| `shakenfist_client/tests/` | Unit tests |
| `tools/flake8wrap.sh` | Flake8 wrapper for CI |
| `tools/gitleaks-scan.sh` | Credential scan of the git history, with a positive control |
| `docs/` | User facing documentation, indexed from README.md |
| `docs/plans/index.md` | Register for phased plans; add a row when starting one |
| `pyproject.toml` | Package metadata and dependencies |

## Before You Simplify Anything

Several things in this client look like tidy-up targets and are not: a
filter that looks redundant, two columns that look like they should be
one, a capability gate that raises where its neighbour stays silent.
Each has been "simplified" at least once already.

They are written down once, in
[docs/code-invariants.md](docs/code-invariants.md), with what breaks
when each one goes. Read the section that covers the area you are
about to change before changing it:

| Area | Invariants | User-facing documentation |
|------|-----------|---------------------------|
| VDI consoles (`apiclient.py`, `commandline/instance.py`) | token never reaches a bearer header, two redaction mechanisms that are not interchangeable | `docs/vdi-console.md` |
| Namespace capacity claims | field-mask body, duration not timestamp, separate state columns, 503 | `docs/namespace-claims.md` |
| Node and cluster capacity views | the binding marker copies the server's arithmetic, four separate ledger columns, `n/a` versus `-` | `docs/cluster-capacity.md` |
| Agent operation deadlines | three budgets kept apart, `is not None` rather than truthiness, one poller | `--help` on `instance execute` / `upload` / `download` |
| Agent operation dependencies | `depends_on` versus `runs_after`, a gate that raises, `await_seconds=0` as the submission idiom | docstrings on `agent_dependency()` and `await_agent_chain()` |
| Transient capacity retry | the body marker is the whole gate, two non-redundant bounds | `docs/transient-capacity-retry.md` |
| Human login and the credential cache | modes at creation, whole-token capability match, where the cache sits in precedence | `docs/human-login.md` |

## Code Conventions

- Python >= 3.7 compatibility (conservative for broad client support)
- Use `importlib.metadata` for entry points, with the
  `importlib-metadata` backport below 3.9. The API differs by version
  as well: `entry_points().select(group=...)` on 3.10 and above,
  `entry_points().get(group, [])` below it
- Single quotes for strings, double quotes for docstrings
- Max line length: 120 characters
- Trim trailing whitespace

## Testing

Unit tests use `testtools` and `stestr`. Run with:

```bash
stestr run                    # all tests
stestr run test_pattern       # specific tests
tox -epy3                     # via tox
```

## When Making Changes

- Ensure changes work with Python >= 3.7
- Run `tox -eflake8` to check style
- Run `stestr run` to verify tests pass
- Changes merge to `develop` through the merge queue, never around it:
  a release tag is only accepted on a commit whose queue run passed. A
  new `functional-tests.yml` job goes in the `needs:` of "Can enqueue"
  and "Can merge". See `docs/development.md`.
- Document user-visible changes in `docs/`, which is where a human
  benefits from them too. Update this file only when a *convention*
  changes, and `ARCHITECTURE.md` only when the *shape of the system*
  does; both are a summary and an index into `docs/`, and both are
  loaded into every session, so a change here costs context on every
  task
