# Agents Guide

## Project Overview

This is the official Python REST API client and CLI (`sf-client`) for
[Shaken Fist](https://github.com/shakenfist/shakenfist), a minimal
cloud platform.

## Quick Start

```bash
# Install with test dependencies
pip install -e ".[test]"

# Run unit tests
stestr run

# Run flake8 on changed files
tox -eflake8

# Run all tox environments
tox
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
| `pyproject.toml` | Package metadata and dependencies |

## Code Conventions

- Python >= 3.7 compatibility (conservative for broad client support)
- Use `importlib.metadata` for entry points, with backport for < 3.9
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
- Update CLAUDE.md if architecture changes significantly
