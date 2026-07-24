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

## VDI Console Access

`shakenfist_client/commandline/instance.py` hosts the `vdiconsole` and
`vdiconsolefile` commands, including the proxy-vs-direct launch logic
and the viewer-selection chain (prefers `ryll` on `PATH`, then falls
back to `remote-viewer`; proxy connections via `ryll` launch with
`--url` and never write the token to disk, all other combinations
write a temporary `.vv` file). `shakenfist_client/apiclient.py` has the
matching `Client` methods: `get_vdi_console_proxy` (`GET
/instances/<ref>/vdiconsoleproxy`), `get_vdi_console_proxy_file`
(fetches the `.vv` body from the URL returned above via a plain
`requests.get()` -- it must NOT attach an SF bearer token, since the
capability is already embedded as a JWT in the URL), and
`get_vdi_token_public_keys` (`GET /admin/vditokenpubkey`).

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
