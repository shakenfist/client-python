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
| `tools/gitleaks-scan.sh` | Credential scan of the git history, with a positive control |
| `docs/` | User facing documentation, indexed from README.md |
| `pyproject.toml` | Package metadata and dependencies |

## VDI Console Access

What the commands do, and which combinations of path and viewer write
the console token to disk, is in `docs/vdi-console.md`. Two things
about the code are not derivable from reading it:

- `get_vdi_console_proxy_file` in `shakenfist_client/apiclient.py`
  fetches the `.vv` body with a plain `requests.get()` and must NOT
  attach an SF bearer token. The capability is already embedded as a
  JWT in the URL, and sending the bearer token as well would put a
  long-lived credential where a single-use one belongs.
- `get_vdi_token_public_keys` (`GET /admin/vditokenpubkey`) has no CLI
  caller. It exists for Kerbside's source driver, so "unused" is not a
  reason to remove it.
- A proxy console URL is a credential, and more than one thing logs
  it: `_request_url()` logs the response body it arrives in, and
  urllib3 logs the request target it is used as. `main.py` redacts JWTs
  at the logging handlers rather than at either call site, so a new log
  line that happens to print one is covered by default. Do not remove
  that filter to make a record easier to read.

The launch logic and viewer-selection chain live in
`shakenfist_client/commandline/instance.py`.

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
