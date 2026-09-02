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
| `docs/plans/index.md` | Register for phased plans; add a row when starting one |
| `pyproject.toml` | Package metadata and dependencies |

## VDI Console Access

What the commands do, and which combinations of path and viewer write
the console token to disk, is in `docs/vdi-console.md`. Three things
about the code are not derivable from reading it:

- `get_vdi_console_proxy_file` in `shakenfist_client/apiclient.py`
  fetches the `.vv` body with a plain `requests.get()` and must NOT
  attach an SF bearer token. The capability is already embedded as a
  JWT in the URL, and sending the bearer token as well would put a
  long-lived credential where a single-use one belongs.
- `get_vdi_token_public_keys` (`GET /admin/vditokenpubkey`) has no CLI
  caller. It exists for Kerbside's source driver, so "unused" is not a
  reason to remove it.
- A proxy console URL is a credential, and rendering it anywhere is
  how that credential escapes. Two mechanisms cover the known routes,
  and they are not interchangeable. `apiclient.redact_tokens()` is
  applied where `get_vdi_console_proxy_file()` raises, because requests
  builds its exception messages out of the URL and an uncaught one is
  printed as a traceback that no logging filter ever sees. `main.py`
  installs the same function as a logging filter, which covers what
  libraries log -- `_request_url()`'s response bodies, urllib3's
  request targets. A new way to render that URL needs one of the two
  applied to it; neither is automatic.

The launch logic and viewer-selection chain live in
`shakenfist_client/commandline/instance.py`.

## Namespace Capacity Claims

What the commands do, the two claim states and what each refusal
status means is in `docs/namespace-claims.md`. Two invariants in the
code look like tidy-up targets and are not:

- `update_namespace_claim()` filters `None` arguments out of the body
  on purpose. The server reads the body as a field mask, so sending
  every dimension -- read back from the claim or otherwise -- turns a
  re-date into a resize and races concurrent writers. Do not "simplify"
  the filtering away.
- The CLI keeps `state` and `coverage_state` as separate columns. They
  answer different questions, and an expired claim (`created` /
  `expired`) is the one an operator has to act on, so collapsing them
  into a single status hides it.

## Agent Operation Deadlines

`deadline_seconds` and `progress_timeout_seconds` bound an in-guest
agent operation on the *server*. Four invariants here read as tidy-up
targets and are not:

- `TERMINAL_AGENT_OPERATION_STATES` hand-duplicates
  `AgentOperation.TERMINAL_STATES` in the server repository
  (`shakenfist/operations/agentoperation.py`). The client cannot import
  it, so a state added there has to be added here too.
- Nothing derives a deadline from the async strategy. The strategy says
  how long this client will block; the deadline says how long the
  operation may live. Deriving one from the other gave every CLI
  invocation a 60 second server side kill, because `pause` is the CLI
  default.
- The timing values are tested with `is not None`, never truthiness. The
  server reads `0` as "no such budget at all", which is a different
  answer from omitting the key, and `--deadline 0` has to reach the wire.
- `_add_agentop_timing()` is called by the three creating helpers rather
  than by `_await_agentop()`, because by the time that is polling the
  POST which created the operation has already gone out.

The capability gate (`agentoperation-deadlines`) fails closed: against a
server which does not advertise it the client sends nothing. The CLI
warns only when the user typed a flag, because an omitted flag already
means "the server default".

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
