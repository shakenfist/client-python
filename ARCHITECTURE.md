# Architecture

## Overview

The Shaken Fist Python client provides two interfaces to the Shaken
Fist cloud platform:

1. **Python API client** (`apiclient.py`) -- a library for
   programmatic access to the REST API
2. **CLI tool** (`sf-client`) -- a Click-based command line interface

## Module Structure

```
shakenfist_client/
├── __init__.py
├── main.py              # CLI entry point, plugin loading
├── apiclient.py         # REST API client (Client class)
├── util.py              # Shared utilities
├── commandline/         # CLI subcommands (one per resource type)
│   ├── admin.py         # Administrative commands
│   ├── ansible.py       # Ansible integration helpers
│   ├── artifact.py      # Artifact management
│   ├── backup.py        # Backup operations
│   ├── blob.py          # Blob storage
│   ├── instance.py      # VM instance management
│   ├── interface.py     # Network interface management
│   ├── label.py         # Label management
│   ├── namespace.py     # Namespace management
│   ├── network.py       # Network management
│   └── node.py          # Cluster node management
├── ansible/             # Ansible modules
│   ├── sf_instance      # Instance module
│   ├── sf_namespace     # Namespace module
│   ├── sf_network       # Network module
│   └── sf_snapshot.py   # Snapshot module
└── tests/
    ├── __init__.py
    └── test_client_apiclient.py
```

## Key Components

### Client Class (`apiclient.py`)

The `Client` class handles all communication with the Shaken Fist
REST API:

- **Authentication**: API key-based, configured from environment
  variables (`SHAKENFIST_NAMESPACE`, `SHAKENFIST_KEY`,
  `SHAKENFIST_API_URL`), `~/.shakenfist`, or
  `/etc/sf/shakenfist.json`
- **Request handling**: Wraps HTTP methods with retry logic and
  error handling
- **Async operations**: Supports configurable strategies for
  waiting on asynchronous operations (instance creation, etc.)
- **VDI console access**: `get_vdi_console_proxy` fetches a
  Kerbside-proxied console descriptor (`GET
  /instances/<ref>/vdiconsoleproxy`); `get_vdi_console_proxy_file`
  fetches the resulting `.vv` file via a plain, unauthenticated
  `requests.get()` since the capability travels as a JWT embedded in
  the URL itself; `get_vdi_token_public_keys` fetches the signing
  public keys (`GET /admin/vditokenpubkey`) used to verify those
  tokens offline. `get_vdi_console_helper` remains the direct-to-
  hypervisor fallback. `docs/vdi-console.md` describes how the CLI
  chooses between the two paths.

### CLI (`main.py`)

Built with [Click](https://click.palletsprojects.com/):

- Entry point registered as `sf-client` console script
- Subcommands organised by resource type in `commandline/`
- Plugin system: third-party packages can extend the CLI by
  registering entry points in the `shakenfist_client.plugin` group
- Plugin loading uses `importlib.metadata` (with
  `importlib-metadata` backport for Python < 3.9)

### Ansible Modules (`ansible/`)

Native Ansible modules for managing Shaken Fist resources directly
from Ansible playbooks.

## Python Version Compatibility

The client targets Python >= 3.7, intentionally more conservative
than the server. This supports the widest range of client platforms,
including enterprise Linux distributions that ship older Python
versions.

When using newer Python features, conditional imports or backport
packages are required. See `main.py` lines 7-10 for an example
with `importlib.metadata`.

## Build and Packaging

- **Build system**: `setuptools` with `pyproject.toml`
- **Versioning**: `setuptools_scm` derives version from git tags
- **Distribution**: Published to PyPI as `shakenfist_client`
- **Optional extras**: `[vdi]` pulls in
  [ryll](https://github.com/shakenfist/ryll) on Linux, the preferred
  VDI viewer for `instance vdiconsole` / `vdiconsolefile`; without it
  the CLI falls back to `remote-viewer` if present on `PATH`

## CI/CD

- **Code formatting**: Daily automated formatting via
  `code-formatting.yml`
- **Functional tests**: Matrix-based CI running against live Shaken
  Fist clusters via `functional-tests.yml`
- **Linting**: flake8 via `tools/flake8wrap.sh`, max line length 120
