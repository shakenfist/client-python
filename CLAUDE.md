# Shaken Fist Python Client

## Project Overview

This is the official Python REST API client and command line interface for
[Shaken Fist](https://github.com/shakenfist/shakenfist), a minimal cloud
platform. The package provides both a programmatic API (`apiclient.py`) and a
CLI tool (`sf-client`).

## Architecture

```
shakenfist_client/
├── main.py              # CLI entry point, plugin loading
├── apiclient.py         # REST API client library
├── util.py              # Shared utilities
└── commandline/         # CLI subcommands
    ├── admin.py         # Administrative commands
    ├── ansible.py       # Ansible integration
    ├── artifact.py      # Artifact management
    ├── backup.py        # Backup operations
    ├── blob.py          # Blob storage
    ├── instance.py      # VM instance management
    ├── interface.py     # Network interface management
    ├── label.py         # Label management
    ├── namespace.py     # Namespace management
    ├── network.py       # Network management
    └── node.py          # Cluster node management
```

### Key Components

- **Client class** (`apiclient.py`): The main API client that handles
  authentication, request/response processing, and async operation strategies.
  Supports configuration from environment variables, `~/.shakenfist`, or
  `/etc/sf/shakenfist.json`.

- **CLI** (`main.py`): Built with Click, provides the `sf-client` command with
  subcommands for managing all Shaken Fist resources.

- **Plugin System**: Third-party packages can extend the CLI by registering
  entry points in the `shakenfist_client.plugin` group.

## Python Version Policy

**The client maintains broad Python version compatibility (currently >=3.7)
to support the widest possible range of client distributions.**

This is intentionally more conservative than the Shaken Fist server itself,
which can freely adopt modern Python features. The rationale:

- The server runs on controlled infrastructure where we choose the OS
- The client runs on user machines with varying distributions (including
  enterprise Linux like RHEL 9 which ships Python 3.9)
- Users should be able to manage their Shaken Fist clusters from any
  reasonable workstation

When adding new dependencies or using newer Python features, always check
compatibility with the minimum supported version. Use conditional imports
or backport packages where necessary.

## Recent Changes

### Plugin Loading Modernization (2026-01)

Replaced deprecated `pkg_resources.iter_entry_points()` with
`importlib.metadata.entry_points()`. This change was required because
`pkg_resources` is slated for removal from setuptools.

The implementation uses:
- `importlib.metadata` (standard library in Python 3.8+)
- `importlib-metadata` backport package for Python < 3.9

The code handles API differences between Python versions:
- Python 3.10+: Uses `entry_points().select(group=...)`
- Python 3.9 and earlier: Uses `entry_points().get(group, [])`

See `main.py` lines 7-10 and 137-147 for the implementation.

## Development Notes

### Testing

Install test dependencies with:
```bash
pip install -e ".[test]"
```

Run tests with:
```bash
stestr run
```

### Building

The package uses `setuptools_scm` for version management from git tags.
