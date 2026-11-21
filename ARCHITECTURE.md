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
├── credentials.py       # The on disk cache for a human login's credentials
├── util.py              # Shared utilities
├── commandline/         # CLI subcommands (one per resource type)
│   ├── admin.py         # Administrative commands
│   ├── ansible.py       # Ansible integration helpers
│   ├── artifact.py      # Artifact management
│   ├── auth.py          # Human login, logout, credential selection
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
    ├── test_client_apiclient.py
    ├── test_client_commandline_instance.py
    ├── test_client_main.py
    └── test_client_util.py
```

## Key Components

### Client Class (`apiclient.py`)

The `Client` class handles all communication with the Shaken Fist
REST API:

- **Authentication**: API key-based, configured from command line
  arguments, environment variables (`SHAKENFIST_NAMESPACE`,
  `SHAKENFIST_KEY`, `SHAKENFIST_API_URL`), the credential cache a human
  login wrote, `~/.shakenfist`, or `/etc/sf/shakenfist.json`, in that
  order. Each source is consulted only while no API URL has been
  established yet, which is why a cached credential records the cluster
  it was minted against; the environment's namespace and key are the
  exception, read whether or not the environment also names the cluster,
  so that an automated caller cannot acquire a person's login. Because
  the cache is read during construction, constructing a `Client` can
  raise `credentials.CredentialCacheException` for a cache which exists
  but cannot be used -- a new failure mode for library embedders, which
  `suppress_configuration_lookup=True` opts out of.
  `docs/human-login.md` describes the order and what the cache beating
  both configuration files costs
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
- **Namespace capacity claims**: `get_namespace_claims`,
  `get_namespace_claim`, `create_namespace_claim`,
  `update_namespace_claim` and `delete_namespace_claim` administer a
  namespace's claim on aggregate cluster capacity
  (`/auth/namespaces/<namespace>/claims`). A 503 raises
  `ServiceUnavailableException` so a retryable refusal can be caught
  apart from a durable one. `docs/namespace-claims.md` describes the
  two claim states and the refusal statuses.
- **Namespace key metadata**: `get_namespace_keys` and
  `get_namespace_key` read a key's expiry, scopes and provenance
  (`/auth/namespaces/<namespace>/keys`, the collection with
  `detail=true`). Both are gated on the `namespace-key-metadata`
  capability and fail closed, because a server without it answers the
  collection with a bare name list rather than an error.
  `get_namespace_keynames` is ungated and keeps that older shape. No CLI
  verb calls `get_namespace_key` yet; the interactive login work is its
  intended consumer. `docs/namespace-keys.md` describes why provenance
  reaches only a `system` caller and why it is kept out of
  comma-separated output.

- **Agent operation deadlines**: `instance_put_blob`, `instance_get`
  and `instance_execute` accept `deadline_seconds` (and, for put and
  get, `progress_timeout_seconds`) which bound the operation's life on
  the server, plus `await_seconds` which bounds how long this client
  polls for it -- three separate budgets that are deliberately not
  derived from one another. `_await_agentop()` is the single place
  which polls operation state: it breaks on any member of
  `TERMINAL_AGENT_OPERATION_STATES` -- five states, `abort` among
  them, hand-kept in step with the server's own list -- and raises
  `AgentOperationFailed`
  rather than waiting out its budget and reporting a timeout, and
  `await_agent_command()`/`await_agent_fetch()` catch that to attach the
  instance's console data. The timing parameters are gated on the
  `agentoperation-deadlines` capability and fail closed.

- **Agent operation dependencies**: the same three creating helpers
  accept `depends_on` and `runs_after`, lists of edges built by the
  module-level `agent_dependency()`, which name agent operations that
  must finish first. The client validates an edge's shape and leaves
  the cluster's own limits to the cluster. `_add_agentop_dependencies()`
  gates both parameters on the `agentoperation-dependencies` capability
  and, unlike the timing parameters beside it, *raises* rather than
  sending nothing, because a dropped ordering edge means "run now".
  `_verify_agentop_dependencies()` then checks that the edges came back
  on the created operation, which catches a database tier too old to
  store them, and deletes the operation when they did not.
  `await_agent_chain()` awaits a submitted chain and walks
  `dependency_failure` back to the operation which actually failed.

- **Placement queries**: `check_placement()` asks the server whether a
  set of prospective instance shapes would fit the cluster now (`POST
  /placement`), gated on the `placement-query` capability and failing
  closed without sending a request. Shapes are passed through as the
  caller built them -- the server is the only authority on how large a
  request is -- and a set which does not fit is a 200 carrying `fits:
  False`, the refusing pipeline `stage` and the expanded-order `shape`
  index, not an exception: the endpoint exists so that a caller can tell
  a full cluster from a failed query. Nothing is reserved, and per-node
  detail is returned only to an administrator. There is no CLI verb yet,
  so there is no `docs/` page either; both arrive with the phase that
  adds the verb.

- **Transient capacity retry**: `Client(retry_transient_capacity=True)`
  waits out a `507` whose body carries `"transient": true`, sleeping for
  a `Retry-After` clamped to `[TRANSIENT_RETRY_MINIMUM,
  TRANSIENT_RETRY_MAXIMUM]` and bounded by the calling deadline
  `_request_url()` was already given. Off by default, and unreachable
  from the CLI. `APIException` carries the response headers
  (`headers=`, keyword with a default so the five positional form
  downstream repositories use keeps working) so the exception handler
  can read that header at all. `docs/transient-capacity-retry.md`
  describes what the server publishes and what the retry costs.

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
- **Supply chain**: `supply-chain.yml` runs gitleaks over the git
  history (`tools/gitleaks-scan.sh`) and skillsaw over the agent
  context. Both are deliberately ungated by path filters, because a
  credential, or an instruction aimed at an agent, lands in a document
  as readily as in code; the workflow's header says so at length
