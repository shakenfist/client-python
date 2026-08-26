Python REST API client for Shaken Fist
======================================

This is a python REST API client and command line client for the minimal
cloud [Shaken Fist](https://github.com/shakenfist/shakenfist).

The library is a complete interface to the Shaken Fist HTTP API.

## Installation

```bash
pip install shakenfist-client
```

Add the `vdi` extra if you want graphical instance consoles to open
seamlessly through the Kerbside SPICE proxy:

```bash
pip install shakenfist-client[vdi]
```

## Usage

```bash
sf-client instance list
sf-client instance show <ref>
sf-client instance vdiconsole <ref>
```

The client reads its credentials from the environment
(`SHAKENFIST_NAMESPACE`, `SHAKENFIST_KEY`, `SHAKENFIST_API_URL`),
from `~/.shakenfist`, or from `/etc/sf/shakenfist.json`.

## Documentation

<!-- These links are absolute, and stay absolute. README.md is the
     package's long description on PyPI, where a relative link resolves
     against pypi.org and 404s. They point at develop because that is
     where the documentation this release's README describes is
     maintained. -->

- [VDI console access](https://github.com/shakenfist/client-python/blob/develop/docs/vdi-console.md)
  -- graphical consoles, the Kerbside proxy and direct paths, and viewer
  selection
- [ARCHITECTURE.md](https://github.com/shakenfist/client-python/blob/develop/ARCHITECTURE.md) -- project structure and key components
- [AGENTS.md](https://github.com/shakenfist/client-python/blob/develop/AGENTS.md) -- guide for AI agents working on this codebase
- [RELEASE-SETUP.md](https://github.com/shakenfist/client-python/blob/develop/RELEASE-SETUP.md) -- one-time release infrastructure setup
