Python REST API client for Shaken Fist
======================================

This is a python REST API client and command line client for the minimal
cloud [Shaken Fist](https://github.com/shakenfist/shakenfist).

The library is a complete interface to the Shaken Fist HTTP API.

## VDI console access

`sf-client instance vdiconsole <ref>` opens a graphical console for an
instance. When the Shaken Fist server advertises the
`vdi-console-proxy` capability, the console is opened seamlessly
through the [Kerbside](https://github.com/shakenfist/kerbside) SPICE
proxy; otherwise the client falls back to its existing
direct-to-hypervisor `.vv` path. Either way this "just works" without
any extra flags.

Install the optional `vdi` extra to get a viewer that supports the
seamless proxy path out of the box:

```bash
pip install shakenfist-client[vdi]
```

This pulls in [`ryll`](https://github.com/shakenfist/ryll) on Linux
(it is not published for other platforms, so the extra resolves
cleanly -- but does nothing -- elsewhere). When `ryll` is on `PATH`
and the proxy is used, the console JWT is never written to disk --
`ryll` performs the token exchange itself directly from the proxy
URL.

Two options tune this behaviour:

- `--viewer <ryll|remote-viewer|PATH>` -- override viewer
  auto-detection, which otherwise prefers `ryll` on `PATH` and falls
  back to `remote-viewer`.
- `--direct` -- force the original direct-to-hypervisor `.vv` path,
  bypassing the Kerbside proxy even when the server advertises it.

`remote-viewer` (from `virt-viewer`) remains fully supported as a
fallback for either path -- the `.vv` file the client writes is a
standard virt-viewer file. `sf-client instance vdiconsolefile`
downloads that same `.vv` file without launching a viewer, and
accepts `--direct` too.

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) -- project structure and key components
- [AGENTS.md](AGENTS.md) -- guide for AI agents working on this codebase
- [RELEASE-SETUP.md](RELEASE-SETUP.md) -- one-time release infrastructure setup
