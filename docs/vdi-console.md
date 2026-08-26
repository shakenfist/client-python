# VDI console access

`sf-client instance vdiconsole <ref>` opens a graphical console for an
instance. There are two ways that console can be reached, and the
client chooses between them for you -- neither needs a flag in the
common case.

## The two paths

**Through the Kerbside proxy.** When the Shaken Fist server advertises
the `vdi-console-proxy` capability, the client asks it for a proxy
descriptor and the session is established through the
[Kerbside](https://github.com/shakenfist/kerbside) SPICE proxy. The
descriptor's URL embeds a short-lived, single-use JWT, so the console
is authorised per instance rather than by exposing the hypervisor.

**Direct to the hypervisor.** Where the server does not advertise that
capability -- an older cluster, or one with no proxy deployed -- the
client falls back to the pre-existing path: it downloads a virt-viewer
`.vv` file describing the hypervisor's SPICE port and launches a viewer
against it.

## Choosing a viewer

Viewer auto-detection prefers [`ryll`](https://github.com/shakenfist/ryll)
if it is on `PATH`, and falls back to `remote-viewer` (from
`virt-viewer`) otherwise.

Install the optional `vdi` extra to get a viewer that supports the
seamless proxy path out of the box:

```bash
pip install shakenfist-client[vdi]
```

That pulls in `ryll` on Linux. It is not published for other
platforms, so the extra resolves cleanly -- but does nothing --
elsewhere.

`remote-viewer` remains fully supported on either path: the `.vv` file
the client writes is a standard virt-viewer file.

## Where the token goes

The combination of viewer and path decides whether the console
credential is ever written to disk:

| Path | Viewer | Behaviour |
|------|--------|-----------|
| Proxy | `ryll` | `ryll --url <proxy URL>`; the JWT is never written to disk |
| Proxy | anything else | The `.vv` is fetched to a temporary file, launched, and deleted when the viewer exits |
| Direct | either | The `.vv` is fetched to a temporary file, launched, and deleted when the viewer exits |

Prefer `ryll` on the proxy path where you can, for that reason.

The token is kept out of the terminal too. `--verbose` turns on debug
logging for the HTTP stack, which prints the target of every request
the client makes, and the proxy descriptor arrives in a response body
that is logged in full at the same level. Both are redacted on the way
out, so a `--verbose` transcript can be pasted into a bug report
without handing over a live console session.

## Options

- `--viewer <ryll|remote-viewer|PATH>` -- override viewer
  auto-detection.
- `--direct` -- force the direct-to-hypervisor `.vv` path, bypassing
  the Kerbside proxy even when the server advertises it.

`sf-client instance vdiconsolefile <ref>` downloads the same `.vv`
file without launching a viewer, and accepts `--direct` too.
