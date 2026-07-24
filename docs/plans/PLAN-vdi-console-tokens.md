# Shaken Fist VDI console tokens phase 4: client, CLI, viewer launch

This is the client-python side of a **cross-repository master plan**
whose plan of record lives in the Shaken Fist repository:

> `shakenfist/docs/plans/PLAN-kerbside-vdi-tokens.md`
> (branch `vdi-console-tokens` until merged)

Phases 1-2 (Shaken Fist mints signed tokens; `/admin/vditokenpubkey`
publishes the public keys) and phase 3 (`pip install ryll`) are done.
This phase makes `sf-client instance vdiconsole` land the user in a
session over the Kerbside proxy when it is available, and adds the two
API-client methods Kerbside's source driver (phase 6) will call.

## Decisions

These refine the master plan's open questions 5 and 10 for the client.

1. **Three `apiclient.Client` methods**, thin, matching the existing
   `get_instance` / `get_vdi_console_helper` style
   (`apiclient.py:537`, `:1131` — `self._request_url('GET', path)` then
   `.json()` or `.text`; no internal capability gate — the CLI probes):

   - `get_vdi_console_proxy(instance_ref)` → GET
     `/instances/<ref>/vdiconsoleproxy`, `return r.json()` (the phase-2
     `{url, expires_at}` body — a 200 JSON payload, not a redirect, so
     the default `allow_redirects=True` is harmless).
   - `get_vdi_console_proxy_file(instance_ref)` → the convenience: call
     `get_vdi_console_proxy`, then `requests.get(result['url'])` and
     `return r.text` (the `.vv`). This is a **plain** `requests.get`
     with a timeout — Kerbside's `/sf-console.vv` endpoint takes no SF
     auth (the capability is the JWT already embedded in the URL), so
     it must NOT go through `_request_url` / the SF bearer token.
     Verifies TLS (requests default).
   - `get_vdi_token_public_keys()` → GET `/admin/vditokenpubkey`,
     `return r.json()`. Net-new for Kerbside's phase-6 source driver;
     unused by the CLI. Its presence in a released client is what
     phase 6 depends on (see release note below).

2. **`sf-client instance vdiconsole` becomes the seamless path**
   (`commandline/instance.py:664`). New options:
   `--viewer <ryll|remote-viewer|PATH>` (override auto-detect) and
   `--direct` (force the direct-to-hypervisor `.vv`, bypassing the
   proxy). Path and viewer are chosen as:

   ```
   direct = --direct or not check_capability('vdi-console-proxy')
   viewer = --viewer or ('ryll' if shutil.which('ryll') else 'remote-viewer')

   if not direct:                                   # Kerbside proxy path
       proxy = get_vdi_console_proxy(ref)           # {url, expires_at}
       if viewer is ryll:
           run [ryll, '--url', proxy['url']]        # no temp file; JWT never on disk
       else:
           vv = get_vdi_console_proxy_file(ref)     # mint + fetch .vv
           temp = write(vv); run [viewer, temp]; cleanup
   else:                                            # direct path (unchanged behaviour)
       vv = get_vdi_console_helper(ref)
       temp = write(vv)
       run [ryll, '--file', temp]  (ryll)  |  run [viewer, temp]  (else)
       cleanup
   ```

   `ryll --url` (proxy) is the only branch that never writes a file —
   ryll performs the token exchange itself. Every other branch fetches
   the `.vv` to a `tempfile.mkstemp()` file and cleans it up in a
   `finally`, exactly as today. `ryll` accepts `--url <exchange URL>`
   or `--file <path>` (ryll README); `remote-viewer` only takes a file.

3. **Auto-prefer the proxy when advertised (open question 5, decided).**
   When the server advertises `vdi-console-proxy` and `--direct` is not
   given, `vdiconsole` uses the proxy path — that is the seamless
   mission goal. `--direct` and a proxy-less server both fall back to
   the existing direct path, which keeps working unconditionally.

4. **List-form `subprocess.run`, never `shell=True`.** The current
   command uses `subprocess.run(f'remote-viewer {debug} {temp_name}',
   shell=True)` (`instance.py:685`). Rewrite all launches as arg lists
   (`subprocess.run([viewer, *args])`). This is a correctness/security
   fix: we now pass a URL containing a JWT to `ryll --url`, and a
   shell-interpolated command line is the wrong place for it. `--debug`
   / verbose still flows through as a list element where applicable.

5. **`vdiconsolefile` works for both paths** (`instance.py:694`). It
   gains `--direct`; without it, and when the proxy capability is
   present, it prints the proxy `.vv` (`get_vdi_console_proxy_file`),
   else the direct `.vv` (`get_vdi_console_helper`). No viewer launch.

6. **Viewer selection collapses to "ryll on PATH" (open question 10,
   updated for phase 3's embed model).** Phase 3 ships `ryll` as a wheel
   whose binary lands on `PATH`, so the strawman's "packaged ryll →
   PATH ryll" distinction is gone: detection is `shutil.which('ryll')`,
   else `remote-viewer`, with `--viewer` to override. `remote-viewer`
   must remain a first-class path (the `.vv` we emit is standard
   virt-viewer format).

7. **`vdi` extra** in `pyproject.toml`
   (`[project.optional-dependencies]`, currently only `test`): add
   `vdi = ["ryll ; sys_platform == 'linux'"]` (annotated `# apache2`).
   The platform marker is deliberate — phase 3 publishes Linux-only
   `ryll` wheels, so `pip install shakenfist-client[vdi]` must not fail
   to resolve on macOS/Windows; there, users install a viewer
   (`remote-viewer` or a ryll release) themselves and the CLI still
   works.

8. **Release note.** Kerbside phase 6 calls `get_vdi_token_public_keys()`,
   so a client release (a `v*` tag → the existing Trusted-Publisher /
   Sigstore `release.yml`) must precede it, and Kerbside then floor-pins
   `shakenfist-client`. Flagged here; the tag itself is an operator
   action, not part of this phase.

## Execution

All in the `client-python-wt-vdi-tokens` worktree (branch
`vdi-console-tokens`), by sub-agents. Review each step's **actual
files**. 4b depends on 4a; 4c is independent.

| Step | Effort | Model | Brief for sub-agent |
|------|--------|-------|---------------------|
| 4a | medium | sonnet | Add the three methods in decision 1 to `shakenfist_client/apiclient.py`, matching `get_instance`/`get_vdi_console_helper`. `get_vdi_console_proxy_file` uses a plain `requests.get(url, timeout=...)` (NOT `_request_url`; no SF auth) and returns `.text`. Add unit tests mirroring the repo's existing apiclient test style (mock `_request_url` to return a fake response with `.json()`/`.text`; mock `requests.get` for the file convenience; assert the proxy-file path does not attach the SF bearer token). |
| 4b | medium | sonnet | Rework `vdiconsole` and `vdiconsolefile` in `shakenfist_client/commandline/instance.py` per decisions 2-6. Add `--viewer`/`--direct` options; implement the path/viewer matrix; `shutil.which` for detection (add `import shutil`); ALL launches via list-form `subprocess.run([...])` (no `shell=True`); preserve `--debug`/verbose passthrough and the `mkstemp`+`finally` cleanup. Keep the existing `check_capability` incapability messaging style (`sys.stderr.write` + `sys.exit(1)`) for the no-viewer-found and proxy-unavailable-with-`--direct`-absent-but-also-no-direct cases. Add CLI tests mocking `shutil.which`, `subprocess.run`, and the client methods, covering each cell of the matrix (proxy+ryll uses `--url` and writes no temp file; proxy+remote-viewer and both direct branches write+clean a temp file; `--direct` forces direct; `--viewer` overrides). |
| 4c | low | sonnet | Add the `vdi` extra (decision 7) to `pyproject.toml`. Update `README.md` (and `AGENTS.md`/`ARCHITECTURE.md` if they describe the console commands) for the seamless `vdiconsole`, the `[vdi]` extra, `--viewer`/`--direct`, and the ryll→remote-viewer fallback. Update `sf-client instance vdiconsole --help` text via the click `help=`. |

## Success criteria

* `get_vdi_console_proxy` / `get_vdi_console_proxy_file` /
  `get_vdi_token_public_keys` exist, are unit-tested, and the
  proxy-file fetch carries no SF bearer token.
* Against a proxy-capable server, `sf-client instance vdiconsole` with
  `ryll` on PATH opens a session via `ryll --url` with the JWT never
  written to disk; with only `remote-viewer`, it fetches the `.vv` to a
  temp file and launches it; `--direct` and a proxy-less server use the
  existing direct path unchanged.
* No launch uses `shell=True`.
* `pip install shakenfist-client[vdi]` pulls `ryll` on Linux and
  resolves cleanly (no `ryll`) elsewhere.
* `tox` (flake8 + unit tests) passes.

## Out of scope

Kerbside's consumption of `get_vdi_token_public_keys()` and the
`/sf-console.vv` exchange (phases 5-6); the client `v*` release tag
(operator action); any change to the direct-to-hypervisor endpoint
itself (server-side).
