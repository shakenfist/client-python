# OIDC authentication phase 7: interactive login flows, the credential set and the token cache

## Prompt

Before responding to questions or discussion points in this
document, explore both the client-python and shakenfist codebases
thoroughly. Read relevant source files, understand existing
patterns (`apiclient`'s verb shape and capability gating, the
`sf-client` click groups and their three output modes, the
`CliRunner` tests in `shakenfist_client/tests/`), and ground your
answers in what the code actually does today. Do not speculate
about the codebase when you could read it instead. Flag any
uncertainty explicitly rather than guessing.

The plan of record is shakenfist's
`docs/plans/PLAN-oidc-authentication.md`. Its phase 7 is the
interactive half of human login: the two OAuth flows a person
runs to turn an identity at an external provider into Shaken Fist
credentials, and the cache those credentials live in afterwards.
The server half is finished and merged -- phase 3 built issuer
discovery, phase 4 mints the entitled credential set, and phase 6
made a key's own metadata readable -- so nothing in this phase
needs a server change, which the survey below verifies rather
than assumes.

The code lands entirely in this repository, so the plan file
lives here alongside the change it plans. That follows
`PLAN-vdi-console-tokens.md`, agent operation deadlines phase 6
and scheduler reservations phase 7b.

Consult this repository's `AGENTS.md` and `CLAUDE.md`. Its
conventions are not identical to the server's: Python >= 3.7,
single quotes, 120 character lines. Note also that this
repository's pre-commit forbids citing plan phases, decisions or
step numbers from code and documentation; this plan file is the
only place those names belong.

## Planning effort

High, as the master plan specifies. Two OAuth flows, a credential
at rest, and a cache whose invalidation rules must not outlive
the server's view of the credential. Review at high effort as
well: this phase writes the first credential this client has ever
stored on disk, and the failure mode -- a cache that quietly
extends the life of a credential the cluster considers dead -- is
silent.

## Scope

In:

* `apiclient` verbs for the two login routes, capability gated.
* A credential cache on disk: its format, its file mode, its
  invalidation rules and the code that reads and writes it.
* The device authorization flow, end to end, including the
  polling rules RFC 8628 specifies.
* The authorization code flow with PKCE, for a host where a
  browser can be opened.
* `sf-client login` and `sf-client logout`.
* Selecting one credential out of the set for use, and showing a
  person what they hold.
* Documentation and release notes for all of it.

Out, explicitly:

* Any change to this cluster's server. The survey confirms none
  is needed; a step which finds otherwise stops and says so
  rather than reaching across the repository boundary.
* `sf-client` verbs for issuers and mapping rules. That is the
  operator's side of federation and is client-python#451.
* Renaming namespace keys to service account tokens. The master
  plan's open question 11 was answered no.
* Functional coverage against a containerised identity provider.
  That is the master plan's phase 9, and it is the only honest
  test of these flows end to end -- which is a reason to sequence
  it after this phase, not a reason to attempt it here.
* Accepting a foreign bearer token directly, in place of
  exchanging it. The master plan's open question 6 answered that
  no for this plan.

## What the survey found

The master plan's phase 7 paragraph was written before phases 3,
4 and 6 executed. Two of its claims survive verification and two
do not.

### 1. "Entirely client-python" is true, and the server is complete

Verified rather than assumed, because the whole shape of this
phase depends on it.

`GET /auth/login` (`shakenfist/external_api/auth.py:2984`) is
public and serves exactly what a client needs to start a flow:
`name`, `issuer_url`, `client_id`, `oidc_scopes`, and whichever
of `authorization_endpoint`, `token_endpoint` and
`device_authorization_endpoint` are set. The endpoints appear
only when populated, deliberately, so that their absence is
itself the answer to "can this provider do the device flow". An
issuer is listed only when it is login capable, which is narrower
than trusted.

`POST /auth/login` on the same resource exchanges a signed
identity token for every credential the identity entitles its
holder to, answering with a list of `{namespace, key_name, key,
expiry, scopes}` objects (`login_mint_example`,
`shakenfist/external_api/auth.py:2573`). Each secret is returned
once and never again.

Both halves are advertised: `human-login-metadata` and
`human-login` are separate capability tokens in the `auth` family
(`shakenfist/external_api/app.py:497` and `:508`), and the
comment there says why they are separate -- a client meeting only
the metadata half can run a flow and then fall back to the
targeted exchange, where a client which assumed the mint would
post to a route that 404s.

Phase 3 also stored all three endpoints, including
`device_authorization_endpoint`
(`shakenfist/trusted_issuer.py:44-46`), so discovery already
supplies what the device flow needs.

### 2. This client has no login anything

`shakenfist_client/commandline/` holds ten modules and none of
them is about authentication: `admin`, `artifact`, `backup`,
`blob`, `instance`, `interface`, `label`, `namespace`, `network`,
`node`. A grep of the whole package for `auth/login`, `def
login`, `device_code`, `device_authorization`, `PKCE` and `pkce`
returns nothing. This phase is greenfield here.

### 3. The multiplicity cannot be resolved per request

This is the finding that changes the plan.

The master plan says "The client holds one namespace key per
namespace and presents the right one for each request". A request
does not carry enough information to choose:

* `Client` is constructed once, eagerly, in the `cli` group
  callback with a single `namespace` and `key`
  (`shakenfist_client/main.py:202-237`).
* `_authenticate()` posts that one pair to `/auth` and returns
  one bearer token (`shakenfist_client/apiclient.py:642-655`),
  which `_request_url()` keeps in `self.cached_auth` and
  refreshes on a 401 (`:664-692`).
* A verb like `get_instance(instance_ref)`
  (`shakenfist_client/apiclient.py:938`) names an object, not a
  namespace. The `namespace=` keyword some verbs accept is an
  *administrator's cross-namespace lookup*, sent in the request
  body and gated on a separate capability
  (`get-instance-namespace`); it is not credential selection.

So "the right one for each request" is not derivable. The set has
to be resolved by an explicit current-namespace selection, which
is D3 below. The master plan's sentence has been corrected at
source.

### 4. Open question 10's proposed cache path cannot exist

Q10 proposes `~/.shakenfist/oidc-cache` with mode 0600. But
`~/.shakenfist` is a *file*, not a directory: the client expands
that exact path and `json.load`s it as one of its four
configuration sources
(`shakenfist_client/apiclient.py:490-501`). A directory of that
name cannot coexist with the configuration file every existing
user already has, so the proposal would break any host that has
one. Corrected at source, and D2 picks a path that does not
collide.

### 5. Q10's refresh-token premise was already answered

Q10 asks where the client caches "the OIDC refresh token and
access token". Phase 0 narrowed that: the client caches namespace
keys and the access tokens minted from them rather than an IdP
refresh token (the master plan's phase 0 resolution of open
question 13). Q10's own wording was never updated. Corrected at
source. The narrowing matters because a cached IdP refresh token
would be a credential whose revocation this cluster does not
control.

### 6. The client has never stored a credential on disk

Outside its tests, the package writes files in exactly two places
and neither is a credential (`commandline/backup.py:37`, a backup
destination, and `commandline/instance.py:749`, a temporary
file). There is no `os.chmod`, no `0o600` and no `0o700`
anywhere. Every part of protecting a credential at rest is new
code in this phase, which is why D5 and the risks section give it
the attention they do. `tools/gitleaks-scan.sh` scans the git
history with a positive control, so test fixtures must not plant
anything that looks like a real secret.

### 7. Capability gating is an established pattern

`IncapableException` (`shakenfist_client/apiclient.py:51`) and
`check_capability()` (`:553`) are the house mechanism, used at
`:859` and `:879` among others. The most recent example is the
pair of namespace key metadata verbs this plan's phase 6 added,
which raise rather than degrade.

### 8. Nothing else found

No earlier phase shipped any part of this phase's scope, and
there is no half-built login in the tree to finish or remove.

### Corrections made at source

As part of the registration commit in the shakenfist repository:

* The phase 7 paragraph's "presents the right one for each
  request" claim, replaced with the explicit selection this plan
  adopts.
* Open question 10's proposed path, which collides with the
  existing configuration file, and its refresh-token premise,
  which phase 0 had already answered.
* The plan's `index.md` description, so it reflects what the
  survey found rather than what the master plan assumed.

A later step should not redo these.

## Decisions

**D1. The plan file lives in this repository.** Every line of
code this phase writes is here, and the three precedents named in
the Prompt all put the plan beside the code. The master plan's
Execution table links it absolutely, because a relative link
cannot cross repositories.

**D2. The cache is `~/.config/shakenfist/credentials.json`, mode
0600, in a directory created mode 0700.** `XDG_CONFIG_HOME` is
honoured when set. Q10's `~/.shakenfist/oidc-cache` is impossible
(survey finding 4) and `~/.shakenfist-credentials` would clutter
the home directory with a second dotfile for the same tool. A
JSON object rather than a bare list, so the file can carry a
format version and the current-namespace selection alongside the
credentials without a second file.

Each entry also records the API URL it was minted against, and
the current selection names a cluster as well as a namespace.
That was missing from this decision as first written, and step
7a's review found why it cannot be: the existing credential
chain is gated on the API URL rather than on the credential, so
each source is consulted only `if not base_url`
(`shakenfist_client/apiclient.py:479-516`). A cache offering a
namespace and key but no API URL has nowhere to join that chain.
The stronger reason is that a namespace key is cluster scoped, so
a cached credential with no record of which cluster minted it
could be presented to a different one.

**D3. The set is resolved by an explicit current namespace, not
per request.** `login` writes every credential it was given and
records one as current -- the only member when there is one, and
otherwise the member the person chose, or none, in which case the
next command says what to pick from rather than guessing. A new
verb switches the selection. Nothing infers a namespace from a
request, because nothing can (survey finding 3).

**D4. Device authorization first, authorization code with PKCE
second.** The device flow needs no browser, no loopback listener
and no redirect registration, so it works over SSH and in a
container -- which is where a Shaken Fist operator usually is.
The browser flow is better where a browser exists, and it is
built second so that the first flow is not held up behind a
loopback HTTP server and a PKCE verifier. `login` picks the flow
from what the cluster advertises and what the host can do, and
`--flow` overrides it.

**D5. What is cached, and what invalidates it.** The cache holds
namespace keys -- which are long lived secrets, and the thing
being protected -- and may hold the access tokens minted from
them. It never holds an identity token or an IdP refresh token:
one is spent at the exchange and the other is a credential whose
revocation this cluster does not control. The rules:

* An access token is used only while its own expiry is in the
  future, with a safety margin; the client already re-authenticates
  on a 401, so a wrong guess is a retry rather than a failure.
* A namespace key whose `expiry` has passed is dropped when the
  cache is read, not offered and allowed to fail.
* `logout` deletes the file, not just the selection, and says how
  many credentials it discarded. A cached credential surviving a
  logout the person initiated is the specific failure Q10 warns
  about.
* The cache is never a *source* of a credential the person did
  not log in for: it is read for login credentials only, and
  carries no copy of a key supplied by flag, environment or
  configuration file.

**D6. The two halves are gated separately.**
`get_login_issuers()` checks `human-login-metadata` and
`login()` checks `human-login`, each raising
`IncapableException` with a message naming what the cluster
cannot do. Separately, because the server's own comment says a
client may meet one and not the other.

**D7. The cache sits between the environment and the
configuration files in the credential precedence.** Today the
order is command line flags, then environment variables, then
`~/.shakenfist`, then `/etc/sf/shakenfist.json`
(`shakenfist_client/apiclient.py:474-516`). The cache is
inserted third.

This is the decision most likely to be argued with, so here is
the reasoning. A person who has just run `sf-client login`
expects the next command to use that login; if a stale
`~/.shakenfist` from a year ago won instead, the login would
appear to have done nothing, and the failure would be a confusing
401 rather than a message. Putting the cache above both
configuration files makes the explicit, recent, interactive act
beat the implicit, old, file-based one. Flags and environment
variables still win, because those are equally explicit and are
how CI and scripts pin a credential -- and a script that sets
`SHAKENFIST_KEY` must not start using a developer's personal
login because they happened to log in on the same machine.

The cost is that an operator with a `~/.shakenfist` who logs in
once will keep using the login until they log out. `logout`
restores the previous behaviour exactly, and the documentation
says so.

**D8. The refusals the server can give are handled by name.**
`POST /auth/login` can answer 401 (refused), 429 (rate limited,
with a budget separate from the exchange's), 413 (body too large)
and 507 (the identity satisfies more mapping rules than one login
may mint for). Each gets a message a person can act on; 507 in
particular is an operator's configuration problem and must not
read as "your password is wrong". The metadata route can answer
429 and 503.

**D9. An empty issuer list is a complete answer.** The server
documents an empty list as "this cluster offers no human login".
`login` says that and exits non-zero without attempting a flow.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 7a | medium | sonnet | none | Add `get_login_issuers()` and `login(token)` to `shakenfist_client/apiclient.py`, capability gated per D6. Mirror the shape of the namespace key metadata verbs added most recently in that file: `check_capability()` first, `raise IncapableException(...)` with a message naming the missing ability, then `self._request_url(...)` and `return r.json()`. Both routes are public on the server, so neither needs authentication -- check how `apiclient` reaches an unauthenticated route today before assuming `_request_url` is right, and say what you found. Add `CliRunner`-free unit tests in `shakenfist_client/tests/test_client_apiclient.py` following the existing capability-gating tests there. No CLI in this step. |
| 7b | high | opus | none | Write the credential cache module, per D2 and D5: read, write, drop-expired, select-current, delete. New file `shakenfist_client/credentials.py`. The file is `~/.config/shakenfist/credentials.json` honouring `XDG_CONFIG_HOME`, mode 0600, directory 0700, carrying a format version, the credential list and the current selection. This client has never written a credential to disk (see the survey), so there is no local pattern to follow: create the directory with the right mode rather than creating then chmod'ing, and make a read of a world-readable file a refusal with a message rather than a silent use. Tests must cover the modes, a corrupt file, a missing file, a file from a future format version, and an expired credential being dropped. Do not put anything resembling a real secret in a fixture: `tools/gitleaks-scan.sh` scans history with a positive control. |
| 7c | high | opus | none | Wire the cache into credential resolution per D7, in `shakenfist_client/apiclient.py`'s `Client.__init__` where the four existing sources are read in order at lines 474-516. The cache is consulted after the environment and before `~/.shakenfist`. Keep the existing precedence among the other three exactly as it is, and leave `Client` single-credentialed: the current selection chooses one key, and nothing in this step makes `Client` hold a set. Add tests that pin the whole precedence order, including that a flag and an environment variable each still beat the cache, which is the property D7 argues for. |
| 7d | xhigh | opus | worktree | Implement the device authorization flow and `sf-client login` / `sf-client logout` in a new `shakenfist_client/commandline/auth.py`, registered in `main.py` beside the other ten groups. The flow is RFC 8628: POST `client_id` and the scopes to the issuer's `device_authorization_endpoint`, show the user code and verification URI, then poll `token_endpoint` honouring `interval`, `slow_down`, `authorization_pending` and `expires_in` rather than polling flat out. The identity token that comes back goes to `apiclient.login()`, whose result is written through the step 7b cache. Handle D8's refusals by name and D9's empty list. The issuer list comes from `get_login_issuers()`; pick the single issuer automatically and prompt among several. Honour the three output modes the other groups implement. Never log or print a token: see `RedactTokensFilter` in `main.py` for what this client already does about that, and note that it covers logging only. |
| 7e | xhigh | opus | worktree | Add the authorization code flow with PKCE as a second flow behind the same `login` command, selected per D4 with a `--flow` override. A loopback HTTP listener on an ephemeral port receives the redirect; generate the PKCE verifier and challenge with `secrets` and `hashlib`, generate and verify the `state` parameter, and refuse a redirect whose state does not match. Bind the listener to localhost only, serve exactly one request, and time out rather than waiting forever. Open the browser with `webbrowser`, and if that fails print the URL instead of hanging. The listener and the verifier are the new security surface in this phase, so the tests must cover a mismatched state, a wrong port, no browser available, and the timeout. |
| 7f | medium | sonnet | none | Add a verb to show and change what the person holds: list the cached credentials with their namespace, expiry and scopes, marking the current one, and switch the selection. Secrets are never printed in any output mode. Where the cluster advertises `namespace-key-metadata`, read each key's live metadata rather than trusting the cache's copy, and say which you used; that capability and its verbs landed in this plan's previous phase. Follow `commandline/namespace.py`'s three output branches, including that the comma separated output has a header row and no serialised objects in it. |
| 7g | high | opus | none | Documentation and release notes. A new `docs/human-login.md` covering both flows, the cache's location and mode, the precedence change D7 makes, what `logout` does, and the fact that provenance on a login-minted key names the person. Index it from `README.md` and mention it in `ARCHITECTURE.md` only if the component inventory genuinely changed. Update the repository's release notes. Then mutation test the security-relevant guards this phase added -- the state check, the file mode refusal, the expiry drop, the precedence order -- on a copy of the tree, one mutant per run under `timeout`, and record which named test killed each. A guard no test notices is a finding, not a note. |

Steps run in order. 7a and 7b are independent of each other but
both run before 7c, and 7d before 7e because the second flow
reuses the first's plumbing. 7f and 7g need everything before
them. None of them may run in parallel in one worktree: this
repository's pre-commit stashes repo-wide.

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| The cache outlives the credential the cluster considers dead, which is the one property Q10 names | D5's rules, and 7g mutation tests the expiry drop. The management session re-runs that mutant itself rather than trusting the report |
| A credential reaches a log, a traceback or the terminal | `RedactTokensFilter` covers logging only, as its own docstring says. 7d and 7e must redact where they raise, and 7g's review greps every new print, log and exception path in the phase's diff rather than the reported one |
| The loopback listener accepts a redirect it should not | 7e verifies `state`, binds localhost only, serves one request and times out. Tests for mismatched state and timeout are named in the brief |
| The precedence change surprises an operator with a `~/.shakenfist` | D7 accepts it deliberately and documents it; `logout` restores the old behaviour exactly. Michael confirms the ordering in the back brief, because it is cheap to change now and expensive after release |
| A fixture plants something a credential scanner flags | 7b's brief says so, and `tools/gitleaks-scan.sh` runs with a positive control |
| A step decides it needs a server change | Out of scope by the Scope section. The step stops and reports; the survey says none is needed, and a step that disagrees has found something the survey missed |

## Definition of done

* [ ] `sf-client login` completes a device authorization flow
      against an issuer that advertises
      `device_authorization_endpoint`, and writes the credential
      set to the cache.
* [ ] `sf-client login --flow=code` completes an authorization
      code flow with PKCE on a host with a browser.
* [ ] `stat -c %a ~/.config/shakenfist/credentials.json` is
      `600`, and `stat -c %a ~/.config/shakenfist` is `700`,
      after a login on a host with neither.
* [ ] Reading a credentials file whose mode is not 0600 refuses
      with a message naming the file, rather than using it.
* [ ] `sf-client logout` removes the file. `test -e
      ~/.config/shakenfist/credentials.json` fails afterwards.
* [ ] A credential whose `expiry` is in the past is not offered:
      a unit test writes one into a cache file and asserts it is
      absent from what the reader returns.
* [ ] The credential precedence is pinned by tests in all four
      orderings D7 names: a flag beats the cache, an environment
      variable beats the cache, the cache beats `~/.shakenfist`,
      and the cache beats `/etc/sf/shakenfist.json`.
* [ ] `get_login_issuers()` and `login()` each raise
      `IncapableException` against a cluster advertising neither
      capability, and the two messages differ.
* [ ] No secret appears in any output mode:
      `sf-client <the new verb> --json`, `--simple` and
      `--pretty` are grepped for the `sfk_` prefix and for the
      fixture's key material, and match nothing.
* [ ] `grep -rn "sfk_" shakenfist_client/tests/` matches no value
      that is shaped like a real key, and
      `tools/gitleaks-scan.sh` passes.
* [ ] An empty issuer list produces the "this cluster offers no
      human login" message and a non-zero exit, not a traceback.
* [ ] A 507 from the mint produces a message that names the
      cluster's mapping rules as the cause, not the person's
      credentials.
* [ ] `stestr run` passes, `tox -eflake8` passes, and
      `pre-commit run --all-files` passes.
* [ ] Every mutation in 7g is killed by a named test, recorded in
      this file's Results section. A surviving mutation is a
      failed definition of done, not a note.
* [ ] `docs/human-login.md` exists, is linked from `README.md`,
      and states the cache path, the file mode, the precedence
      position and what `logout` does. No fact about any of those
      four is stated differently in two places.
* [ ] `git diff --stat develop... -- ':!docs/plans'` touches only
      `shakenfist_client/apiclient.py`,
      `shakenfist_client/credentials.py`,
      `shakenfist_client/commandline/auth.py`,
      `shakenfist_client/main.py`, files under
      `shakenfist_client/tests/`, `README.md` and files under
      `docs/`. No change to this repository's packaging, and no
      change in the shakenfist repository at all.

## Back brief

Three things want Michael's answer before 7a starts. The first
two are cheap to settle now and expensive to redo.

1. **Is phase 7 one phase or two?** As planned it is seven steps,
   two of them `xhigh`, and 7e -- a loopback listener plus PKCE
   -- is a security surface of its own. Splitting into 7a (the
   device flow, the cache and `login`/`logout`) and 7b (the
   browser flow) would give a reviewable boundary between them
   and get a working login in front of users sooner. Keeping it
   whole matches what the master plan's Execution table says
   today. The plan is written whole; splitting it means renaming
   this file and adding a row, which is cheap before any code
   moves and annoying afterwards.

2. **D7, the credential precedence.** The cache is proposed above
   both configuration files and below flags and the environment.
   That means an operator who logs in once keeps using the login
   until they log out, in preference to their `~/.shakenfist`.
   The alternative is to put the cache last, where it only
   applies to a host that has no configuration file at all --
   safer for existing users, and it makes `login` look broken on
   every host that has one.

3. **May this session open the two pull requests?** There are
   two: this plan file plus its index row here, and phase 6's
   close-out plus phase 7's registration in shakenfist.

Nothing in this phase touches a live cluster, so there is no gate
for that. Step 7d and 7e are tested against a stub issuer rather
than a real one; a real provider is phase 9's job.

## Future work

* Functional coverage of both flows against a containerised
  identity provider, which is the master plan's phase 9 and the
  only end-to-end test of this phase.
* `sf-client` verbs for issuers and mapping rules, so an operator
  can configure what this phase consumes: client-python#451.
* Sorting a namespace key listing in the database's collation
  rather than Python's codepoint order. Raised against the
  previous phase's follow-up and declined there as an API
  behaviour change; it has no owner yet.

## Execution notes

This file was committed at planning time. Execution began on
2026-11-19 AEDT.

**Back brief answered, before 7a.** Michael settled all three
questions. The phase stays whole rather than splitting the
browser flow into its own half. D7 stands as planned, with the
cache above both configuration files and below flags and the
environment. This session may push both branches and open both
pull requests, here and in the shakenfist repository; merging
remains his.

**Two facts corrected at source before the steps that depend on
them.**

D2 did not say the cache records which cluster minted each
credential, and it has to, for the two reasons now given there.

Step 7a's row said to send its requests through `_request_url()`.
That method authenticates before doing anything else, so for a
caller who has not logged in yet it raises out of `POST /auth`
with a namespace and key of `None` before the login request is
made -- the chicken and egg the row did not see.
`_actual_request_url()` sends no Authorization header when there
is no cached credential, because `requests` drops a header whose
value is `None`, and it still maps a status code to an exception.
The step used that instead, and its tests patch it rather than
the shared fixture's `_request_url` mock.

**Step 7a found two defects outside its brief**, both fixed in
its commit rather than deferred. `check_capability()` matched a
capability as a substring of the API root page, so `human-login`
matched inside `human-login-metadata` and a metadata-only cluster
passed the gate on the mint -- which is exactly the failure the
server's own comment on those two tokens warns about. And both
bodies of the mint were being debug logged in full: the request
is an identity token and the response is every secret the call
will ever return, and the filter `sf-client` installs redacts a
JWT, which a namespace key is not.

**A defect this phase surfaces but 7a did not fix.**
`UnconfiguredException` is not caught by `main.py`'s
`GroupCatchExceptions`, so on a host with no `~/.shakenfist` and
no `SHAKENFIST_API_URL` the first verb a new user ever runs,
`login`, ends in a traceback rather than a message. It predates
this phase and affects every verb, but `login` is the one where
having no configuration is the normal case. Step 7d owns it.

**The steps ran in one worktree, not the isolated ones two rows
asked for** (2026-11-20, AEDT). The isolation column exists so
that steps running at the same time do not collide over a
pre-commit stash which is taken repository wide. Nothing ran at
the same time: the browser flow extends the file the device flow
creates, so those two are ordered by the work itself, and every
other row either writes to the shared tree or is the only thing
running. What the column would have bought was therefore nothing,
and what it would have cost was transferring each step's work
between trees before review, which means reviewing a tree the
step never ran its own checks in.

**A defect the review of the credential precedence found.** The
step filled in a credential for a caller which had named a
cluster but no namespace or key -- which nothing answered before,
since every source is gated on there being no API URL yet -- and
did it without re-reading the environment. The environment block
above it only runs when the environment supplied the API URL too,
so a caller which passed the cluster as a flag with
`SHAKENFIST_KEY` set in its environment had the cache outrank
that key: the inversion the precedence decision exists to
prevent, in the words that decision uses. Fixed by consulting the
environment there before the cache, which is also what the
command line already does, since click reads both variables
whether or not it was given an API URL.

**A defect the review of the device flow found.** A login which
minted several credentials and selected none of them said nothing
at all when the cache already held a selection for a *different*
cluster, because the advice was conditioned on there being no
selection rather than on none of this login's own credentials
being selected. That is the worse of the two cases: the next
command resolves the surviving selection and talks to the other
cluster as the other namespace, so a login which had just
succeeded appears to have done nothing. The other cluster and
namespace are now named in the message.

## Results

### Documentation

`docs/human-login.md` covers both flows, the cache's location and mode
and every refusal reading it can produce, the precedence position D7
argues for and the one addition made for a caller which named a cluster
but no credential, what `logout` does and why it needs no cluster, and
the provenance a login-minted key carries -- including that the server
serves `provenance` to the `system` namespace alone, so the person who
logged in does not see it in their own namespace's listing. Linked from
`README.md`, whose Usage paragraph listed the credential sources and
now names the cache among them.

`ARCHITECTURE.md` was changed because the component inventory did:
`credentials.py` and `commandline/auth.py` are new modules, and the
`Client` authentication bullet listed the credential sources, which the
cache has changed. `AGENTS.md` was not: no convention changed, and the
invariants a reader might otherwise need -- create with the mode rather
than chmod afterwards, the redaction filter matching only a JWT, the
verbs the CLI group must not build a client for -- are each carried at
length in the docstrings and comments beside the code they constrain,
and `AGENTS.md` costs context on every task in the repository.

**Release notes: there is nothing in this repository to update.** There
is no release notes file and no changelog; releases run
`softprops/action-gh-release` with `generate_release_notes: true`
(`.github/workflows/release.yml:248`), so GitHub builds the notes from
the commit messages between tags at tag time. The step row's
instruction to update them is satisfied by the commit messages this
phase already wrote, and inventing a file would create a second
convention. Separately, the shakenfist repository's
`docs/release_notes/v07-v08.md` documents federated identity and says
nothing about human login; that is a different branch and a different
pull request, so it was left alone and raised with Michael instead.

### Mutation testing

Harness:
`/srv/nobackups/claude-tmp/.../scratchpad/step-7g/mutate.py`, run
against a copy of the tree in `step-7g/tree`, one mutant per run with
the suite bounded by a timeout and the mutated file restored from
`step-7g/pristine` rather than with git. Thirty mutants across the four
guards the step row names; the output is `step-7g/mutate-final.out`.
The harness prints the module path of the copy it tested before
applying anything, because a script run from the scratch directory
otherwise imports the installed package.

**Thirty mutants, thirty killed.** The two which survived the first
pass are recorded below with what was added to kill them.

| Guard | Mutant | Killed by |
|-------|--------|-----------|
| state check | `_states_match()` returns True always | `test_client_commandline_auth.PkceTestCase.test_the_state_comparison_refuses_everything_but_a_match` |
| state check | states compared by length only | `PkceTestCase.test_the_state_comparison_refuses_everything_but_a_match` |
| state check | a redirect with no state is accepted | `LoopbackListenerTestCase.test_a_redirect_with_no_state_at_all_is_refused` |
| state check | the code is read before the state is checked | `LoopbackListenerTestCase.test_a_mismatched_state_is_refused_and_the_code_discarded` |
| state check | a wrong state no longer ends the login | `AuthorizationCodeFlowTestCase.test_a_mismatched_state_stops_the_flow_before_the_exchange` |
| state check | every login uses the same state | `PkceTestCase.test_no_two_logins_share_a_verifier_or_a_state` |
| file mode | `UNSAFE_FILE_BITS = 0` | `test_client_credentials.CredentialsTestCase.test_a_readable_file_is_refused_and_names_itself` |
| file mode | only the group and other read bits are unsafe | `CredentialsTestCase.test_every_group_and_other_bit_is_refused` |
| file mode | the mode is not checked when the cache is read | `CredentialsTestCase.test_a_readable_file_is_refused_and_names_itself` |
| file mode | the written file's mode is not verified | `CredentialsTestCase.test_a_write_which_cannot_verify_the_mode_refuses` |
| file mode | the directory is created 0755 | `CredentialsTestCase.test_the_directory_is_created_owner_only` |
| file mode | `UNSAFE_DIRECTORY_BITS = 0` | `CredentialsTestCase.test_a_group_writable_directory_is_refused` |
| file mode | the directory's mode is not checked on read | `CredentialsTestCase.test_a_group_writable_directory_is_refused_on_read` |
| file mode | `CACHE_FILE_MODE = 0o666` | `CredentialsTestCase.test_the_file_is_created_owner_only` -- **survived the first pass** |
| file mode | a created directory's mode is not verified | `CredentialsTestCase.test_a_directory_created_with_the_wrong_mode_refuses` -- **survived the first pass** |
| expiry drop | `_has_expired()` returns False always | `CredentialsTestCase.test_an_expired_credential_is_dropped_on_read` |
| expiry drop | the boundary is `<` rather than `<=` | `CredentialsTestCase.test_a_credential_expiring_exactly_now_is_dropped` |
| expiry drop | no expiry is treated as expired | `CredentialsTestCase.test_a_credential_with_no_expiry_is_kept` |
| expiry drop | `_live_view()` keeps every credential | `test_client_commandline_auth.CredentialCommandTestCase.test_an_expired_credential_is_not_listed` |
| expiry drop | the selection survives the credential it named | `CredentialsTestCase.test_a_selection_naming_an_expired_credential_is_dropped` |
| precedence | the cache is consulted before the environment | `test_client_apiclient.ApiClientConfigurationLookupTestCase.test_an_environment_variable_beats_the_cache` |
| precedence | the cache overwrites a credential already supplied | `ApiClientConfigurationLookupTestCase.test_a_flag_beats_the_cache_without_an_api_url` |
| precedence | `~/.shakenfist` is consulted before the cache | `ApiClientConfigurationLookupTestCase.test_the_cache_beats_the_user_configuration_file` |
| precedence | `/etc/sf/shakenfist.json` is consulted before the cache | `ApiClientConfigurationLookupTestCase.test_the_cache_beats_the_system_configuration_file` |
| precedence | the environment is not re-read for a named cluster | `ApiClientConfigurationLookupTestCase.test_an_environment_credential_beats_the_cache_for_a_named_cluster` |
| precedence | half a credential is completed from the cache | `ApiClientConfigurationLookupTestCase.test_a_half_supplied_credential_is_not_completed_from_the_cache` |
| precedence | half an environment credential is completed from the cache | `ApiClientConfigurationLookupTestCase.test_an_environment_key_alone_is_not_completed_from_the_cache` |
| precedence | the selection is used whichever cluster made it | `ApiClientConfigurationLookupTestCase.test_another_cluster_is_not_answered_from_the_cache` |
| precedence | an ambiguous cache is guessed between | `ApiClientConfigurationLookupTestCase.test_several_credentials_for_a_named_cluster_are_not_guessed_between` |
| precedence | an unreadable cache is passed over silently | `ApiClientConfigurationLookupTestCase.test_a_cache_which_cannot_be_read_is_refused_not_passed_over` |

**The two survivors, and what they were.** Neither was a guard which
worked and went unnoticed; both were a mode nothing compared against.

`CACHE_FILE_MODE` is read by no code at all: `tempfile.mkstemp()`
creates the cache file 0600 and `_write_atomically()` verifies it
against `UNSAFE_FILE_BITS`, so the constant documents an intention and
nothing checked that the intention matched the file. Loosening it to
0666 changed nothing and no test noticed.
`test_the_file_is_created_owner_only` now compares the created mode
against the constant rather than against a literal, which is the only
thing that can keep the two from drifting apart;
`test_the_directory_is_created_owner_only` was given the same treatment
for `CACHE_DIRECTORY_MODE`, which code does read.

`_ensure_directory()` re-stats a directory it has just created and
refuses one whose mode came out looser than it asked for. No test
reached that branch, because a umask cannot produce it -- only a
filesystem granting more than `mkdir()` was asked for can, which is
exactly what the branch is for. The new
`test_a_directory_created_with_the_wrong_mode_refuses` patches
`os.mkdir` to produce a 0755 directory, modelled on its sibling
`test_a_write_which_cannot_verify_the_mode_refuses`, and both the
branch and the mutant are now covered.

### A flaky test this step found and fixed

`LoopbackListenerTestCase.test_a_wrong_state_ends_the_login_rather_than_buying_a_guess`
failed roughly one full-suite run in eight, and nine runs in twelve when
run on its own. The product is not at fault. The test queues two
redirects and asserts that the wrong-state one is the one served, but
both were sent from threads of their own, so which connection the
listener accepted first was a race: when the good redirect won, the
listener correctly handed back its code and the assertion that the login
was refused failed. That matters here beyond the flake, because this
test is one of the four which kill the state-check mutants.

The refused redirect is now connected and written from the test's own
thread, so `connect()` has returned before the second is started and the
accept queue order is settled; its response is read back off the raw
socket afterwards. 30 runs of the test alone and 15 of the whole suite,
all passing.

### Checks

* `.tox/py3/bin/python -m stestr run --no-subunit-trace`: 690 tests
  passing, 0 failing. The baseline was 689; the extra one is the
  directory mode test added above.
* `pre-commit run --all-files`: passed.

Worth noting for a later reader: this repository's pre-commit config
holds actionlint, shellcheck, skillsaw, `tox -eflake8` and `tox -epy3`.
It has no documentation link hook and no plan-number hook; those are the
shakenfist repository's. The constraint against naming plans, phases or
decisions outside a plan file was followed in `docs/human-login.md`
regardless, and the one documentation link it adds was checked by hand.
