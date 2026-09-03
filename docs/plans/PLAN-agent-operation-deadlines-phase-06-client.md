# Agent operation deadlines phase 6: the client

## Prompt

Plan the next phase of shakenfist's `PLAN-agent-operation-deadlines.md`
with the `next-phase` skill, after phase 5 merged as PR #3941. Phases
1 to 5 built the server half: operations carry a wall-clock deadline
and a per-command progress timeout, they expire when either budget
runs out, and a stalled attempt is retried rather than being fatal.
None of that is reachable from this repository yet, because no client
helper sends either parameter and no await loop recognises the
terminal states the server can now produce.

This is the first phase of the plan whose code lands in
`client-python` rather than in `shakenfist`, so the plan file lives
here, alongside the change it plans.

## Planning effort

High, for one reason that is not the code volume. The client half of
this is small and has two worked precedents in this repository to copy
(#352 and #369). What makes it high effort is that sending a new
parameter to an endpoint is a compatibility decision, and the survey
found the server does not currently advertise anything a client can
gate on -- so this phase either crosses back into `shakenfist` for a
capability token or knowingly breaks new clients against old servers.
Decision 2 is where that is settled, and it is the decision most
likely to be argued with.

## Scope

**In scope.**

- `deadline_seconds` derived from the caller's own await budget in the
  three agent-operation creating helpers (`instance_put_blob`,
  `instance_execute`, `instance_get`) and in `_await_agentop`.
- `progress_timeout_seconds` on `instance_put_blob` and
  `instance_get` only -- never on `instance_execute`, which the server
  deliberately does not accept.
- A capability token so a new client does not send either parameter to
  a server that predates them. This is one small commit in
  `shakenfist`; see decision 2.
- `--deadline` on `sf-client instance execute`, `upload` and
  `download`; `--progress-timeout` on `upload` and `download` only.
- Terminal-state fail-fast in every agent-operation await loop, which
  is client-python#363, extended to the `expired` state phase 4
  introduced.
- Repairing the three hardcoded windows in `await_agent_fetch()` that
  make its `timeout` argument a lie (survey finding 5). In scope
  because this phase rewrites those exact loops to derive their budget
  from the caller; see decision 6.
- Unit tests, of which there are none today for any of this
  (survey finding 7).

**Out of scope.**

- The `shakenfist_ci` functional suite's own await loops, and the
  absolute-ceiling work for its instance and agent-state awaits
  (#3770). The master plan puts both in phase 7 and nothing here
  changes that.
- Operator-facing and user-facing documentation of the timing model,
  and the `v07-v08` release note. Phase 7 writes that once, for both
  halves at the same time.
- `await_agent_add_instance_interface()` and the other helpers that
  merely call through to `_await_agent_command()`. They inherit the
  fix without needing their own change.
- Anything about cluster operations. `ClusterOperationFailed` is
  borrowed as a naming precedent (decision 5) and otherwise untouched.

## What the survey found

The master plan's client section (`PLAN-agent-operation-deadlines.md`,
"Client (sibling `client-python` repository)") is accurate in
everything it asserts, and incomplete in one way that matters. Checked
line by line against this tree at `7fba547`:

1. **No helper sends either parameter.** Confirmed:
   `instance_put_blob(instance_ref, blob_uuid, path, mode)`
   (`apiclient.py:1269`), `instance_execute(instance_ref,
   command_line)` (`:1279`) and `instance_get(instance_ref, path)`
   (`:1289`) each build a fixed `data` dict with no room for a budget.

2. **The server takes what the master plan says it takes.** Verified
   in `shakenfist/external_api/instance.py`: put declares both
   parameters (`:1785`), get declares both (`:1870`), and execute
   declares `deadline_seconds` only, with an explicit note at `:1925`
   that it does not accept `progress_timeout_seconds`. The
   asymmetry is deliberate and phase 3 recorded it.

3. **The client has never heard of `expired`.** `grep` for the string
   across `shakenfist_client/` returns nothing. `_is_error_state()`
   (`:177`) exists but is about instances -- it matches `error` and
   the `*-error` transitional states -- and is not consulted by any
   agent-operation loop.

4. **client-python#363 is open**, and its body already anticipates
   this phase: it names `expired` as planned and asks for a distinct
   exception rather than `AgentAwaitTimeout`.

5. **`await_agent_fetch()` ignores its own `timeout`.** Not in the
   master plan, and worse than it looks. The signature is
   `await_agent_fetch(self, instance_uuid, path, timeout=120)`
   (`:1648`); `timeout` is forwarded to `await_agent_ready()` and then
   never used again. The operation wait is `while time.time() -
   start_time < 120`, and the two waits after it are `< 60` -- both
   measured from the same `start_time` as the 120-second wait that
   precedes them. An operation that takes more than 60 seconds
   therefore enters "wait for the operation to have results" with its
   window already expired, so that loop runs zero iterations. It is
   latent rather than constant: results are usually already present by
   the time the state reads `complete`, and the
   `AgentCommandError('operation returned no results')` below only
   fires when they lag it. A slow transfer is exactly when they do.

6. **"The await timeout" is three different numbers.** `_await_agentop`
   (`:1255`) budgets with `_calculate_async_deadline(self.async_strategy)`,
   which returns -1 for `ASYNC_CONTINUE`, 60 for `ASYNC_PAUSE` and
   3600 for `ASYNC_BLOCK` (`:167`). With `ASYNC_CONTINUE` the deadline
   is already in the past when the loop starts, so it checks state once
   and returns. `await_agent_command()` (`:1578`) budgets with its own
   `timeout=120` argument instead. `await_agent_fetch()` has both and
   uses neither (finding 5). A phase that says "pass the await timeout
   as the deadline" has to say which of the three it means, per call
   site. Decision 3 does.

7. **There is no existing test coverage to extend.**
   `shakenfist_client/tests/test_client_apiclient.py` has 74 tests and
   not one of them touches `_await_agentop`, `await_agent_command`,
   `await_agent_fetch`, or any of the three creating helpers. Every
   test this phase needs is a new one.

8. **The compatibility gap the master plan does not cover.** The plan
   says "Old clients keep working: they simply never send the new
   parameters". True, and it says nothing about the other direction.
   New clients against old servers is a real hazard here:
   `log_request` in `shakenfist/external_api/base.py:1231` merges the
   JSON body into handler kwargs with an unconditional
   `kwargs.update(j)`, so a `deadline_seconds` key reaching a handler
   that predates the parameter is an undeclared kwarg, not an ignored
   one. The client's guard for exactly this is
   `check_capability(token)` (`:289`), a substring match against the
   server's root HTML page -- and `API_CAPABILITIES` in
   `shakenfist/external_api/app.py:320-332` has no token for agent
   operation deadlines. Phase 3 added the parameters without
   advertising them. Decision 2 resolves this.

9. **Two worked precedents in this repository**, both merged within
   the last month, and both the same shape as half this phase:
   PR #352 (`await-error-states`, `ebca6f3`) taught the *instance*
   awaits to treat a terminal state as terminal, and PR #369
   (`await-deadline`, `4c9cafe`) gave `create_instance()` a caller
   `timeout` that bounds the whole call including its retries, with
   `timeout=None` preserving the async-strategy default for callers
   that rely on it. Between them they contribute 208 lines of test
   precedent. This phase should read as a third instance of the same
   pattern, not as a new one.

10. **The cross-repository plan convention is already settled.**
    `docs/plans/index.md` in this repository says so in its preamble
    ("most of what lands here is planned in another repository"), and
    `PLAN-vdi-console-tokens.md` is the worked example: the plan of
    record is shakenfist's `PLAN-kerbside-vdi-tokens.md`, whose phase
    table (`:378`) references it as plain text -- "PLAN-...md (in that
    repo, branch `vdi-console-tokens-client`)" -- because a relative
    markdown link cannot cross repositories.

Nothing the master plan asserts turned out to be false, so there are
no corrections to apply at source beyond the two additions recorded
under *Corrections applied at source* below.

The survey was re-run on 2026-09-01 against the then-current
`develop` of both repositories, because the plan sat uncommitted
while phase 5's follow-up fix (PR #3970) and an unrelated week of
merges landed. Every finding above still held; only line numbers had
drifted, and all of the ones this file cites were refreshed in place.
Two are worth naming because their drift was larger than the rest and
a reader who trusts a stale number lands in the wrong function:
`await_agent_command()` moved from `:1507` to `:1578` and
`await_agent_fetch()` from `:1576` to `:1648`.

## Decisions

1. **The plan file, the branch and the PR live in `client-python`.**
   Following finding 10 and the `next-phase` rule that a plan lives
   with the code it plans. The branch is
   `agent-operation-deadlines-phase-06-client`: the VDI precedent used
   the shorter `vdi-console-tokens-client`, but the master plan's
   table references phases by number, and a branch name that carries
   the number is easier to match back to the row.

2. **A capability token is added to `shakenfist`, and the client gates
   on it.** This is the decision to argue with, because it makes a
   "client-python phase" open a second, small PR against the server.
   The alternatives are worse. Sending the parameters unconditionally
   breaks every new client against any server older than phase 3, and
   finding 8 shows the failure is a rejected request rather than a
   silently ignored field. Sniffing the server version instead of a
   capability would be the first place in this client to do so; every
   other feature gate here is a capability token. Doing nothing leaves
   the client unable to set a deadline at all, which is the phase.
   The token is `agentoperation-deadlines`, added to the `instances`
   family in `API_CAPABILITIES`; it costs one line plus its test, and
   it is the piece phase 3 should have shipped. Recorded in the
   master plan's phase 6 row as part of this planning commit so it is
   not mistaken for scope creep at implementation time.

3. **Each call site derives its deadline from the budget it already
   has, and none of them invent one.** Per finding 6: `_await_agentop`
   uses its `_calculate_async_deadline(...)` value, and sends nothing
   when that value is negative (`ASYNC_CONTINUE` -- the caller is not
   waiting, so it has no budget to propagate and the server's default
   is the right one). `await_agent_command` and `await_agent_fetch`
   use their own `timeout` argument. Every creating helper grows
   `deadline_seconds=None` and `progress_timeout_seconds=None` kwargs
   which override, so a caller that knows better always wins. `None`
   means "send nothing", matching #369's treatment of `timeout=None`.

4. **`--progress-timeout` goes on `upload` and `download` only.**
   `execute` gets `--deadline` alone. This follows the server
   (finding 2) rather than offering a flag the API would refuse, and
   it is the CLI-visible half of phase 3's decision that `execute`
   must not publish a progress timeout it cannot honour.

5. **A terminal state raises `AgentOperationFailed`, not
   `AgentAwaitTimeout`.** New exception, mirroring the existing
   `ClusterOperationFailed(message, op_type, op_uuid, op_view)` at
   `:129` and dropping the `op_type` it does not need. Reusing
   `AgentAwaitTimeout` would keep #363's real complaint -- that a
   definitive failure is reported as a timeout -- while fixing only
   the wasted wait. Terminal is read from a module-level frozenset
   `TERMINAL_AGENT_OPERATION_STATES = frozenset({'complete', 'error',
   'expired', 'deleted'})` rather than an `!= 'complete'` test, so the
   next state the server adds is one edit. Those four are exactly
   `AgentOperation.TERMINAL_STATES` in
   `shakenfist/operations/agentoperation.py:41`, checked rather than
   guessed; the client cannot import it, so the duplication is
   deliberate and the constant's comment should say where the
   authority lives.

6. **`await_agent_fetch()`'s hardcoded windows are repaired here.**
   Finding 5 is a pre-existing bug and the `next-phase` skill's
   default is to file it rather than fix it. Fixing it anyway,
   because this phase rewrites those three loops to fail fast and to
   derive a deadline from `timeout`, and leaving `< 120` and `< 60`
   literals inside loops whose budget now comes from the caller would
   ship an incoherent function. It is one step, called out separately
   in the step plan so it can be dropped without disturbing the rest.

7. **When the capability is absent, behaviour is exactly today's.**
   No parameters sent, no warning, no exception -- the client still
   works against an old server, it simply cannot bound the operation.
   The fail-fast change (decision 5) is *not* gated: recognising a
   terminal state the server has always been able to produce is
   correct against every server version, and `expired` from an old
   server simply never arrives.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 6a | low | sonnet | none | The server-side capability token, in the `shakenfist` repository on its own branch and its own PR -- this is the only step that does not touch `client-python`. In `shakenfist/external_api/app.py`, add `'agentoperation-deadlines'` to the `'instances'` list in `API_CAPABILITIES` (`:320-325`), placing it after `'instance-clusteroperations'`. The token names the phase 3 parameters as a set: `deadline_seconds` on put/execute/get and `progress_timeout_seconds` on put/get. Check whether `shakenfist/tests/` has a test asserting the capability list's contents and extend it if so; if not, do not invent one, because `render_capabilities()` has no logic to test. Do not touch the endpoints themselves -- the parameters already exist and are already declared. Commit subject: `Advertise the agent operation deadline parameters.` |
| 6b | medium | sonnet | none | The exception, the terminal-state set, and the fail-fast rewrite of the three agent-operation await loops. Nothing about deadlines yet. In `shakenfist_client/apiclient.py`: add `TERMINAL_AGENT_OPERATION_STATES = frozenset({'complete', 'error', 'expired', 'deleted'})` as a module constant near `_is_error_state()` (`:177`), and `AgentOperationFailed(message, op_uuid, op_view=None)` immediately after `AgentCommandError` (`:125`), mirroring `ClusterOperationFailed` (`:129-141`) minus its `op_type`. Then rewrite three loops to break on any member of the set and raise `AgentOperationFailed` for the members that are not `complete`: `_await_agentop` (`:1255`), the operation wait in `await_agent_command` (`:1585-1590`), and the operation wait in `await_agent_fetch` (`:1653-1658`). Keep `AgentAwaitTimeout` for the case it actually describes -- the budget ran out with the operation still in flight. The exception message must name the state and the operation uuid; `await_agent_command`'s existing timeout message gathers console data for the report, and the new failure path should do the same, since a failed agent command is exactly when an operator wants it. Tests in `shakenfist_client/tests/test_client_apiclient.py`, which has no coverage of these helpers at all: a `complete` operation returns normally; each of `error`, `expired` and `deleted` raises `AgentOperationFailed` on the first poll rather than after the timeout (assert the mocked clock or the poll count, not the wall time); and a never-settling operation still raises `AgentAwaitTimeout`. Read the tests PR #352 (`ebca6f3`) added for the instance awaits first and follow their shape. Commit subject: `Fail fast on terminal agent operation states.` |
| 6c | medium | sonnet | none | Propagate the deadline. In `shakenfist_client/apiclient.py`, add `deadline_seconds=None` and `progress_timeout_seconds=None` kwargs to `instance_put_blob` (`:1269`) and `instance_get` (`:1289`), and `deadline_seconds=None` alone to `instance_execute` (`:1279`) -- the server refuses a progress timeout on execute (see decision 4 and `shakenfist/external_api/instance.py:1925`). Each helper adds the keys to its `data` dict only when the value is not None **and** `self.check_capability('agentoperation-deadlines')` is true (decision 7; `check_capability` is at `:289`). Then wire the defaults per decision 3: `_await_agentop` (`:1255`) passes its `_calculate_async_deadline(self.async_strategy)` value, but only when it is positive -- `ASYNC_CONTINUE` returns -1 and means the caller is not waiting; `await_agent_command` (`:1578`) and `await_agent_fetch` (`:1648`) pass their own `timeout`. Since `_await_agentop` is called by the creating helpers rather than the other way round, the deadline has to be computed by the caller of the helper, not inside `_await_agentop`: read the three call sites at `:1277`, `:1287` and `:1297` before deciding where the arithmetic goes, and say in a comment why. An explicit kwarg always wins over the derived value. Tests: each helper sends the parameters when the capability is present, sends neither when it is absent, and an explicit kwarg overrides the derived value; `instance_execute` never sends `progress_timeout_seconds` even when asked. Commit subject: `Send agent operation deadlines from the client.` |
| 6d | low | sonnet | none | Repair `await_agent_fetch()`'s budget (decision 6, survey finding 5). In `shakenfist_client/apiclient.py:1648`, the three loops use the literals `120`, `60` and `60` while the signature offers `timeout=120`; all three measure from one `start_time`, so the two 60-second windows are already expired whenever the operation itself took longer than a minute, and the `AgentCommandError('operation returned no results')` below then fires whenever the results lag the state, which is exactly what a slow transfer looks like. Make every loop bound itself by `timeout` from the same `start_time`, and make the timeout message quote the real budget rather than the string `120 seconds`. Do not change the signature or the default. Test that a fetch whose operation completes after 90 seconds of mocked time still reaches its results, which fails against the current code. Commit subject: `Honour the fetch timeout it was given.` |
| 6e | medium | sonnet | none | The CLI surface, in `shakenfist_client/commandline/instance.py`. Add `--deadline` to `instance execute` (`:830`), `instance upload` (`:803`) and `instance download` (`:864`), and `--progress-timeout` to `upload` and `download` only (decision 4). Both are `click.option(..., type=click.INT, default=None)` with help text saying that the value is in seconds, that 0 disables that budget, and that omitting it uses the server default. Pass them through to the three apiclient helpers as the kwargs step 6c added. Note `instance upload` calls `instance_put_blob` at `:822` after an artifact upload that may itself take a long time -- the deadline applies to the agent operation only, and the help text should not imply it covers the upload. Check `shakenfist_client/tests/test_client_commandline_instance.py` for the idiom used to test existing options and follow it: each flag reaches the client method, and omitting it passes None. Commit subject: `Add deadline flags to the agent CLI verbs.` |
| 6f | low | sonnet | none | Closeout. In this repository, set the phase 6 row of `docs/plans/index.md` to `Complete`. In `shakenfist`, set the phase 6 row of `docs/plans/PLAN-agent-operation-deadlines.md`'s phase table to `Complete` and move `docs/plans/index.md`'s plan row from `6 of 9` to `7 of 9`, leaving its status `In progress`. That is two repositories and therefore two commits; the shakenfist one can ride along with step 6a's PR if that has not merged yet, and otherwise needs its own. Write no user-facing documentation: phase 7 writes the timing story once, for both halves, and a CLI flag documented here would be the second place it is described. Commit subject: `Close out the agent operation deadline client work.` |

## Corrections applied at source

Made as part of this planning commit, so a later step does not redo
them:

- The master plan's *Client* section gains a sentence recording that
  the new parameters need a capability token before a client can send
  them safely, and that phase 6 adds it (survey finding 8). The
  section's compatibility paragraph covers old clients only.
- The master plan's phase 6 row and `docs/plans/index.md` row in
  `shakenfist` both gain a note that the plan file lives in
  `client-python`, referenced as plain text for the reason finding 10
  gives.

## Corrections applied in review

The automated review of the phase 6 pull request
(client-python#380) found that decision 3 was wrong, and that
correcting it made several smaller things wrong too. All of the
following are applied on the branch:

- **Decision 3 is reversed for the derived case.** Deriving a
  deadline from the async strategy conflated "how long this client
  will wait" with "how long the operation may live". Those are not
  the same number, and the CLI defaults to `pause`, so *every*
  `sf-client instance execute`, `upload` and `download` acquired a 60
  second server side hard deadline where previously the server's own
  default (`AGENT_OPERATION_DEFAULT_DEADLINE`, 600 seconds) applied.
  An upload into a guest would have started dying at a minute.
  Nothing is derived now: a value is sent only when a caller passed
  one, and omitting it means the server default, which is what the
  CLI help text already claimed. `_derive_agentop_deadline()` is
  gone; `_add_agentop_timing()` replaces it.
- **`await_agent_command` and `await_agent_fetch` send the budget
  that is left.** Both take `start_time` *before*
  `await_agent_ready()` and share it with every loop below, so
  sending the full `timeout` as the deadline kept the operation alive
  on the server long after the client stopped watching it. They now
  send `max(1, round(timeout - elapsed))`. The floor of one second
  exists because the server reads a deadline of `0` as "no wall clock
  deadline at all", which is the opposite of an exhausted budget.
- **`_await_agentop` gains `await_seconds`.** Without it,
  `await_agent_command(timeout=120)` could still block for the hour
  `ASYNC_BLOCK` allows inside `instance_execute`, before reaching its
  own already-expired loop. The three budgets stay three numbers --
  this is the third, and it is the one the caller controls.
- **The console data enrichment is reachable.** `_await_agentop`
  raises `AgentOperationFailed` as soon as it polls a terminal
  failure state, which is the common case, so the enriched raise in
  `await_agent_command` only ever fired in the narrow window where
  the state turned terminal after the poll gave up. Both helpers now
  catch and re-raise through `_enriched_agent_failure()`, and a test
  exercises the real `instance_execute` chain rather than mocking it.
- **Decision 7 gains a warning for the explicit case.** Silence is
  right when nothing was asked for, since an omitted flag and an old
  server produce the same behaviour. A typed `--deadline` is a
  request, so the CLI now says on stderr that the server cannot
  accept it. The library stays silent, because `await_agent_command`
  passes a deadline on every call and must keep working against an
  old server.
- **The dead `if not op['results']` guard in `await_agent_command`
  moved above the subscripts it protects.** It was unreachable, so an
  empty results dict raised `KeyError`. Same class of bug as decision
  6 and three lines from code this phase already touched.
- **The clock-mocking tests use an advancing fake clock.** A fixed
  `side_effect` list breaks with `StopIteration` on any added
  `time.time()` call, and the slow-fetch test's claim about the
  pre-fix behaviour was untrue as written for that reason.

Every one of these is covered by a mutation: breaking the property on
purpose fails a test with a message that names it.

## Corrections applied in the second review round

The second automated review of client-python#380 read the corrected
tree. Everything it found follows from the first round's corrections
rather than contradicting them:

- **`AgentOperationFailed` reaches the CLI's exception handler.** The
  three agent verbs call the creating helpers directly, and those now
  raise on a terminal failure instead of returning an in flight
  operation. `GroupCatchExceptions` did not know the class, so the
  user got a traceback -- and once the server side deadlines deploy,
  `expired` makes that the *common* outcome rather than an edge case.
- **`_enriched_agent_failure()` never eats the failure it decorates.**
  `get_instance()` and `get_console_data()` fail for exactly the
  reasons the operation did: a `deleted` operation is usually an
  instance which went away underneath it. Both lookups are now best
  effort, and a failed lookup still yields an `AgentOperationFailed`
  carrying the operation uuid and state. The `AgentAwaitTimeout` report
  a few lines below had the identical shape and the identical fix, so
  both now share `_agent_failure_context()` rather than one of them
  being right.
- **The outer "wait for the operation to be complete" loops are
  gone.** Once `_await_agentop()` is given the caller's own budget it
  returns either a complete operation or an in flight one whose
  deadline passed, and raises for everything terminal in between. The
  loop below it was therefore always entered already expired, and the
  terminal state check below *that* could never see a terminal state.
  `_await_agentop()` is now the only place which polls operation
  state. Two tests reached those branches only by mocking
  `instance_execute`/`instance_get` to return a state the real chain
  can no longer produce; they now drive the real chain.
- **`instance upload` warns before it transfers anything.** The
  capability check needs nothing the upload produces, so warning
  afterwards told the user their `--deadline` could not be honoured
  only once a multi-gigabyte file had crossed the network.
- **Fire and forget does not mean fire and ignore.** `ASYNC_CONTINUE`
  raises for a terminal failure like every other strategy, because an
  operation which is dead in the POST response cannot usefully be
  handed back to poll later. That was already the behaviour; the
  docstring now says so and a test pins it.
- **`_warn_if_timing_unsupported()` takes a plain dict.** It was
  carrying `--deadline` and `--progress-timeout` through `**kwargs`,
  which CPython allows but which makes a reader stop and check.
- **Test gaps closed.** `await_seconds` is asserted to be forwarded by
  each of the three helpers (a helper which dropped it would have
  passed the whole suite while restoring the ASYNC_BLOCK bug); the
  upload CLI tests now vary `blob-search-by-hash` and
  `agentoperation-deadlines` independently, so the capable path and
  the blob-recycling path are both covered.

The remaining review items were declined with reasons rather than
applied. The one second floor on an exhausted budget stays, because an
operation nobody is waiting for should be reaped rather than left
running under the server's default -- the docstring now says that in
those terms. The capability substring match is pre-existing and fails
closed. There is still no user-facing document, because this plan's
"Out of scope" section gives that to phase 7 deliberately, and
`AGENTS.md` now says so rather than leaving the absence to read as an
oversight.

## Risks and mitigations

- **The capability token lands after the client change.** If 6c
  merges before 6a is deployed, a client built from `develop` sends
  nothing, because `check_capability` fails closed. That is the
  designed behaviour and not a risk to the user; the risk is to the
  *tests*, which must not assume the token exists. Mitigation: 6c's
  tests set the capability explicitly in both directions, and 6a is
  listed first so the review order matches the deploy order.

- **`_await_agentop`'s deadline arithmetic lands in the wrong place.**
  The helper is called by the three creating methods *after* the POST
  has already been sent, so a deadline computed inside it would be
  computed too late to travel with the request. Mitigation: step 6c's
  brief says to read all three call sites first and to record the
  reasoning in a comment; the reviewer checks that the value sent is
  the budget the caller will actually apply, not one derived after the
  fact.

- **Fail-fast changes an exception type callers may catch.** Anything
  catching `AgentAwaitTimeout` to mean "the operation did not
  succeed" now sees `AgentOperationFailed` for the failure cases.
  Mitigation: both derive from `Exception` rather than from each
  other, so this is a real behaviour change and is deliberate -- it is
  what #363 asks for. The in-tree consumers are the CI suite (phase 7,
  which will be updated with it) and `client-python-k3s`; the step 6b
  reviewer greps both for `AgentAwaitTimeout` and reports what it
  finds rather than changing them here.

- **The CI suite's awaits are not covered by this phase and will
  still spin.** They are phase 7's, and phase 7 is now the only thing
  standing between the server-side work and the CI flake this whole
  plan exists to fix. Mitigation: none needed within this phase, but
  the phase 7 planner should read this as the reason not to defer it
  further.

## Definition of done

Runnable from the repository root of `client-python` unless stated
otherwise. The python checks need the project importable, so run them
with `.tox/py3/bin/python`.

```sh
# 1. Terminal states are a set, and every state the server can reach
#    is in it.
.tox/py3/bin/python - <<'EOF'
from shakenfist_client import apiclient

for s in ('complete', 'error', 'expired', 'deleted'):
    assert s in apiclient.TERMINAL_AGENT_OPERATION_STATES, s
assert 'executing' not in apiclient.TERMINAL_AGENT_OPERATION_STATES
assert 'queued' not in apiclient.TERMINAL_AGENT_OPERATION_STATES
print('terminal states ok')
EOF

# 2. The failure exception exists and carries what a caller needs.
.tox/py3/bin/python - <<'EOF'
from shakenfist_client import apiclient

e = apiclient.AgentOperationFailed('boom', 'an-op-uuid', {'state': 'expired'})
assert e.op_uuid == 'an-op-uuid'
assert e.op_view['state'] == 'expired'
assert not isinstance(e, apiclient.AgentAwaitTimeout)
print('exception ok')
EOF

# 3. Every creating helper accepts the parameters the server accepts,
#    and execute accepts no progress timeout.
.tox/py3/bin/python - <<'EOF'
import inspect
from shakenfist_client import apiclient

def params(name):
    return set(inspect.signature(getattr(apiclient.Client, name)).parameters)

for name in ('instance_put_blob', 'instance_get'):
    assert 'deadline_seconds' in params(name), name
    assert 'progress_timeout_seconds' in params(name), name
assert 'deadline_seconds' in params('instance_execute')
assert 'progress_timeout_seconds' not in params('instance_execute')
print('signatures ok')
EOF

# 4. No await loop tests a bare equality against 'complete' any more.
#    Three loops were rewritten; none may be left behind.
test 0 -eq "$(grep -c "state'\] == 'complete'" shakenfist_client/apiclient.py)" \
    && echo 'no bare complete comparisons'

# 5. await_agent_fetch has no hardcoded budget left.
test 0 -eq "$(sed -n '/def await_agent_fetch/,/def await_agent_add/p' \
    shakenfist_client/apiclient.py | grep -c 'start_time < \(120\|60\)')" \
    && echo 'fetch budget honoured'

# 6. The CLI offers the flags the server backs, and no others.
#    There is no module entry point -- pyproject declares the console
#    script as shakenfist_client.main:cli -- so drive it through click's
#    own test runner rather than a subprocess.
.tox/py3/bin/python - <<'EOF'
from click.testing import CliRunner

from shakenfist_client.main import cli

def options(*argv):
    r = CliRunner().invoke(cli, list(argv) + ['--help'])
    assert r.exit_code == 0, (argv, r.output)
    return r.output

assert '--deadline' in options('instance', 'execute')
assert '--progress-timeout' not in options('instance', 'execute')
for verb in ('upload', 'download'):
    assert '--deadline' in options('instance', verb), verb
    assert '--progress-timeout' in options('instance', verb), verb
print('cli flags ok')
EOF

# 7. The suite passes and the new coverage exists.
tox -epy3
test 0 -lt "$(grep -c 'AgentOperationFailed' \
    shakenfist_client/tests/test_client_apiclient.py)" \
    && echo 'fail-fast is tested'
```

And, in the `shakenfist` repository:

```sh
# 8. The capability the client gates on is actually advertised.
grep -q "'agentoperation-deadlines'" shakenfist/external_api/app.py \
    && echo 'capability advertised'
```

Two things this list deliberately does not assert, because they are
not falsifiable as written and phase 7 owns them: that the timing
model is documented, and that the CI suite fails fast.

## Future work

- **The three await budgets stay three numbers.** Decision 3 makes
  each call site propagate the budget it has rather than unifying
  them, because unifying them changes the behaviour of callers that
  rely on the async strategy. A later change could give the client one
  await-budget concept; it should be its own piece of work with its
  own compatibility argument, not a rider on this one.

- **`_calculate_async_deadline(ASYNC_CONTINUE)` returns -1, which
  every caller treats as a deadline in the past.** It works, and it
  reads like a bug at every call site that has to special-case it.
  Worth replacing with `None` and an explicit "no deadline" branch,
  across the whole client rather than in the two places this phase
  touches.

- **`await_agent_command` and `await_agent_fetch` duplicate a
  poll-until-settled loop that `_await_agentop` also implements.**
  After this phase all three will implement the same terminal-state
  rule, in three places. Collapsing them is an obvious tidy-up and is
  out of scope here only because doing it while also changing what
  they do would make the diff impossible to review.

## Back brief

Before starting, the implementing session should state back:

1. Which repository each step's commit lands in, and that step 6a is a
   separate PR against `shakenfist` rather than part of this branch.
2. What happens when `check_capability('agentoperation-deadlines')` is
   false, in one sentence, and why that is not gated for the
   fail-fast change.
3. Which of the three await budgets each modified call site sends, and
   why `ASYNC_CONTINUE` sends nothing.

**Gate.** Do not start step 6c until decision 2 has been confirmed by
the operator. It is the decision that turns a single-repository phase
into a two-repository one, and reversing it after 6c is written means
rewriting every gated call site and its tests. Steps 6a, 6b and 6d
carry no such risk and can proceed.
