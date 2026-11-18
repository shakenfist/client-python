# Dependency-aware agent operations phase 3: the client

## Prompt

Plan the next phase of shakenfist's
[PLAN-agent-operation-dependencies.md](https://github.com/shakenfist/shakenfist/blob/develop/docs/plans/PLAN-agent-operation-dependencies.md)
with the `next-phase` skill, after phase 2 merged as
shakenfist#4909 (`e5354766a4`). Phases 1 and 2 built the server
half: an agent operation carries `depends_on` and `runs_after` edge
lists, three endpoints accept them behind the
`agentoperation-dependencies` capability token, and the sidechannel
dispatcher evaluates them between `agent_operation_next()` and
`start_instance_executor()`, cascading a dependent whose
`depends_on` dependency failed to the new `abort` terminal state
with the cause in `dependency_failure`.

None of that is reachable from this repository. No helper sends
either parameter, and `TERMINAL_AGENT_OPERATION_STATES` does not
hold `abort`, so an `abort` the server can now emit is a state this
client does not recognise.

This is the second phase of the plan whose code lands outside
`shakenfist`, so the plan file lives here, alongside the change it
plans, following the
[phase 6 precedent](PLAN-agent-operation-deadlines-phase-06-client.md)
of the deadlines plan this one builds on. Unlike that phase, this
one is genuinely two-repository work: see decision 1.

## Planning effort

High, and the review effort the master plan asks for is high as
well. The code is small -- one frozenset entry, two keyword
arguments on three methods, two helpers and a functional test --
but three things make it high effort rather than medium:

1. There is a single-commit ordering obligation carried down from
   phase 0 and restated by phase 2, and getting it wrong creates a
   caller-visible hang rather than a test failure.
2. Sending an ordering edge is a compatibility decision with a
   failure mode the capability token does not cover: the server's
   own comment records that a new `sf-api` against an older
   `sf-database` answers 200 and stores **empty edge lists**. A
   client which assumes the token is sufficient publishes silent
   unordered execution during a staged upgrade. Decision 5 is where
   that is settled.
3. The master plan's phase 3 row asks for "chain submission
   helpers", and the survey found the surface those would be built
   on is not what the row assumes. Decision 6 declines part of the
   row and says why.

## Scope

**In scope, in this repository.**

- `abort` added to `TERMINAL_AGENT_OPERATION_STATES`
  (`apiclient.py:234`), in its own commit, first.
- `depends_on` and `runs_after` keyword arguments on
  `instance_put_blob`, `instance_execute` and `instance_get`,
  gated on the `agentoperation-dependencies` capability.
- An edge constructor, so a caller writes an edge rather than a
  dict literal, and client-side shape validation of what it
  builds (decision 4).
- Verification that the edges a creating call sent came back on
  the operation it created (decision 5).
- `await_agent_chain()`: await a submitted chain and, on an
  `abort`, walk `dependency_failure` back to the operation that
  actually failed, so the exception names the cause rather than
  the last link (decision 7).
- Unit tests, in `AgentOperationAwaitTestCase` and a new case
  for the chain walk.
- The `Agent Operation Dependencies` invariants section in
  `AGENTS.md`, beside the existing `Agent Operation Deadlines`
  one. This is a convention change -- the hand-duplicated
  frozenset and the two-repository ordering obligation are not
  inferrable from the code -- so it belongs there rather than in
  `docs/`.

**In scope, in `shakenfist`.**

- The first functional test of a real dependency chain against a
  live cluster, which the master plan assigns to this phase and
  phase 2's first review round declined for want of a client that
  could submit an edge. New file
  `shakenfist/deploy/shakenfist_ci/smoke_ci_tests/test_agentop_dependencies.py`.
- `abort` added to `base.BaseNamespacedTestCase.AGENT_OPERATION_FAILED_STATES`
  (`base.py:1678`), which is a second copy of the same list with
  the same omission and which the master plan does not mention.
  Survey finding 4.
- Phase 2's close-out and this phase's registration in the master
  plan's Execution table and `docs/plans/index.md`.

**Out of scope.**

- `docs/developer_guide/agent_operations.md`, the API reference
  vocabulary, the lazy-cascade and swept-dependency ambiguities,
  and the example index. All of that is phase 4 (decision 11 of
  the master plan), and phase 2's definition of done asserts the
  page does not exist yet.
- `client-python-k3s` adopting dependencies. Phase 4. It already
  knows `abort` (its #164, released in v0.4.0), which was phase
  1's prerequisite and is not revisited here.
- CLI flags for `--depends-on` and `--runs-after`. Declined with a
  reason and an issue; see decision 8.
- client-python#389 (no `state` property on
  `AgentOperationFailed`, unvalidated `--deadline`, stale
  `AGENTS.md` forward reference), #390 (post-await loops re-spend
  the caller's clock) and #404 (no per-call `async_strategy`).
  Each touches code this phase edits and none is this phase's
  defect; #404 in particular is what makes the submission idiom in
  decision 6 awkward from inside a plugin, and is the right place
  to fix that.
- Mixed-type edges between agent and cluster operations
  (shakenfist#4824), declined by decision 4 of the master plan.
- A submitter identity on an agent operation, which the master
  plan's Future work records.

## What the survey found

The master plan's phase 3 row was written during phase 2 and is
accurate in its substance. Two of its specifics are wrong, and the
survey found two things the row does not mention. Checked against
`client-python` at `63571d7` and `shakenfist` at `8a68d42ace`.

1. **The frozenset is where the row says it is, and holds what the
   row says it holds.** `TERMINAL_AGENT_OPERATION_STATES =
   frozenset({'complete', 'error', 'expired', 'deleted'})` at
   `shakenfist_client/apiclient.py:234`, with a comment already
   saying it hand-duplicates `AgentOperation.TERMINAL_STATES` and
   must be kept in step. The server side now holds
   `STATE_ABORT` (`shakenfist/operations/agentoperation.py:73-75`),
   so the two are out of step today.

2. **The await loop is at `:1523`, not `:1443`, and it does not
   spin forever.** `_await_agentop()` begins at `:1498` and its
   state test is at `:1523`. The row's "would spin on an aborted
   operation ... and then hang awaiting it forever" overstates it:
   the loop is bounded by `await_seconds` where the caller passed
   one, and otherwise by `_calculate_async_deadline()` (`:213`),
   which is -1 for `ASYNC_CONTINUE`, 60 for `ASYNC_PAUSE` and 3600
   for `ASYNC_BLOCK`. The real defect is worse in one way and
   better in another: an aborted operation is not recognised as a
   failure, so the loop polls to its deadline and then **returns
   the aborted operation with no exception**, exactly as it returns
   one still in flight. `await_agent_command()` (`:2094`) then
   raises `AgentAwaitTimeout` -- "did not complete within specified
   timeout" -- about an operation which will never run and whose
   timeout was never the problem. A caller is misinformed rather
   than hung. The single-commit obligation is unchanged: the window
   the row describes is real, it just produces a wrong answer
   instead of an unbounded wait.

3. **The row's reason the window cannot be reached today is
   correct.** `instance_put_blob` (`:1631`), `instance_execute`
   (`:1646`) and `instance_get` (`:1663`) each take explicit named
   parameters and build a fixed `data` dict. There is no
   `**kwargs` passthrough and no caller-supplied body, so no
   caller can create an operation that reaches `abort`.

4. **There is a second copy of the list, in `shakenfist`, with
   the same omission, and the master plan does not mention it.**
   `base.BaseNamespacedTestCase.AGENT_OPERATION_FAILED_STATES =
   ('error', 'expired', 'deleted')` at
   `shakenfist/deploy/shakenfist_ci/base.py:1678`, read by
   `_await_command()` (`:1685`), by `_await_agentop_complete()`
   (`:2048`) and by this phase's own
   `_await_agentop_state()` helper precedent in
   `test_agentop_deadlines.py:206`. An aborted operation is not in
   it, so every one of those waits would burn its whole window on
   an operation that is already finished. It is unreachable today
   for the same reason the client's copy is -- no suite test can
   submit an edge -- and the first test that can is the one this
   phase adds, in the same pull request, which is why fixing it on
   the second of this phase's two pull requests is safe. A third
   copy exists in `client-python-k3s`
   (`shakenfist_client_k3s/cluster.py:138`) and **already holds
   `abort`**, so the ecosystem sweep found exactly the two to fix.

5. **Every agent-operation await in this repository funnels
   through the one frozenset, so the one-line change covers all of
   them.** `AGENTS.md` already records this as an invariant:
   "`_await_agentop()` is the only place which polls an agent
   operation's state. `await_agent_command()` and
   `await_agent_fetch()` hand it their own remaining budget and
   then catch `AgentOperationFailed`". Confirmed by grep: the
   frozenset has exactly one reader, `:1523`. This is why the
   `abort` commit is genuinely one line of behaviour change and
   not a sweep.

6. **A non-awaiting submission needs no new code path.**
   `_await_agentop()` tests `if await_seconds is not None`, so
   `await_seconds=0` sets a deadline already reached: the loop
   checks the state once, raises if it is already a terminal
   failure, and otherwise returns the operation as the POST
   described it. That is precisely the semantics chain submission
   needs, and it is documented behaviour rather than an accident
   -- the `ASYNC_CONTINUE` path relies on the same mechanism.
   Decision 6 rests on this.

7. **The POST response is the created operation's
   `external_view()`, which carries `depends_on` and
   `runs_after`.** `shakenfist/external_api/instance.py:2445`
   returns `o.external_view()`, and the view
   (`operations/agentoperation.py:281-294`) carries both edge lists
   plus `dependency_failure`. So a client can check that the edges
   it sent were stored, which decision 5 needs.

8. **The published edge shape is `{op_uuid, settle_seconds?}` and
   nothing else.** `agent_dependency` in
   `shakenfist/schema/agentoperation_dependency.py` sets
   `model_config = ConfigDict(extra='forbid')` and carries
   `op_type` as a stored-but-not-published constant, because "a
   caller who had to supply a constant could only ever get it
   wrong". The API's parameter type is `arrayofagentdependency`
   with `maxItems: MAX_AGENT_DEPENDENCIES` (64,
   `external_api/base.py:626`), and `settle_seconds` has
   `maximum: config.AGENT_OPERATION_DEPENDENCY_WAIT`
   (`base.py:1142`), which phase 2 lowered to 300.

9. **`dependency_failure` can name a dependency which has not
   failed.** The cascade writes `{op_type, op_uuid, state}`
   (`agentoperation.py:602-606`), and the dispatcher aborts for two
   reasons which share the field: a dependency in a failed state
   (`CASCADE_EVENT_FAILED`) and a wait or settle that outran the
   budget (`CASCADE_EVENT_GAVE_UP`), where the recorded `state` may
   be one of `DEPENDENCY_PENDING_STATES`
   (`daemons/sidechannel/main.py:175-178`). The field can also hold
   two sentinels which are not states at all: `'missing'` and
   `'unreadable'` (`:181`, `:194`). A chain walk which assumes the
   named dependency failed is therefore wrong in three ways, which
   decision 7 handles.

10. **The CLI has nothing an ordering edge could attach to.**
    `sf-client instance execute` (`commandline/instance.py:872`)
    prints the results and never the operation UUID, and exits 1
    with "Results not available." when `'0' not in
    op.get('results', {})` -- which is exactly what a freshly
    queued dependent returns. `download` (`:912`) does the same.
    Decision 8 is why this phase adds no flags.

11. **`instance show --agentoperations` already carries the new
    fields.** `_show_instance()` assigns the whole agent operation
    views (`out['agent_operations'] = agentops`) with no field
    allowlist, so `depends_on`, `runs_after` and
    `dependency_failure` print without a change here.

**Corrections applied at source.** Findings 2 and 4 are wrong or
missing in the master plan's phase 3 row, and findings 2, 4, 6, 9
and 10 shape what this phase does. The master plan's row and the
`docs/plans/index.md` row in `shakenfist` are corrected in the
close-out commit on this phase's `shakenfist` branch, so a later
step does not need to redo it.

## Decisions

1. **This phase is two pull requests across two repositories, the
   client one first, and the plan file lives here.** The master
   plan's row says the phase "lands in the sibling repository, so
   its plan file lives there", which the survey found to be
   two-thirds true: the functional test the same row assigns to
   this phase can only live in `shakenfist`, because that is where
   the suite is
   (`shakenfist/deploy/shakenfist_ci/smoke_ci_tests/`), and
   `shakenfist/actions`'s `smoke-cluster.yml` runs the suite from
   the `shakenfist` checkout.

   The ordering is forced rather than chosen.
   `setup-test-environment/action.yml:31-36` checks out
   `shakenfist/client-python` with no `ref`, so it takes that
   repository's default branch for every run the client did not
   trigger. A functional test in `shakenfist` which calls a new
   client helper therefore fails on `shakenfist`'s own CI until the
   helper is on `client-python`'s `develop`. The reverse direction
   works: a `client-python` pull request runs its own client
   against the `shakenfist` suite at `develop`.

   This is not a new shape. The deadlines plan's phase 6 landed
   three pull requests across the same two repositories, and its
   `Merged` cell records each: `client-python 4557100ce (#380);
   shakenfist 4afa29476 (#4005), 864608276 (#4015)`. This phase's
   cell takes the same form.

   The alternative -- deferring the functional test to phase 4,
   which is in `shakenfist` already -- was rejected. Phase 2
   declined the test once; deferring it again on a technicality
   about repository boundaries is how a test gets declined a third
   time, and phase 4's scope (a developer-guide page, the API
   reference vocabulary, the example index and the `k3s`
   adoption) is large enough to absorb it without anyone noticing.

2. **`abort` enters the frozenset in the first commit of the
   client pull request, alone.** No parameters, no helpers, no
   CLI, nothing else in that commit. This is the obligation phase
   0 found between `TERMINAL_STATES` and `state_targets`, restated
   by phase 2 for the client: adding the submission parameters
   first opens a window in which a caller can create an operation
   which aborts, and then be told by `await_agent_command()` that
   it timed out (survey finding 2).

   Keeping it alone, rather than merely first, is deliberate. A
   one-line commit with its own test is a commit a reviewer can
   check the ordering of at a glance, and it is revertable without
   taking the parameters with it.

3. **The capability gate fails closed by raising, not by sending
   nothing.** This is a deliberate divergence from the
   `agentoperation-deadlines` precedent, where
   `_add_agentop_timing()` silently sends neither timing parameter
   to a server which does not advertise the token, and the CLI
   warns only when the user typed a flag.

   The two cases are not alike. An omitted deadline means "the
   server's own default applies", which is what an old server
   would have given anyway -- so dropping it costs the caller a
   budget and nothing else. A dropped ordering edge means "run
   now", which is the precise outcome the caller declared the edge
   to prevent: a `put` and the `execute` that reads the file it
   placed would both be dispatched against an instance whose
   executor takes them in queue order with no guarantee the first
   finished. Silence there is worse than a refusal.

   So `instance_put_blob`, `instance_execute` and `instance_get`
   raise `IncapableException` when the caller passed either edge
   list and `check_capability('agentoperation-dependencies')` is
   false, with a message naming the capability -- the same shape
   those three methods already use for their own
   `instance-put-blob` / `instance-execute` / `instance-get`
   checks. A caller which passed neither list is unaffected and
   never consults the token.

4. **The client validates an edge's shape and not the
   deployment's policy.** The helper accepts either a UUID string
   or a mapping. A string must parse as a UUID; a mapping's keys
   must be a subset of `{'op_uuid', 'settle_seconds'}`, mirroring
   the server model's `extra='forbid'`, and `op_uuid` must be
   present and parse. Anything else raises `ValueError` before a
   request goes out.

   What the client does **not** check is `MAX_AGENT_DEPENDENCIES`
   (64) or the `settle_seconds` ceiling. Both are deployment
   policy read from config on the server, phase 2 moved the
   ceiling from 86400 to 300 in a single release, and a client
   carrying a copy would refuse a value a differently configured
   cluster accepts -- or, worse, accept one a stricter cluster
   refuses, having told the caller it was fine. This is the same
   division the server's own schema draws, and for the same stated
   reason: `agentoperation_dependency.py:115-121` declines to
   enforce the ceiling in the schema because "it is a deployment
   policy value", and checks it in the API helper instead.

   The edge constructor is a module-level
   `agent_dependency(operation, settle_seconds=None)` which takes
   an operation view (a dict with a `uuid`) or a UUID string, so
   the common case reads
   `depends_on=[agent_dependency(first_op)]` rather than
   `depends_on=[{'op_uuid': first_op['uuid']}]`. It does not emit
   `op_type`: the server stores that constant and does not publish
   it, and sending it would be refused by `extra='forbid'` at the
   parameter.

5. **A creating call verifies that the edges it sent came back,
   and raises if they did not.** The capability token says this
   `sf-api` understands the parameters. It does not say the
   database tier does. The server's own comment is explicit
   (`external_api/app.py:578-583`): "A new sf-api against an older
   sf-database accepts the parameters, answers 200, and stores
   empty edge lists, because the fields it sends are unknown to
   the older service and are dropped."

   That is the one failure mode in this phase which is silent,
   unordered execution reported as success, and the POST response
   already carries what is needed to catch it (survey finding 7).
   So after a creating call which sent either list, the client
   compares the `op_uuid` set it sent against the one the returned
   view carries, and raises `IncapableException` naming the
   database tier if they differ. One set comparison on a response
   the client already has; no extra round trip.

   This is the decision most likely to be argued with, on the
   grounds that a client should not police a server's storage
   tier. The counter is that nothing else can: the mismatch is
   invisible to the API node that answered, the operator sees a
   200, and the caller sees an operation that ran early. A client
   that can detect a broken promise about ordering for free should
   not hand the caller the broken version.

6. **No chain submission DSL.** The master plan's row asks for
   "chain submission helpers", and this phase ships the pieces a
   chain needs without a function that submits one.

   What a chain needs is three things: a way to name an edge
   (decision 4), a way to submit without waiting, and a way to
   await the result (decision 7). The second needs no new code --
   `await_seconds=0` on a creating call submits and returns the
   operation as created, raising only for a failure already
   visible in the response (survey finding 6) -- so the only thing
   a DSL would add is a second place in this repository that knows
   which parameters each of the three endpoints takes. Phase 4's
   `client-python-k3s` adoption is the first real consumer of a
   chain, and inventing the submission shape before the consumer
   exists is how the wrong shape gets published and then has to be
   kept.

   What this phase does instead is write the idiom down: the
   `AGENTS.md` section records that `await_seconds=0` is how a
   chain link is submitted, and that awaiting each link as it is
   submitted defeats the chain entirely. If phase 4 finds the
   idiom too sharp, that is the moment to add the helper, with a
   caller to shape it.

7. **`await_agent_chain()` reports the operation that actually
   failed, and says so even when it cannot name one.** Given the
   operations of a submitted chain, it awaits the last one and
   returns it on success. On an `abort` it walks
   `dependency_failure['op_uuid']` back through the chain to the
   first operation whose own failure was not a dependency failure,
   and raises `AgentOperationFailed` naming that operation, its
   state and the hop count.

   The walk has to tolerate three things survey finding 9 found,
   and each is a branch rather than an assumption:

   - `dependency_failure['state']` may be a **pending** state,
     because the dispatcher also aborts a dependent whose wait or
     settle outran `AGENT_OPERATION_DEPENDENCY_WAIT`
     (`CASCADE_EVENT_GAVE_UP`). The walk stops there and says the
     chain ran out of its dependency-wait budget, which is a
     different remedy from a failed step.
   - It may be the sentinel `'missing'`, meaning the dependency
     was swept or deleted. The master plan's decision 9 records
     that a completed, aborted, expired or deleted dependency is
     swept an hour after it finished while an errored one never
     is, so `'missing'` cannot be resolved into success or
     failure. The walk says that rather than guessing.
   - It may be the sentinel `'unreadable'`, meaning the
     dispatcher could not read the dependency's state at all. The
     walk says to look at the cluster, not the chain.

   The walk is bounded by the number of operations it was given,
   so a cycle it cannot construct still cannot hang it. It never
   fetches an operation outside the list it was handed: a chain
   may cross namespaces, and a walk which chased a UUID the caller
   did not submit would turn a reporting helper into an
   authorisation question.

8. **No CLI flags, and an issue instead.** Survey finding 10:
   `sf-client instance execute` prints the results and never the
   operation UUID, and exits 1 with "Results not available."
   whenever the operation has no results yet -- which is the state
   every queued dependent is in. So a `--depends-on` flag would
   have no UUID from a previous invocation to name, and using it
   would make the command fail. Publishing the flags would mean
   also publishing a "submit and print the UUID" mode, which is a
   CLI design question about agent operations generally and not
   about dependencies.

   Filed as [client-python#452](https://github.com/shakenfist/client-python/issues/452)
   rather than invented here, which records the three things that
   would have to come first: a `--no-wait` mode for the three agent
   verbs, the flags themselves, and a decision about what a
   non-zero exit means for an operation that has not run yet.
   `instance show --agentoperations` needs no change: it already
   prints the new fields (survey finding 11).

9. **The functional test forces its failure with a deadline, not
   with a failing command.** A command which exits non-zero
   *completes* as an operation -- the exit code is a result, not a
   failure -- so it is not a `depends_on` failure. The lever that
   is already proven in this suite is
   `test_agentop_deadlines.py`'s: occupy the instance's single
   executor with a blocking operation, create the victim behind it
   with a five-second `deadline_seconds`, and let it expire while
   queued. `expired` is a failed `depends_on` dependency, so the
   dependent cascades to `abort`.

   The test therefore builds three operations on one instance: a
   blocker, a victim with a short deadline, and a dependent whose
   `depends_on` names the victim. It asserts the dependent reaches
   `abort`, and that its `dependency_failure` names the victim's
   UUID and the state `expired`. Ordering gets its own test, with
   no induced failure: two operations where the second
   `depends_on` the first, asserting the second does not reach
   `executing` before the first is terminal.

   The test uses the `ASYNC_CONTINUE` client the deadlines test
   already builds for exactly this reason, and its own
   `_await_agentop_state()` precedent, rather than the base
   class's complete-only waits.

## Step plan

The steps are two groups. Steps 1 to 6 are the `client-python`
pull request, on branch
`agent-operation-dependencies-phase-03-client` in
`../client-python-wt-aod-p03`. Steps 7 to 9 are the `shakenfist`
pull request, on branch
`agent-operation-dependencies-phase-03-functional-test` in
`../shakenfist-wt-aod-p03`, and step 9 **must not be pushed until
step 6 has merged** (decision 1).

Step 0 has already been taken: phase 2's close-out and this
phase's registration are the first commit on the `shakenfist`
branch.

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 1 | medium | sonnet | none | **In `client-python`.** Add `'abort'` to `TERMINAL_AGENT_OPERATION_STATES` at `shakenfist_client/apiclient.py:234`, and update the comment above it (`:230-233`) which says the frozenset is "exactly `AgentOperation.TERMINAL_STATES`" -- it now lists five states, and the server-side tuple is at `shakenfist/operations/agentoperation.py:73-75`. In `shakenfist_client/tests/test_client_apiclient.py`, add `'abort'` to `AgentOperationAwaitTestCase.TERMINAL_FAILURE_STATES` (`:2019`), which is the tuple the existing parametrised tests `test_await_agentop_terminal_failure_raises_on_first_poll` (`:2048`) and `test_await_agentop_async_continue_terminal_failure_still_raises` (`:2076`) iterate, so both gain `abort` coverage for free. Add one further test asserting `'abort' in apiclient.TERMINAL_AGENT_OPERATION_STATES`, so the ordering obligation has an explicit guard rather than only an incidental one. **This commit contains nothing else** -- no parameters, no helpers -- for the reason in decision 2. Commit subject: `Treat an aborted agent operation as terminal.` |
| 2 | high | opus | none | **In `client-python`.** Add the edge constructor and its validation to `shakenfist_client/apiclient.py`, near the other module-level helpers around `:223-234`. Two pieces. (a) `def agent_dependency(operation, settle_seconds=None)`: accepts an operation view (a mapping with a `'uuid'` key) or a UUID string, returns `{'op_uuid': <str>}` plus `'settle_seconds'` only when it was passed. Do **not** emit `op_type`: the server's `agent_dependency` model (`shakenfist/schema/agentoperation_dependency.py`) sets `extra='forbid'` on the published parameter and stores `op_type` itself as a constant it deliberately does not publish. (b) `def _agentop_dependency_edges(name, edges)`: normalises a list whose members are strings or mappings into a list of edge dicts, raising `ValueError` naming `name` (`'depends_on'` / `'runs_after'`) and the index for: a member that is neither a string nor a mapping; a mapping with a key outside `{'op_uuid', 'settle_seconds'}`; a missing `op_uuid`; an `op_uuid` that `uuid.UUID()` refuses. Do **not** bound the list length or `settle_seconds` -- decision 4 says why, and the server does the same thing for the same reason at `agentoperation_dependency.py:115-121`. Unit tests for every refusal and for both accepted member shapes. Commit subject: `Build an agent operation dependency edge.` |
| 3 | high | opus | none | **In `client-python`.** Add `depends_on=None, runs_after=None` to `instance_put_blob` (`apiclient.py:1631`), `instance_execute` (`:1646`) and `instance_get` (`:1663`). In each: when either is not None, require `check_capability('agentoperation-dependencies')` and raise `IncapableException` naming that capability if it is absent (decision 3 -- this is deliberately a raise, not the silent drop `_add_agentop_timing()` does for the timing parameters; say so in a comment, because the asymmetry will read as an oversight otherwise). Run each list through `_agentop_dependency_edges()` from step 2 and put the result in `data` only when the caller passed it, so a caller who passed nothing sends no keys and reaches no capability check. The server accepts both parameters on all three endpoints -- unlike `progress_timeout_seconds`, which `instance_execute` refuses -- so there is no asymmetry to preserve here; the API side is `shakenfist/external_api/instance.py:2351-2353` and `:2466-2468`. Unit tests: each method sends the edges; each raises `IncapableException` when the capability is absent and the caller passed edges; each sends nothing and raises nothing when the caller passed neither. Commit subject: `Send agent operation dependency edges.` |
| 4 | high | opus | none | **In `client-python`.** Add the stored-edge verification of decision 5. After each of the three creating calls has its POST response and before `_await_agentop()` is called on it, compare the set of `op_uuid` values the call sent against the set the response's `depends_on` / `runs_after` carry, and raise `IncapableException` if they differ, with a message that says the API server accepted the edges but the database tier did not store them and names the database tier as what to upgrade. Put the comparison in one private helper called by all three, not three copies. The reason this check exists is quoted in `shakenfist/external_api/app.py:578-583`; cite that file in the comment rather than restating it. The POST response is `o.external_view()` (`shakenfist/external_api/instance.py:2445`), which carries both edge lists, so no extra request is needed. Compare `op_uuid` sets only -- not `settle_seconds`, which an older tier would drop along with the rest of the edge, and not order, which the server does not promise. Unit tests: a response echoing the edges passes; a response with empty lists raises; a response with a subset raises; a response missing the key entirely raises (an older server's view may not carry the field at all). Commit subject: `Refuse a server which drops dependency edges.` |
| 5 | high | opus | none | **In `client-python`.** Add `await_agent_chain(self, operations, timeout=None)` to `Client` in `shakenfist_client/apiclient.py`, near `_await_agentop` (`:1498`). `operations` is the list of operation views a caller submitted, in submission order. Await the **last** one via `_await_agentop(..., await_seconds=timeout)` and return it on success. Catch `AgentOperationFailed` and, when the failed operation's state is `'abort'`, walk `dependency_failure['op_uuid']` back through `operations` to find the root cause, then raise a new `AgentOperationFailed` naming the root operation, its state, and how many hops back it was. Implement the three branches decision 7 names, consulting `shakenfist/daemons/sidechannel/main.py:175-196` for the vocabulary: a `dependency_failure['state']` in `('initial', 'preflight', 'queued', 'executing')` means the dependency-wait budget was spent rather than a step failing (`AGENT_OPERATION_DEPENDENCY_WAIT`, 300 seconds by default) -- say that; `'missing'` means the dependency was swept or deleted and success cannot be distinguished from failure -- say that, citing that an errored operation is never swept while every other terminal one is after an hour; `'unreadable'` means the dispatcher could not read it, which is a cluster problem rather than a chain problem -- say that. Bound the walk by `len(operations)` and never fetch an operation that is not in the list the caller passed (decision 7 says why). A new test case class in `shakenfist_client/tests/test_client_apiclient.py`, modelled on `AgentOperationAwaitTestCase` (`:2010`) and reusing its `_FakeClock` (`:1982`): success; an abort whose cause is the immediately preceding operation; an abort two hops back; each of the three unresolvable branches; an abort whose `dependency_failure` names a UUID not in the list; an abort with no `dependency_failure` at all. Commit subject: `Report the chain operation which actually failed.` |
| 6 | medium | sonnet | none | **In `client-python`.** Add an `## Agent Operation Dependencies` section to `AGENTS.md`, after the existing `## Agent Operation Deadlines` section. Record only what an agent cannot infer from the code: that `TERMINAL_AGENT_OPERATION_STATES` now has five members and that `abort` had to arrive before the submission parameters, not after, and why; that the capability gate here **raises** where the deadlines gate silently sends nothing, and why the two differ; that the stored-edge check exists because an older `sf-database` drops the fields and answers 200; that `await_seconds=0` is how a chain link is submitted and that awaiting each link as it is submitted defeats the chain; and that the chain walk must not resolve a UUID the caller did not submit. Do **not** write a usage guide: phase 4 writes `docs/developer_guide/agent_operations.md` in the server repository, and a second description here is the duplication the doc-discipline policy exists to prevent. Point at that page as the forward reference. While there, do not touch the stale forward reference in the deadlines section, which says "There is deliberately no `docs/` page for the timing model yet" at `AGENTS.md:142-145` -- client-python#389 cites it as `:121-125`, which has since drifted. It is that issue's to fix, and fixing it here would make this pull request the carrier for an unrelated one. Commit subject: `Record the dependency invariants.` Then run `pre-commit run --all-files` (and `tox -eflake8`), and stop at the push. |
| 7 | medium | sonnet | none | **In `shakenfist`** (`../shakenfist-wt-aod-p03`). Add `'abort'` to `base.BaseNamespacedTestCase.AGENT_OPERATION_FAILED_STATES` at `shakenfist/deploy/shakenfist_ci/base.py:1678`, which is survey finding 4: the suite's own copy of the terminal-failure list, read by `_await_command()` (`:1685`) and `_await_agentop_complete()` (`:2048`), both of which would otherwise burn their whole window on an operation that is already finished. Add a comment saying the list is the suite's copy of `AgentOperation.TERMINAL_STATES` minus `complete`, and naming `client-python`'s `TERMINAL_AGENT_OPERATION_STATES` and `client-python-k3s`'s `AGENT_OP_FAILED_STATES` as the other two copies, so the next person adding a state knows there are three. No unit test in this repository asserts on this tuple -- checked at planning time: `grep -rn 'AGENT_OPERATION_FAILED_STATES' shakenfist/tests/` is empty, and the only readers outside `base.py` are `smoke_ci_tests/test_agentop_deadlines.py`. So this step changes one tuple and one comment and adds no test; the coverage comes from step 8, whose cascade test is the first thing in the suite that can reach `abort`. Commit subject: `Fail fast on an aborted operation in CI.` |
| 8 | xhigh | opus | none | **In `shakenfist`.** Write `shakenfist/deploy/shakenfist_ci/smoke_ci_tests/test_agentop_dependencies.py`, modelled closely on `test_agentop_deadlines.py` in the same directory -- read that file first; it is the precedent for everything structural here. Reuse its shape: a `BaseNamespacedTestCase` subclass with `namespace_prefix`, a `setUp` which asserts `check_capability('agentoperation-dependencies')` and builds an `ASYNC_CONTINUE` `nonblocking_client` (its `setUp` comment at `:120-150` explains exactly why a blocking client cannot write these tests), a network, `_create_ready_instance()`, and a local `_await_agentop_state()` (`:186`). Two tests. (a) **Ordering**: two operations on one instance, the second declaring `depends_on` on the first via `apiclient.agent_dependency()`; assert the second does not reach `executing` while the first is non-terminal, and that both eventually complete. (b) **Cascade**: three operations on one instance -- a blocker holding the single executor for `BLOCKER_SECONDS`, a victim queued behind it with `deadline_seconds=SHORT_DEADLINE_SECONDS` so it expires while queued, and a dependent whose `depends_on` names the victim; assert the dependent reaches `abort`, and that its `dependency_failure` carries the victim's UUID and the state `expired`. Decision 9 explains why a failing command is not a usable lever. Copy the constant-with-a-comment discipline of the deadlines file: every timeout is a named module constant whose comment says what it is insurance against, and every absolute bound exists to turn a hang into a diagnosable failure rather than to assert a latency. Which tier the file belongs in (`smoke_ci_tests`) follows the deadlines test; check `.github/workflows/functional-lanes.yml` and `shakenfist/actions`'s `smoke-cluster.yml` for how a tier's directory is selected and confirm a new file in that directory is picked up without registration. Commit subject: `Test an agent operation dependency chain in CI.` |
| 9 | low | sonnet | none | **In `shakenfist`.** Push-and-verify step, gated: do not run it until step 6's pull request has merged to `client-python`'s `develop` (decision 1 -- `setup-test-environment` takes this repository's CI client from that default branch, so step 8's test cannot pass before then). Confirm the merge, then push the branch and open the pull request. Nothing to implement. |

## Risks and mitigations

- **The two pull requests merge out of order, or the second is
  pushed early.** Step 8's test calls `apiclient.agent_dependency()`
  and the new keyword arguments, so pushing it before the client
  has merged spends a full cluster CI run to produce an
  `AttributeError`. *Mitigation:* step 9 is an explicit gated step
  whose only content is the ordering check, and the management
  session runs it rather than a sub-agent. The failure is loud and
  cheap to recover from -- a re-push -- rather than silent.

- **The cascade test is slow and flaky, because it waits for an
  expiry behind a blocker.** This is the risk the deadlines test
  already carries, and the cost is paid on every pull request.
  *Mitigation:* reuse that file's constants and its stated
  discipline -- `BLOCKER_SECONDS` is six times what is needed,
  `AGENTOP_STATE_TIMEOUT` is an anti-hang bound and not a latency
  assertion. The management session reads step 8's diff
  specifically for a timeout that encodes an expected duration
  rather than an insurance margin, which is the way this class of
  test goes bad.

- **Decision 5's check refuses a cluster that is merely
  mid-upgrade, and a caller reads it as a client bug.** A staged
  upgrade with `sf-api` ahead of `sf-database` is a supported
  intermediate state, and this check turns it from silent
  misbehaviour into a hard failure for any caller using edges.
  *Mitigation:* the exception message must name the database tier
  as the thing to upgrade and must not read as a client fault;
  step 4's brief requires that, and the management session checks
  the exact wording rather than the behaviour. The alternative --
  staying silent -- is what decision 5 argues against.

- **Decision 3's raise breaks a caller who passes
  `depends_on=None` explicitly through a wrapper.** A library
  caller forwarding `**kwargs` can easily pass `None` for an edge
  list it does not use. *Mitigation:* the capability check is
  gated on `is not None`, not on truthiness, so `None` reaches no
  check -- and an *empty list* is also a caller who declared
  nothing. Step 3's tests must cover `depends_on=None` and
  `depends_on=[]` separately; an empty list that triggered the
  capability check would be a refusal with no cause.

- **The chain walk is the only genuinely novel logic, and its
  three unresolvable branches are untestable against a real
  cluster.** `'missing'` needs an hour of sweep latency and
  `'unreadable'` needs a database fault. *Mitigation:* they are
  unit tested against fabricated views, which is honest about what
  is being proven; the functional test covers only the branch a
  cluster can actually produce. The plan says so rather than
  implying the functional test covers the walk.

- **A reviewer asks for the chain submission helper decision 6
  declines.** *Mitigation:* the decision states the reasoning and
  names the moment to revisit it (phase 4, with a consumer). If the
  reviewer is right, the helper is additive and costs this phase
  nothing to have left out.

## Definition of done

Each item is checkable, and the command is given where there is
one. An item that cannot be checked is reported as such, not
ticked.

- `'abort' in TERMINAL_AGENT_OPERATION_STATES`, and the frozenset
  has exactly five members. Check: `python -c "import
  shakenfist_client.apiclient as a;
  print(sorted(a.TERMINAL_AGENT_OPERATION_STATES))"` prints
  `['abort', 'complete', 'deleted', 'error', 'expired']`.
- The commit that adds `abort` to the frozenset touches **no**
  file that mentions `depends_on` or `runs_after`. Check: `git
  show --stat <sha>` lists only `apiclient.py` and
  `tests/test_client_apiclient.py`, and `git diff <sha>^ <sha> |
  grep -c 'depends_on\|runs_after'` is 0. This is the falsifiable
  form of decision 2. **Corrected during execution:** the check
  first read `git show <sha>`, which includes the commit message,
  and that message has to name `depends_on` to explain why
  `abort` exists at all -- so the criterion could never pass on a
  commit which justified itself. The diff is what the criterion
  is about.
- That commit is an ancestor of the commit that adds the
  submission parameters. Check: `git merge-base --is-ancestor
  <abort sha> <parameters sha>`.
- The frozenset still has exactly one reader. Check: `grep -c
  'TERMINAL_AGENT_OPERATION_STATES' shakenfist_client/apiclient.py`
  is 2 (the definition and `_await_agentop`'s test), which is what
  makes the one-line change sufficient.
- `agent_dependency()` never emits `op_type`. Check: `python -c
  "import shakenfist_client.apiclient as a;
  print(a.agent_dependency('00000000-0000-4000-8000-000000000000'))"`
  prints a dict whose keys are exactly `['op_uuid']`.
- `_agentop_dependency_edges()` refuses an unknown key, a
  non-UUID string, a missing `op_uuid` and a member of the wrong
  type, and **accepts** a `settle_seconds` of 86400 and a list of
  100 edges. The acceptances are the point: they are the inverse
  of a bound the client deliberately does not carry (decision 4),
  so a test asserting the refusal would encode the wrong
  contract.
- Each of the three creating methods raises `IncapableException`
  when edges are passed to an incapable server, and raises
  nothing when they are not passed. Check: the unit tests, plus
  `grep -n '_add_agentop_dependencies'
  shakenfist_client/apiclient.py` shows one definition and three
  callers, with the capability token named in that helper.
  **Corrected during execution.** This criterion first required
  the token to appear in each of the three methods, which this
  repository's own idiom argues against: `_add_agentop_timing()`
  already holds the timing parameters' capability decision, so
  the dependency helper belongs beside it, where the two
  divergent answers to "what does an incapable server mean" read
  side by side instead of in three near-identical comments.
- Passing `depends_on=None` and passing `depends_on=[]` both reach
  no capability check and send no key. Asserted by two tests, not
  by inspection.
- The stored-edge verification has exactly one implementation.
  Check: the helper decision 5 requires has three callers and the
  comparison logic appears once; `git grep -c` for the helper
  name in `apiclient.py` is 4.
- `await_agent_chain()` never issues a request for a UUID outside
  the list it was handed, asserted by a test that counts calls
  rather than by inspection.
- The chain walk's three unresolvable branches each produce a
  distinct message, asserted by three tests which assert on the
  distinguishing phrase rather than on the whole string.
- `AGENTS.md` records invariants and does not teach usage. Three
  checks, because the first alone is not falsifiable: `grep -c
  'depends_on' AGENTS.md` is greater than 0 (it is 0 today, checked
  at planning time, so the section must move it); the new section
  contains **no fenced code block**, checked by reading the diff;
  and `grep -n 'developer_guide/agent_operations' AGENTS.md` finds
  the forward reference to the page phase 4 writes.
- The stale deadlines forward reference is **unchanged**. It reads
  "There is deliberately no `docs/` page for the timing model yet"
  at `AGENTS.md:142-145` -- client-python#389 cites it as
  `:121-125`, which has drifted. Check: `git diff develop --
  AGENTS.md` adds a section and modifies nothing inside the
  deadlines one. It is that issue's to fix.
- `'abort' in base.BaseNamespacedTestCase.AGENT_OPERATION_FAILED_STATES`
  in `shakenfist`, and its comment names all three copies of the
  list. Check: `grep -n -A 6 'AGENT_OPERATION_FAILED_STATES ='
  shakenfist/deploy/shakenfist_ci/base.py`.
- The new functional test file exists and needs no registration
  anywhere, because the smoke tier is discovered by directory.
  Check: `shakenfist/deploy/smoke-ci.conf` reads
  `test_path=./shakenfist_ci/smoke_ci_tests` -- confirmed at
  planning time -- and `git grep -n 'test_agentop_deadlines'`
  returns hits only under `docs/`, so no workflow or config
  enumerates the tier's files by name.
- Every absolute timeout in the new functional test is a named
  module constant with a comment. Check: read the diff; no bare
  integer appears as a `timeout=` or in a `time.time() - start >`
  comparison.
- The functional test asserts on `dependency_failure`'s
  `op_uuid` **and** `state`, not on the presence of the field.
  Check: read the assertions.
- `docs/developer_guide/agent_operations.md` still does not exist
  in `shakenfist`. It is phase 4's. Check: `test -e` returns
  false.
- Phase 2's row in the master plan's Execution table reads
  `Complete` with `e5354766a4` in its `Merged` cell, and the
  `shakenfist` `docs/plans/index.md` row agrees. Check: `cd
  ../shakenfist-wt-aod-p03 && python3
  tools/check-plan-status.py`.
- `pre-commit run --all-files` is green in both repositories, and
  `tox -eflake8` was additionally run by hand per step in
  `client-python` -- the `flake8` pre-commit hook lints `HEAD~1`,
  not the working tree.
- Step 8's pull request was pushed only after step 6's had merged.
  Check: the `client-python` merge commit's committer date
  precedes the `shakenfist` branch's first push, stated in the
  pull request body.

## Execution notes

Implemented 2026-11-18 (AEDT) with the `implement-phase` skill, one
sub-agent per step at the row's model and effort, each step reviewed
and committed by the management session.

The back brief was answered before step 1: decision 4's division and
`await_agent_chain()`'s signature and messages were both confirmed
as written, the two repositories run in the plan's order rather than
in parallel, and this session may open the `client-python` pull
request.

Departures from the plan, as they happened:

1. **Step 1 covers more than the row claimed.**
   `TERMINAL_FAILURE_STATES` is iterated by four tests, not the two
   the row names: `test_await_agentop_terminal_failure_raises_on_first_poll`
   and `test_await_agentop_async_continue_terminal_failure_still_raises`,
   and also the `await_agent_command()` and `await_agent_fetch()`
   equivalents which drive the real chain rather than a mocked one.
   So the tuple entry bought four tests' worth of `abort` coverage.

2. **Step 2 gained two things in review.** The constructor built
   `{'op_uuid': 'None'}` when handed an operation view with no
   `uuid`, which the cluster would have refused a request later and
   further from the mistake; it now refuses that itself. More
   consequentially, neither helper canonicalised the UUID. The
   server's `op_uuid` is a pydantic `UUID4`, so `external_view()`
   hands back the canonical lower case form -- an upper case UUID
   sent through as typed would have come back looking to step 4's
   comparison like an edge the database tier had dropped, raising
   against a perfectly healthy cluster. Both are covered by tests.

3. **Step 3's gate tests truthiness, not `is not None`.** The step's
   brief asked for `is not None` while also requiring that an empty
   list reach no capability check and send no key; only truthiness
   satisfies both, and an empty list is a caller who declared
   nothing. The timing parameters keep `is not None` because zero is
   a real budget there.

4. **A definition of done check was unrunnable as written.** The
   criterion proving that the terminal state arrived alone piped
   `git show` -- which includes the commit message -- into a grep
   for the edge parameter names. The message names `depends_on`
   because that is why `abort` exists, so the check reported 1
   where it demanded 0 while the diff was clean. Corrected at
   source to read the diff.

5. **Step 3's edges moved into `_add_agentop_dependencies()`.** The
   first implementation inlined the gate and the normalisation in
   all three methods, which is three copies of the same ten lines
   and the thing step 4's single-implementation criterion exists to
   prevent. The helper mirrors `_add_agentop_timing()`, which is
   where this repository already keeps a capability decision about
   an agent operation parameter. The definition of done's check for
   the capability token was corrected at source to match.

## Back brief

Before executing any step, back brief the operator on the
understanding of this plan and how the intended work aligns with
it.

Two gates in particular, both cheap to agree and expensive to
redo:

- **Before step 2**, confirm the edge constructor's signature and
  the division in decision 4 -- that the client validates shape
  and refuses to carry `MAX_AGENT_DEPENDENCIES` or the
  `settle_seconds` ceiling. That division is published API surface
  the moment it ships, and a later change to it is a change to
  what callers may send.
- **Before step 5**, confirm `await_agent_chain()`'s signature and
  the three message shapes for the unresolvable branches. The walk
  is the only novel logic in the phase, and its messages are what
  an operator reads when a chain fails at three in the morning.

Step 9 is a gate rather than a step: the management session, not a
sub-agent, confirms step 6 has merged before anything is pushed
from the `shakenfist` worktree.
