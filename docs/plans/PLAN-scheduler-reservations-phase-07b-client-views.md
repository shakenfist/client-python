# Scheduler reservations phase 7b: capacity and claim views in sf-client

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
`docs/plans/PLAN-scheduler-reservations.md`. Its phases 7a and 7b
were added on 2026-10-20 after Michael asked, "Is it just me, or
have we completely missed implementing any form of visibility for
claims and node business in general in the shakenfist command
line UI?" The survey found that, apart from the claim CRUD verbs,
nothing that plan publishes can be read from `sf-client`. This
phase covers the client half. Its code lands entirely in this
repository, so the plan file lives here, alongside the change it
plans. That follows `PLAN-vdi-console-tokens.md` and agent
operation deadlines phase 6. Phase 4b's plan is the exception:
half of 4b's work was shakenfist's functional tests.

Consult this repository's `AGENTS.md` and `CLAUDE.md`. Its
conventions are not identical to the server's: Python >= 3.7,
single quotes, 120 character lines.

<!-- shared-block: plan-file-conventions v2 -->
Plan file conventions (shared block; do not edit -- the canonical
copy lives in shakenfist/development at
`templates/shared-blocks/plan-file-conventions.md`):

- All planning documents live in `docs/plans/`.
- Detailed planning gets one plan file per phase. Phase files are
  named for their master plan, sit in the same directory as it,
  and append `-phase-NN-descriptive` before the `.md` extension.
- The master plan tracks its phases in a table under its Execution
  section. `Merged` is last, and the push audit is the last row,
  for the reasons given in `plan-push-audit-phase`:

  | Phase | Plan | Status | Merged |
  |-------|------|--------|--------|
  | 1. Schema migration | PLAN-thing-phase-01-schema.md | Not started | |
  | 2. Public API | PLAN-thing-phase-02-api.md | Not started | |
  | 3. Push audit | - | Not started | |

- One commit per logical change, and at minimum one commit per
  phase. Unrelated changes are not batched into a single commit.
  Each commit is self-contained: it builds, passes tests, and has
  a message explaining what changed and why.
<!-- shared-block-end -->

This file does not "sit in the same directory as" its master
plan, which is in another repository. That departure is the
precedent described above. The master plan names this file in
its Execution row instead of linking it, because a relative link
cannot cross repositories.

## Planning effort

Planned at medium effort. Almost all of this is a new command, or
a new column, that mirrors one already in the tree. Only one
judgement is expensive to get wrong: the shape of the capacity
view (D2 to D4). Once a command's output is in operators' scripts
and in phase 8's operator guide, it is hard to change. The back
brief gates that step.

## Scope

**In scope.** Everything below works against a server at
shakenfist `develop`.

- `sf-client node capacity`, a per-node rendering of
  `/admin/resources`.
- Fixing `sf-client node resources`.
- `apiclient` verbs and CLI commands for namespace events and
  claim events (phase 7's endpoints).
- `peak_used_*` in `namespace claim show`.
- Daemon states in `node show`.
- A `docs/` page, and the `AGENTS.md`, `CLAUDE.md` and README
  touches that page needs.
- The readers of phase 7a's surface, gated on its capability
  tokens: the cluster claims ledger under `node capacity`, each
  excluded node's reason in place of D4's sentence, and
  `namespace claim list --all` and `--coverage`. These were 7a's
  step 8 until 2026-10-27; see D7.

**Out of scope.**
- A release. CI and the conductor track `develop`, so phase 8
  cuts one release covering phases 4b to 7b (4b's D7).
- Shakenfist functional coverage of the new CLI commands. The
  functional suite drives `apiclient`, not `sf-client`. The only
  new `apiclient` surface here is two thin events verbs over
  endpoints that phase 7 already covers functionally.

## What the survey found

Survey taken against client-python `d789e0b` and shakenfist
`ebc0f9772b`, both at `develop` on 2026-10-20. The master plan's
7b stub was written in the same session, from the same reading.
Two of its claims were wrong, and both are corrected at their
source on shakenfist's `sr-capacity-visibility` branch, so a
later step does not need to redo it:

- It said this plan would live in client-python "as 4b's did".
  4b's plan lives in shakenfist. The precedent here is agent
  operation deadlines phase 6.
- It placed the 7a-gated client work in 7b. D7 moves it into 7a.

The remaining findings:

1. **`/admin/resources` has a client verb and no command.**
   `apiclient.get_cluster_resources()` (`apiclient.py:1788`)
   gates on `cluster-resources` and nothing in
   `shakenfist_client/commandline/` calls it. Its only consumer is
   CI: shakenfist/actions' `ci_headroom_collect.sh` and the
   suite's capacity wait.
2. **`per_node` is keyed by node UUID, not name.** shakenfist's
   `docs/developer_guide/ci_cloud_sizing.md:444` records this
   because the CI wait tripped over it. `get_nodes()` returns both
   `uuid` (from `baseobject._external_view()`) and `name`, so a
   view can join them.
3. **`per_node` silently omits three kinds of node.** These are
   non-hypervisors, hypervisors whose `node_queue_waiting` exceeds
   `UNREASONABLE_QUEUE_LENGTH` (`scheduler.py:1088`), and nodes
   that `get_active_node_metrics()` did not return. A client
   joined against `get_nodes()` can tell the first case from the
   other two, but cannot tell the last two apart. Telling them
   apart is 7a's job.
4. **The `per_node` fields are the ones the master plan's table
   lists.** As of `scheduler.py:1047-1218`, they are:
   - CPU: `cpu_max_per_instance`, `cpu_schedulable`,
     `cpu_reservation_clamped`, `cpu_committed_row_present`,
     `cpu_limit` (None without a capacity row), `cpu_hard_max`,
     `cpu_measured`, `domains_enumerated_at`, `cpu_committed`,
     `cpu_available` (may be negative), `cpu_load_1/5/15`.
   - Memory: `memory_reservation_clamped`, `memory_reserved_mb`,
     `ram_max_per_instance`, `ram_max`, `ram_committed`,
     `ram_available`.
   - Disk: `disk_available`, `disk_limit_gb` (None without a
     row), `disk_committed_gb`.
   - Instances: `instances_total`, `instances_active`.

   `total` carries `cpu_available`, `ram_available` and
   `capacity_degraded`. Several of these keys arrived without a
   capability token over the 0.8 cycle (the `cpu_*` ledger
   breakdown, the clamp flags and the disk counters among them),
   so a new client can meet an older server that lacks some of
   them.
5. **`node resources` is broken in two ways.** It indexes `[0]`
   before its empty check (`node.py:166`), so a node with no
   `resources` event raises IndexError. In the pretty and simple
   paths it also reads `event['extra']['resource']`, with the
   literal key instead of the loop variable (`node.py:174`,
   `:180`), which raises KeyError on any real event. The JSON path
   works. This was found by reading the code, not by running it.
   Its data (the node's last raw metrics event) differs from
   `/admin/resources`: it is the measurement side, without the
   ledger.
6. **Phase 7's events endpoints have no client verbs.** They are
   `/auth/namespaces/<ns>/events` and
   `/auth/namespaces/<ns>/claims/<claim_ref>/events`
   (shakenfist `external_api/app.py:574`, `:592`), both admin
   only, both taking `event_type` and `limit` like every other
   events endpoint, and advertised as `namespace-events` and
   `namespace-claim-events` (`app.py:418-425`). Events rows are
   `EventReadRow` (`schema/event.py:55`), the same shape
   `node events` already renders, `fqdn` included.
   `apiclient._get_events()` (`apiclient.py:654`) builds
   `'/' + object_plural + '/' + ref + '/events'`, so passing
   `'auth/namespaces'` as the plural reaches the namespace
   endpoint unchanged.
7. **`peak_used_*` has no capability token.** It landed in
   shakenfist `1b54cca3c9` on 2026-10-02 and is in no release. A
   claim from an older server lacks the keys entirely. A claim
   whose row is missing reads `None`. A claim the cluster has not
   raised yet reads `0` (`namespace_claim.py:350-355`).
   `_claim_rows()` and `claim_show` index every other field
   directly (`namespace.py:285-351`), so indexing these directly
   would crash against the cluster this client is most likely to
   meet.
8. **`IncapableException` is not caught by
   `GroupCatchExceptions`** (`main.py:113-175`). Commands that
   gate catch it themselves (`network.py:127`, `node.py:210`).
   New gated commands must do the same, or a missing capability
   prints a traceback.
9. **`node show` drops what the server already sends.**
   `Node.external_view()` returns a `daemon-<name>-state` key for
   each of `Node.VALID_DAEMONS` (`node.py:246-250`, `:564`), and
   `node show` prints none of them.

## Decisions

**D1. The capacity view is `sf-client node capacity [NODE]`, not
`admin resources`.** An operator asking how busy the nodes are
looks under `node`. Every node command is already admin only, so
the `admin` group buys no clarity. Its only member is `lock`, an
internals view. Naming one node filters the rendering
client-side. The endpoint has no per-node form, and this phase
does not add one.

**D2. Committed, measured, limit and hard max are separate
columns, and the pretty output marks the one that binds.** The
server publishes all four because "this node measures as idle
but is refusing work" is only diagnosable if you can see which
ledger is binding (`scheduler.py:1135-1137`). Merging them into a
"used" column is the same mistake `namespace claim` refuses to
make with `state` and `coverage_state`. AGENTS.md records it for
the same reason. Pretty output marks the binding CPU figure:
`max(measured, committed)` gives a charge, and the ceiling is
`cpu_limit` when the node has a capacity row and `cpu_hard_max`
when it does not: the limit replaces hard max, it does not join
it (`_has_sufficient_cpu()`). The arithmetic is
the server's (`scheduler.py:1150-1151`), copied, not invented, and
it carries a comment naming its source, as
`TERMINAL_AGENT_OPERATION_STATES` does.

**D3. Pretty output is three tables (CPU, memory, disk), not one
wide one.** One row per node across all three dimensions is about
twenty columns. That wraps on any terminal and defeats the side
by side reading D2 exists for. Each table leads with the node
name. A notes column carries `no capacity row`, `clamped`, and a
negative headroom. A totals line gives `total.cpu_available` and
`total.ram_available`. If `total.capacity_degraded` is true, a
warning banner goes above everything, because a degraded read
makes every committed figure a zero that means nothing. Simple
output is one flat CSV row per node with a fixed column order,
`capacity_degraded` repeated as a column. JSON output is the
server payload verbatim, with no name join. Every other command's
JSON is the wire format, and scripts already read this one by
UUID.

**D4. Hypervisors missing from `per_node` are listed, not
dropped.** After joining against `get_nodes()`, pretty output
prints a line under the tables naming every hypervisor absent
from `per_node` as "not reported by the scheduler (queue length
or stale metrics)". The client cannot say which until 7a, and
must not guess. Non-hypervisors are omitted, and the `--help`
text says so. Of everything here, this is the decision most
likely to be argued with: it prints a sentence that admits
ignorance. The alternative is to show nothing, and that silence
is the defect the master plan's survey table calls out: the
busiest node in the cluster is the one the view cannot show.

**D5. Keys missing from an older server render as `n/a`, not as
a crash and not as zero.** Finding 4 says key-by-key arrival is
the norm here. `n/a` is distinct from `-`, which renders a
server's own `None`, such as `cpu_limit` without a row. The
`peak_used_*` fields follow the same three-way rule (finding 7):
missing renders `n/a`, `None` renders `-`, a number renders the
number. This deliberately departs from `_claim_expiry`'s
direct-index rule, and the code says why: `expires_at` has
existed since claims did, and these keys have not.

**D6. `node resources` is fixed, not retired.** It shows the
node's own raw measurement event, which `node capacity` does not
replace. The fix is the loop variable and an empty check that
exits non-zero before indexing. Its help text gains one sentence
pointing at `node capacity` for the ledger. Retiring it would be
a breaking change, made for no reason beyond the fact that it has
been broken.

**D7. The client work gated on 7a moves into 7a's phase plan.**
That is the claims ledger columns, `claim list --all`, and the
exclusion reasons in D4's line. 7a designs those response shapes,
and the reader of a surface is the best test of it. The gap this
whole insertion exists to close is a server surface nobody reads.
7a's plan should carry one client-python step, the way agent
operation deadlines phase 6 carried shakenfist steps, and D4's
line should be upgraded in place. The master plan's 7a and 7b
stubs are corrected to say so.

*Reversed on 2026-10-27.* 7a merged (shakenfist#4654) with that
step deferred, because it renders into this phase's `node
capacity`, which did not exist yet. Holding 7a open across a
client phase would have left its status wrong for as long as this
phase takes. D7's reason for moving the work is spent: 7a's
shapes are now designed and on `develop`, so a step briefed
against them cannot be done twice. The work is step 6 here, with
7a's brief carried over, and step 4's back brief mock covers its
output too, so Michael agrees the whole `node capacity` format
once.

**D8. Gated commands catch `IncapableException` themselves and
exit 1 with the exception's message,** following `network.py:127`
(finding 8). Moving it into `GroupCatchExceptions` would be the
better fix. It would also change the output of every gated
command in the tree, which is out of scope, so it is recorded
under Future work.

## Step plan

| Step | Effort | Model | Isolation | Brief for sub-agent |
|------|--------|-------|-----------|---------------------|
| 1 | medium | sonnet | none | **Events verbs and commands.** In `shakenfist_client/apiclient.py`, add `get_namespace_events(namespace, event_type=None, limit=None)` and `get_namespace_claim_events(namespace, claim_uuid, event_type=None, limit=None)` beside `get_node_events` (`:688`). Each first checks its capability (`namespace-events` and `namespace-claim-events` respectively) and raises `IncapableException` with the house wording (see `get_cluster_resources` at `:1788`). Each then calls `self._get_events('auth/namespaces', namespace, ...)`, or `('auth/namespaces/' + namespace + '/claims', claim_uuid, ...)`. `_get_events` (`:654`) builds the path and applies the `events-by-type` gate itself. In `shakenfist_client/commandline/namespace.py`, add `namespace events NAMESPACE [-t TYPE] [-l LIMIT]` and `namespace claim events NAMESPACE CLAIM_UUID [-t] [-l]`. Render exactly as `node events` does (`commandline/node.py:134-159`: timestamp, fqdn, duration, message, extra, in all three output modes). Reuse the existing `_get_namespaces` and `_get_claims` completers. Both commands catch `IncapableException`, print its message and `sys.exit(1)` (pattern: `commandline/network.py:127`). Tests: apiclient tests for the URL built and the capability refusal (follow the existing `get_namespace_claims` tests in `tests/test_client_apiclient.py`), and `CliRunner` tests in `tests/test_client_commandline_namespace.py` for each output mode and for the incapable path. Commit subject: "Add namespace and claim events commands." |
| 2 | low | sonnet | none | **`peak_used_*` and `node show` daemon states.** In `commandline/namespace.py` `claim_show` (`:328-351`), print `peak_used_cpus`, `peak_used_memory_mb` and `peak_used_disk_gb` after the matching `used_*` line. Render a key missing from the dict as `n/a`, `None` as `-`, and otherwise the value. Add a small helper with a comment: these keys arrived in shakenfist `1b54cca3c9` with no capability token, which is why they are not indexed directly like the fields around them (`_claim_expiry`'s comment at `:272` explains the direct-index rule this departs from). Do not add them to `claim list`, which is already seven columns wide. In `commandline/node.py` `node_show` (`:23-72`), after the role lines and before metadata, print a `daemon states` section. List every key matching `daemon-<name>-state`, sorted by name, with `None` rendered as `-`. Pretty mode indents it as metadata is, and simple mode uses a `daemon,<name>,<state>` CSV like metadata's. JSON output already carries them, so leave it alone. Tests: claim show against a claim with, without and with `None` peak keys. Node show has no test file yet, so create `tests/test_client_commandline_node.py` with the `_invoke` pattern from the namespace tests. Commit subject: "Show claim high water marks and node daemon states." |
| 3 | low | sonnet | none | **Fix `node resources`** (`commandline/node.py:162-185`). Fetch the events list, and if it is empty print `No resources event found` and `sys.exit(1)` before indexing. Replace `event['extra']['resource']` with `event['extra'][resource]` in both the pretty and simple paths. Add one sentence to the help: this is the node's last raw measurement, and `node capacity` shows what the scheduler admits against. Tests in `tests/test_client_commandline_node.py`: an empty event list exits 1, and a real event renders every key in pretty and simple modes. Commit subject: "Fix sf-client node resources." |
| 4 | high | opus | none | **`sf-client node capacity [NODE]`.** **Back brief before writing code (see Back brief).** Implement D1 to D5 in `commandline/node.py`. Fetch `get_cluster_resources()` and `get_nodes()`. Build `uuid -> name` from the node list and render unknown UUIDs as the UUID. If `NODE` is given, filter to it by name or UUID, and exit 1 if it is not in `per_node`, printing D4's sentence when it is a hypervisor. Pretty output has three PrettyTables. **CPU**: node, committed, measured, limit, hard max, available, schedulable, load 1/5/15, notes. **Memory**: node, committed, available, max, reserved, max per instance, notes. **Disk**: node, committed GB, limit GB, available GB, notes. Notes carry `no capacity row` (from `not cpu_committed_row_present`), `cpu clamped` / `memory clamped`, and `over limit` when available is negative. Mark the binding CPU figure with `*`, using the server's arithmetic: charge = `max(cpu_measured, cpu_committed)`, ceiling = `cpu_limit` when the node has a capacity row, else `cpu_hard_max` (shakenfist `scheduler.py:1094-1151`, and shakenfist's `docs/operator_guide/scheduler.md` around line 398). Cite those in a comment saying the arithmetic is copied, and that a change there must change here. Print a totals line from `total`. If `total.capacity_degraded` is true, print a banner first saying the committed figures could not be read and read as zero. Then list hypervisors (from `get_nodes()`, `is_hypervisor`) absent from `per_node`, with D4's exact wording. Simple output is one CSV header and one row per node, every per_node key in a fixed order you define once as a module constant, with name, uuid and capacity_degraded columns. JSON output is `get_cluster_resources()` verbatim. Render a key missing from the payload as `n/a` and a `None` as `-` (D5), through one helper, since the whole view needs the rule. Catch `IncapableException` as in step 1. Tests: a fixture payload built from the field list in this plan's finding 4, covering a node with no capacity row, a clamped node, a negative headroom, a degraded total, a hypervisor absent from `per_node`, an unknown UUID, a payload missing a key, and the binding marker on measured, on committed and on limit. Commit subject: "Add sf-client node capacity." |
| 5 | medium | sonnet | none | **Documentation.** Create `docs/cluster-capacity.md`, covering `node capacity` (what each column is, which ceiling binds and why there are two ledgers, what `no capacity row`, `clamped` and the degraded banner mean, and why some hypervisors are listed as not reported), `node resources` and how it differs, and `namespace events` / `namespace claim events`. Explain why calibrating a claim means reading the namespace's events rather than the claim's: a claim's events die with the claim (shakenfist `app.py:418-424`'s comment). Link shakenfist's `docs/operator_guide/scheduler.md` for the admission model instead of restating it. In `docs/namespace-claims.md`, add `peak_used_*` and the events command, and link the new page. Add the page to README's documentation list, matching the existing entries' absolute-URL form (`README.md:42-51`). In `AGENTS.md`, add a short section mirroring the claims one: the binding marker's arithmetic is a copy of the server's, the columns are never merged, absent hypervisors are listed and never dropped, and `n/a` versus `-` is a deliberate distinction. Keep it to invariants, with detail in the docs page. Add a `CLAUDE.md` Recent Changes entry in the style of the existing ones. Commit subject: "Document the capacity and events commands." |
| 6 | high | opus | none | **The readers of phase 7a's surface** (formerly 7a's step 8; see D7). The server shapes are shakenfist's `admin_resources_get_example` and `admin_claims_get_example` in `shakenfist/external_api/admin.py`, and the exclusion reasons are the closed set `EXCLUSION_REASONS` in `shakenfist/scheduler.py`. Implement the format agreed at step 4's back brief, which included this step's output: (a) add a cluster claims ledger table under `node capacity`'s pretty output (columns: dimension, total, claimed, unclaimed used, unallocated), plus the ledger's keys in simple mode, with `row_present` false rendering D5 `-` and `degraded` true adding a banner line. Gate on `cluster-capacity`; when the token is absent, print nothing for the ledger and say so in `--help`. (b) Where the server sends `excluded` (gate on `cluster-resources-excluded`), replace D4's sentence with each node's reason, keeping D4's wording as the fallback for an older server. (c) `namespace claim list` gains `--all` (NAMESPACE becomes optional, and exactly one of the two must be given) and `--coverage active\|expired`. The `--all` view adds a `namespace` column ahead of `CLAIM_COLUMNS` and keeps `state` and `coverage` as separate columns (the rule recorded above `CLAIM_COLUMNS`). Catch `IncapableException` per D8. Tests: `CliRunner` tests for each, including old-server payloads without the new keys or tokens. Update client-python `docs/cluster-capacity.md` (step 5's page) and `docs/namespace-claims.md`. Step 7's floor audit covers this step's code. Commit subject: "Show the claims ledger and exclusions." |
| 7 | medium | opus | none | **Push audit floor and close-out.** client-python has no `PUSH-AUDIT.md`. The `plan-push-audit-phase` shared block says a phase that lands in another repository is audited in the pull request that lands it, and that a repository without a runbook gets at least the five-item floor checklist. So apply that checklist to `develop...HEAD` and record a verdict, or a reason it does not apply, for each item in a *Push audit* section of this plan. Item 4 greps every sibling checkout under the parent directory for `node resources` and for any `per_node` consumer, and states the greps. Add the questions this phase raises: does any new command print a credential (expected no, since none are handled), and does any rendering path crash on an older server (exercise each command against a payload with every optional key removed). Then write the plan's *Outcome* section: what each step did, anything deferred and where it was recorded. Run `pre-commit run --all-files` and `stestr run`. Commit subject: "Close out scheduler reservations phase 7b." |

Steps 1 to 3 are independent and may run in any order. Step 4
reuses step 2's missing-versus-None helper if it has landed, so
run it after 2. Step 5 needs 1 to 4. Step 6 renders into step 4's
command and edits step 5's page, so it follows both, and step 7
is last.

## Risks and mitigations

- **The binding marker drifts from the server's arithmetic.**
  This is the same hazard as `TERMINAL_AGENT_OPERATION_STATES`. A
  wrong `*` is worse than none, because it sends an operator to
  the wrong ledger. Mitigation: step 4's comment names the source
  lines, AGENTS.md records the copy, and the management session
  checks the step 4 tests against `scheduler.py:1094-1151` while
  reviewing, not against the brief.
- **The view goes stale against the server's key set.** A key
  added to `per_node` later does not appear in pretty output.
  Mitigation: simple and JSON output carry every key, and pretty
  is documented as curated. This is accepted rather than
  engineered away.
- **Operators read `n/a` as zero.** Mitigation: the docs page
  defines both sentinels, and step 7's older-server exercise
  checks they appear where expected.
- **The UUID join silently mislabels.** Mitigation: an unknown
  UUID renders as itself, never as a guessed name, and it is
  covered by a step 4 test.

## Definition of done

- [x] `grep -c "get_cluster_resources" shakenfist_client/commandline/*.py`
      is at least 1. It is 0 at `d789e0b`.
- [x] `sf-client node resources <node>` exits 0 with a real event,
      and 1 with none, in pretty and simple modes. This is checked
      by `CliRunner` tests that fail at `d789e0b`. Revert step 3's
      fix locally and the new tests go red.
- [x] `sf-client node capacity` against step 4's fixture prints
      every hypervisor in the node list, either in a table or on
      the not-reported line. A test asserts that the set of names
      printed equals the set of hypervisors.
- [x] No pretty table has a column that merges committed and
      measured. Read the step 4 code; this cannot be grepped.
- [x] `namespace events` and `namespace claim events` exit 1 with
      the capability message, not a traceback, against a client
      whose `root_html` lacks the token. A test covers it.
- [x] `namespace claim show` against a claim dict with no
      `peak_used_*` keys exits 0 and prints `n/a` three times.
- [x] `sf-client node capacity` against an `excluded` fixture
      prints each node's reason instead of D4's sentence. Against
      an older server's payload, with neither `cluster_capacity`,
      `excluded` nor their tokens, it prints D4's sentence and no
      ledger, and exits 0. `CliRunner` tests cover both.
- [x] The ledger renders every figure as `-` when `row_present`
      is false, and adds the banner line when `degraded` is true.
      A test covers each.
- [x] `namespace claim list` with both `NAMESPACE` and `--all`, or
      with neither, exits non-zero with a usage message. A test
      covers both.
- [x] `pre-commit run --all-files` and `stestr run` pass.
- [x] The *Push audit* section has a verdict for each of the five
      floor items.
- [ ] shakenfist's master plan row for 7b carries
      `client-python <sha> (#pr)` in its `Merged` cell. This is
      done at the next shakenfist phase close-out, not in this
      pull request, per `plan-phase-landing`.

Checked by the management session on 2026-10-28: each item's named
test passes, step 3's two bugs restored on a copy turn all three
`node resources` tests red, and `pre-commit` and `tox -e py3` pass
(332 tests). The last item waits for the merge.

## Back brief

Before executing any step of this plan, please back brief the
operator as to your understanding of the plan and how the work you
intend to do aligns with that plan.

**Additional gate before step 4.** Step 4 fixes an output format
that operators will script against and that phase 8's guide will
quote. Before writing code, the step 4 sub-agent returns a mocked
rendering of all three output modes against the fixture, with
the binding marker, the notes, the degraded banner and the
not-reported line all visible. It also mocks step 6's additions
against the same fixture: the cluster claims ledger table, its
simple-mode keys, an excluded node's reason in place of the
not-reported sentence, and one `claim list --all` table. The
management session takes that to Michael. Agreeing a mock costs a message, and changing the
format after phase 8's guide quotes it costs a docs revision and
a script break.

## Future work

- **`IncapableException` in `GroupCatchExceptions`** (D8). Every
  gated command catches it by hand, and an ungated one that
  forgets prints a traceback.
- **A per-node `/admin/resources` form.** `node capacity NODE`
  filters client-side. That is fine at cluster sizes the
  project runs today, and would be a 7a question if it stopped
  being fine.
- **sfui.** The web UI has no capacity view either. It is out of
  scope for a client-python phase, and is noted here so it is not
  forgotten.

## Execution notes

Implementation began on 2026-10-28 (Australia/Sydney).

**Back brief answers.**
- Steps run one at a time in this worktree, 1 to 7, because every
  step runs `pre-commit` in the same tree.
- Step 4's gate stays: its mock, covering step 6's additions too,
  goes to Michael before any code is written.
- This session may push the branch and open the pull request once
  the definition of done passes.

**Premises re-checked before briefing.** The findings still hold,
including both `node resources` bugs. Line references have moved
since the survey, so briefs locate code by symbol:
- client-python, after #438 and other merges:
  `get_cluster_resources` is at `apiclient.py:1853`, `claim_show`
  at `namespace.py:335`, `node_show` at `node.py:26`, `node_events`
  at `:139` and `node_resources` at `:165`.
- shakenfist, after #4654: `summarize_resources()` starts at
  `scheduler.py:1138`, and the CPU binding arithmetic the binding
  marker copies is at about `:1255-1290`. References above to
  `:1094-1151` and `:1150-1151` mean that code.
- The unit tests live in `shakenfist_client/tests/`, not `tests/`.

**Step 4.**
- The plan said `cpu_limit` and `cpu_hard_max` were both
  ceilings. The server's `_has_sufficient_cpu()` uses the row's
  limit when there is a row and hard max only when there is not,
  so D2 and the step 4 row were corrected at source, and the
  marker follows the server.
- The format went to Michael as a mock before any code and was
  approved as drafted, with simple mode keeping its sections.
- One case the mock left open: JSON output with `NODE` keeps the
  payload's shape and cuts `per_node` to that node.
- Mutations: five of six killed. The survivor drops the
  `cpu_committed_row_present` half of the no-row test, which no
  real payload can tell apart from `cpu_limit` being None,
  because the server sets both from the same row. The check is
  kept as a defence.

**Step 6.**
- The mock was followed, with one change. Names in the
  not-reported list are padded to the longest name, not a fixed
  width, so that real node names line up.
- Cases the mock left open:
  - An unknown reason is printed as sent.
  - A detail the server left out is not printed.
  - An `excluded` uuid the node list does not know, or one that
    is not a hypervisor, is left to JSON.
  - `--coverage` without `--all` is a usage error, because the
    per-namespace listing cannot filter.
- A hypervisor with no `excluded` entry, on a server that has
  the token, was briefed to fall back to the old two-cause
  sentence. That would be false: the server gives every active
  node its queue or metrics reason, so such a node was not
  active. It now prints "no reason given (node state X)".

## Push audit

client-python has no `PUSH-AUDIT.md`, so this is the
`plan-push-audit-phase` floor checklist plus this phase's own
questions. It was run on 2026-10-28 over
`git diff origin/develop...HEAD`: 14 files, +2448/-17. The
local `develop` ref was stale at `d789e0b` (#432), and the
branch is based on `origin/develop` at `05d04eb` (#438), so
`develop...HEAD` would also have audited #433 to #438, which
merged separately. Nothing was fetched. The definition of
done's references to `d789e0b` are left alone on purpose,
because they name a point before the phase.

**1. Recorded landings.** Not yet applicable. The phase lands
as one pull request, and its merge commit is recorded once it
merges. Until then, the range is the branch's eight commits,
`0c6340d..37a70ea`. shakenfist's master plan row gets
`client-python <sha> (#pr)` at the next shakenfist close-out,
as the definition of done says.

**2. Unattended runners.** Does not apply. The diff adds no
cron job, timer, scheduled workflow or self-update, and changes
nothing under `.github/`. Every change is an interactive
`sf-client` command or an `apiclient` verb.

**3. Secrets and private hosts.** Pass. A grep of the diff's
added lines for credential words, key headers, IPv4 addresses
and home-lab hostnames finds only the capability tokens' names
and `192.168.1.1`. That address is a generic RFC 1918
placeholder in the `NODE` test fixture, not an address from any
network.

**4. Removed or renamed surface.** Pass. The diff removes or
renames no command, option, `apiclient` verb or key. These are
the output changes:
- `node resources` used to crash in pretty and simple modes; it
  now renders, and exits 1 with no event.
- `node show` simple output gains a `daemon,name,state` section
  before `metadata,key,value`.
- The `claim show` pretty label column grows from 16 to 19
  characters.
- `claim list`'s `NAMESPACE` becomes optional, but only
  alongside `--all`.

The greps ran from the parent directory over every sibling
checkout, excluding `.git`, `.tox`, `.ansible`, `node_modules`,
`target`, `.venv` and `__pycache__`:

    grep -rIl -F 'node resources' .
    grep -rIl -F 'per_node' .
    grep -rIn -E 'sf-client[^|]*node resources' .
    grep -rIl -E 'admin/resources|get_cluster_resources|ci_headroom' actions

Each repository has several worktrees, and all of them were
grepped. Counts below are files in the primary checkout, with
the worktrees in brackets where they differ.
- `node resources`:
  - shakenfist: 4 (and 4 in each of five worktrees). These are
    the server's own event message, docs and plan text.
  - private-ci: 2 (and 2 in each of four worktrees). These are
    plan prose saying the command crashed, which this phase
    fixes.
  - No script anywhere invokes it. The third grep finds only
    that private-ci prose.
- `per_node`:
  - shakenfist: 65 per checkout, all server code, docs and
    tests.
  - private-ci: 8 (4 to 8 in its worktrees). This is the
    conductor's `nodefit.py` and `ledger.py`. They read
    `get_cluster_resources()` through `apiclient`, which is
    unchanged.
  - Hits in other repositories are unrelated identifiers, such
    as `*_per_node` test names and deployment variables.
  - actions: none. `ci_headroom_collect.sh` names
    `/admin/resources` in a comment but reads Loki, not
    `sf-client`.

No consumer of `sf-client` output, or of anything renamed, is
affected.

**5. Fail-closed publishing.** Pass. The only publisher is
`release.yml`, which runs on `v*` tags. This phase cuts no
release (Scope, out of scope), and changes no workflow,
packaging or version metadata.

**Does any new command print a credential?** No. None of the
new or changed commands reads or handles a credential:
- `node capacity`, `node show` and `node resources` print node
  names, uuids, states and capacity figures.
- The claim commands print claim fields and limits.
- The two events commands print event rows: timestamp, fqdn,
  duration, message and extra.

**Does any rendering path crash on an older server?** No
reachable one. Every new or changed command was driven through
`CliRunner` with the existing fixtures, stripped of every
optional key, under `.tox/py3/bin/python -B`: 78 runs, 70 pass.

| Command | Payloads | Result |
|---------|----------|--------|
| `node capacity`: pretty, simple, json, and `NODE` for a reported and an unreported node | Every `per_node` key removed and `total` empty, under four conditions: old tokens; new tokens with `excluded` and `cluster_capacity` absent; both present but empty; an empty payload. Nodes cut to uuid, name and `is_hypervisor` | 36 pass. Exit 0, or 1 for an unreported node |
| `node show` | No `daemon-*-state`, no `is_database_node`, no node metadata capability | 3 pass |
| `node resources` | No event | 3 pass, exit 1 |
| `node resources` | `extra` None or absent | json passes; pretty and simple raise TypeError (4 fail) |
| `namespace claim show`, `claim list NAMESPACE`, `claim list --all` | No `peak_used_*`, no `updated_at`; `--all` without the token | 12 pass (`n/a` shown; no token exits 1) |
| `namespace events`, `namespace claim events` | Optional fields None; no token | 12 pass (no token exits 1) |
| `namespace events`, `namespace claim events` | `duration` absent | json passes; pretty and simple raise KeyError (4 fail) |

The eight failures are **unreachable**. No server, old or new,
sends either payload:
- The server serialises event rows with
  `row.model_dump(mode='json')` (shakenfist
  `external_api/base.py:2384`), which emits `duration` and
  `extra` even when they are None.
- The only resources event is written with `extra=retval`, a
  dict (`daemons/resources/main.py:695`).

Both patterns predate this phase. `node events` on `develop`
indexes `e['duration']`, and the `event.get('extra')` loop in
`node resources` was there before step 3. The two new events
commands copy `node events` on purpose (step 1's brief). Both
failures are recorded and declined, with no fix and no issue.

## Outcome

Steps, with their commits:
- Planning: `0c6340d` planned the phase, and `df45c8d` folded in
  phase 7a's deferred client readers as step 6 (D7 reversed).
- Step 1, `03c5530`: `get_namespace_events()` and
  `get_namespace_claim_events()`, each gated on its token, plus
  `namespace events` and `namespace claim events`, which exit 1
  on a missing capability.
- Step 2, `3812802`: `peak_used_*` in `claim show` through the
  new `util.render_optional()`, which renders a missing key as
  `n/a` and a None as `-`. Daemon states in `node show`.
- Step 3, `a228295`: `node resources` fixed. It exits 1 with no
  event, and reads each key by the loop variable.
- Step 4, `595e7d6`: `node capacity [NODE]`, with three tables,
  the binding marker copied from the server's arithmetic, notes,
  the degraded banner, the not-reported hypervisors, and simple
  and JSON forms. The format was approved by Michael as a mock
  before any code was written.
- Step 5, `17ce3f1`: `docs/cluster-capacity.md`, plus
  `docs/namespace-claims.md`, README, AGENTS.md and CLAUDE.md.
- Step 6, `37a70ea`: the exclusion reasons and the cluster claims
  ledger in `node capacity`, gated on their tokens, and
  `claim list --all` and `--coverage`.
- Step 7: this push audit and close-out.

Deferred, and where each is recorded:
- `IncapableException` in `GroupCatchExceptions` (D8): Future
  work.
- A per-node `/admin/resources` form: Future work.
- An sfui capacity view: Future work.
- A release: Scope says phase 8 cuts one covering 4b to 7b.
- shakenfist's master plan `Merged` cell: the definition of done
  says it is filled at the next shakenfist close-out.
- The two unreachable crash classes: declined in the Push audit
  above.

Nothing deferred was found unrecorded.

Departures recorded in the Execution notes:
- The CPU ceiling is `cpu_limit` with a row and `cpu_hard_max`
  without one, not both. D2 and step 4 were corrected at source.
- Simple mode keeps its not-reported and ledger sections, as
  approved at the mock, rather than D3's single flat table.
- JSON with `NODE` keeps the payload's shape and cuts
  `per_node` to that node.
- Step 4 killed five of six mutants. The `row_present` survivor
  cannot be told apart in a real payload, and the check stays as
  a defence.
- Not-reported names are padded to the longest name, not a fixed
  width.
- An unknown exclusion reason is printed as sent, and a detail
  the server left out is not printed.
- `excluded` uuids that are unknown, or are not hypervisors, are
  left to JSON.
- `--coverage` without `--all` is a usage error.
- A hypervisor with no `excluded` entry prints "no reason given
  (node state X)", not the old two-cause sentence.
