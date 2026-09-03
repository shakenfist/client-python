# Plans index

Every planning document in this repository, oldest first.

The client is usually the small end of a change that is mostly
somewhere else, so most of what lands here is planned in another
repository. A plan appears below when this repository holds the plan
file, whether or not the plan of record is ours.

Status cells use the shared vocabulary the `plan-index` consistency
audit enforces across the fleet: `Proposed`, `Not started`,
`In progress`, `Blocked`, `Complete`, `Abandoned` or `Superseded`. A
status says whether the plan still wants attention and nothing else --
what happened lives in the plan file.

| Date | Plan | Intent | Status |
|------|------|--------|--------|
| 2026-07-20 | [VDI console tokens phase 4](PLAN-vdi-console-tokens.md) | The client-python side of a cross-repository plan whose plan of record is shakenfist's `PLAN-kerbside-vdi-tokens.md`: three apiclient methods for the token-minting endpoints, and a `sf-client instance vdiconsole` that lands the user in a session over the Kerbside proxy when the server advertises one | Complete |
| 2026-08-30 | [Agent operation deadlines phase 6](PLAN-agent-operation-deadlines-phase-06-client.md) | The client-python side of a cross-repository plan whose plan of record is shakenfist's `PLAN-agent-operation-deadlines.md`: deadlines and progress timeouts propagated from each await helper's own budget, `--deadline` and `--progress-timeout` on the three agent CLI verbs, and await loops that fail fast on a terminal operation state instead of polling to their timeout (client-python#363). The `agentoperation-deadlines` capability this gates on is shakenfist#4005, merged; until a cluster runs a server with it, the propagation here is inert by design | Complete |
