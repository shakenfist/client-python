# Cluster capacity

`sf-client node capacity` shows, for each hypervisor, what the scheduler
believes the node has committed, what it measures, and what it will let
one more instance take. It exists to answer "this node looks idle but is
refusing work" -- which can only be diagnosed by seeing the two ledgers
the scheduler charges a node against side by side.

For how the scheduler admits an instance, what a capacity row is and how
namespace claims fit in, read the operator guide:
[scheduler.md](https://github.com/shakenfist/shakenfist/blob/develop/docs/operator_guide/scheduler.md).
This page only explains what the client prints.

All of these commands are cluster administrator operations. `node
capacity` and the two events commands also need a server which
advertises the matching capability; against an older server they print
the client's refusal message and exit 1.

## node capacity

```bash
sf-client node capacity          # every hypervisor
sf-client node capacity sf-4     # one node, by name or UUID
```

Pretty output is three tables -- CPU, memory and disk -- rather than one
wide one, because a row across all three dimensions does not fit a
terminal and defeats reading the CPU columns against each other. Every
table leads with the node name. This is real output (from the fixture the
tests use, so the node names are made up):

```
CPU (vCPUs; * marks what the scheduler admits against):
+--------------------------------------+-----------+----------+-------+----------+-----------+-------------+--------------------+-----------------+
| node                                 | committed | measured | limit | hard max | available | schedulable | load 1/5/15        | notes           |
+--------------------------------------+-----------+----------+-------+----------+-----------+-------------+--------------------+-----------------+
| sf-1                                 | 24*       | 18       | 90*   | 90.0     | 66.0      | 30          | 2.41 / 2.13 / 1.87 |                 |
| sf-2                                 | 1         | 2*       | 3*    | 3.0      | 1.0       | 1           | 0.9 / 0.8 / 0.7    | cpu clamped     |
| sf-3                                 | 0         | 6*       | -     | 90.0*    | 84.0      | 30          | 2.41 / 2.13 / 1.87 | no capacity row |
| sf-4                                 | 72*       | 64       | 60*   | 60.0     | -12.0     | 30          | 21.3 / 19.8 / 18.2 | over limit      |
| sf-5                                 | n/a       | 10       | n/a   | 90.0     | 80.0      | 30          | 0.5 / 0.4 / 0.3    |                 |
| 00000000-0000-4000-8000-000000000099 | 4*        | 4*       | 90*   | 90.0     | 86.0      | 30          | 2.41 / 2.13 / 1.87 |                 |
+--------------------------------------+-----------+----------+-------+----------+-----------+-------------+--------------------+-----------------+

Memory (MB):
+--------------------------------------+-----------+-----------+----------+----------+------------------+-----------------+
| node                                 | committed | available | max      | reserved | max per instance | notes           |
+--------------------------------------+-----------+-----------+----------+----------+------------------+-----------------+
| sf-1                                 | 40960     | 344064    | 393216.0 | 8192     | 81920            |                 |
| sf-2                                 | 2048      | 1024      | 16384.0  | 14336    | 1024             | memory clamped  |
| sf-3                                 | 0         | 380928    | 393216.0 | 8192     | 81920            | no capacity row |
| sf-4                                 | 405504    | -12288    | 393216.0 | 8192     | 81920            | over limit      |
| sf-5                                 | 40960     | 344064    | 393216.0 | 8192     | 81920            |                 |
| 00000000-0000-4000-8000-000000000099 | 40960     | 344064    | 393216.0 | 8192     | 81920            |                 |
+--------------------------------------+-----------+-----------+----------+----------+------------------+-----------------+

Disk (GB):
+--------------------------------------+-----------+-------+-----------+-----------------+
| node                                 | committed | limit | available | notes           |
+--------------------------------------+-----------+-------+-----------+-----------------+
| sf-1                                 | 700       | 1900  | 1780.0    |                 |
| sf-2                                 | 20        | 100   | 80.0      |                 |
| sf-3                                 | 0         | -     | 1780.0    | no capacity row |
| sf-4                                 | 1950      | 1900  | -15.0     | over limit      |
| sf-5                                 | n/a       | n/a   | 1780.0    |                 |
| 00000000-0000-4000-8000-000000000099 | 700       | 1900  | 1780.0    |                 |
+--------------------------------------+-----------+-------+-----------+-----------------+

Total available: 317.0 vCPUs, 1414144 MB RAM

Hypervisors not reported by the scheduler (queue length or stale metrics):
    sf-6
    sf-7
```

### The CPU columns

| Column | What it is |
| --- | --- |
| committed | vCPUs the scheduler's ledger has promised to instances placed on the node (`cpu_committed`) |
| measured | vCPUs of running libvirt domains, as the node last reported them (`cpu_measured`). The node republishes this only about once a minute, so it lags a ledger that has just been filled |
| limit | The ceiling in the node's capacity row (`cpu_limit`) |
| hard max | The ceiling derived from the node's schedulable threads and the overcommit ratio (`cpu_hard_max`) |
| available | What the server computed as left over (`cpu_available`) |
| schedulable | Threads left after the node's reservation (`cpu_schedulable`) |
| load 1/5/15 | The node's load averages |

Committed, measured, limit and hard max are never merged into one "used"
column. They are different ledgers, and which one binds is the whole
diagnosis.

### What the asterisk means

A `*` marks the figures the scheduler admits a new instance against. The
rule, as the server implements it:

- The **charge** is the larger of measured and committed. If the two tie,
  both are marked.
- The **ceiling** is the capacity row's own limit when the node has a
  row, and hard max only when it does not. The limit *replaces* hard max:
  the two are never ceilings together, so on a node with a row, hard max
  is shown but never marked.
- A node without a row is charged nothing committed, so its charge is its
  measurement and its ceiling is hard max.

An instance is refused when the charge plus what it asks for exceeds the
ceiling. In the output above, sf-1 is bound by its committed figure
against its limit, sf-2 by its measurement against its limit, and sf-3,
which has no row, by its measurement against hard max.

The client copies this arithmetic from the server's scheduler, because
the response does not say which figure bound. If a payload lacks any of
the inputs, nothing is marked: a wrong marker sends you to the wrong
ledger, which is worse than none.

### Notes

| Note | Meaning |
| --- | --- |
| `no capacity row` | The node has no row in the capacity ledger, so there is no committed figure and no limit. The scheduler measures it against hard max, and admission then lets it through unguarded. Typical of a node mid-upgrade or one the reconciler has not sized |
| `cpu clamped` / `memory clamped` | The node's reservation was over-large and a safety clamp engaged, so `schedulable` (or `reserved`) is a floor or cap rather than the configured arithmetic. The configuration is the thing to fix |
| `over limit` | The matching `available` figure is negative: the node holds more than its limit. Shown on whichever table it applies to |

### n/a and -

These are different on purpose. `n/a` means the key was missing from the
response: an older server never sent it, so the client does not know.
`-` means the server sent the key with no value, for example `limit` on
a node without a row. The client never substitutes zero for either. In
the output above sf-5 is a node from an older server's dialect.

### Totals and the degraded banner

The line under the tables gives the cluster's total available vCPUs and
memory. If the scheduler could not read its capacity counters, a banner
is printed above everything:

```
WARNING: the scheduler could not read the capacity counters. Every committed figure
below reads as zero, and every limit as -, because of that failed read and not because
the nodes are idle. Read it again before acting on these figures.
```

A degraded read makes every committed figure zero and every node look
like it has no capacity row, so the figures mean nothing until the read
is repeated.

### Hypervisors that are not reported

Hypervisors missing from the scheduler's response are listed under the
tables and never dropped:

```
Hypervisors not reported by the scheduler (queue length or stale metrics):
    sf-6
    sf-7
```

The sentence names both causes -- the node's queue was too long, or its
metrics were stale or absent -- because the response cannot tell them
apart and the client will not guess. The busiest node in the cluster is
plausibly one of these, so silently omitting them would hide exactly the
node you are looking for. Nodes which are not hypervisors are omitted.

### One node

Naming a node prints its three tables and nothing cluster-wide (no totals
and no not-reported list). It exits 1 if the name is unknown, if the node
is not a hypervisor, or if it is a hypervisor the scheduler did not
report.

### Simple and JSON output

Simple output is one CSV row per node, with every figure the server
sends in the server's order and `capacity_degraded` repeated on each
row, so it can be read by position. For a whole-cluster view, a
`not_reported,name,uuid,reason` section follows the rows, one line per
missing hypervisor; the reason is `n/a` because the server never sends
one. Naming a node omits that section. Real output for one node:

```
name,uuid,capacity_degraded,cpu_max_per_instance,cpu_schedulable,cpu_reservation_clamped,memory_reservation_clamped,cpu_committed_row_present,cpu_limit,cpu_hard_max,cpu_measured,domains_enumerated_at,cpu_committed,cpu_available,cpu_load_1,cpu_load_5,cpu_load_15,memory_reserved_mb,ram_max_per_instance,ram_max,ram_committed,ram_available,disk_available,disk_limit_gb,disk_committed_gb,instances_total,instances_active
sf-2,00000000-0000-4000-8000-000000000002,False,32,1,True,True,True,3,3.0,2,1792886390.12,1,1.0,0.9,0.8,0.7,14336,1024,16384.0,2048,1024,80.0,100,20,1,1
```

JSON output is the server's response unchanged, keyed by node UUID, with
no names joined in. When a node is named, `per_node` is cut down to it
and the rest of the response keeps its shape.

## node resources

```bash
sf-client node resources sf-1
```

This prints the node's own last raw `resources` event: what the node
measured about itself. It is not what the scheduler admits against, and
it knows nothing of the committed ledger. Use `node capacity` for
admission. `node resources` exits 1 with `No resources event found` when
the node has published none.

## node show: daemon states

`node show` also lists each daemon's state under `Daemon states:`, sorted
by name, with `-` for a daemon whose state is empty:

```
Daemon states:
    agent   : degraded
    cluster : -
    sf-api  : running
```

Simple output prints them as `daemon,<name>,<state>` rows, and JSON
output already carried them.

## Events

```bash
sf-client namespace events ci [-t TYPE] [-l LIMIT]
sf-client namespace claim events ci <claim-uuid> [-t TYPE] [-l LIMIT]
```

Both are rendered as `node events` is: timestamp, node, duration,
message and extra, in pretty, simple and JSON modes. Against a server
that lacks the `namespace-events` or `namespace-claim-events` capability
they print the client's refusal and exit 1.

### Calibrate from the namespace, not the claim

A claim's events are removed with the claim. Once a claim has been
deleted or replaced, `namespace claim events` for it has nothing to
show, and the claim's own history is gone with it. The namespace's events
outlive its claims, and a claim's deletion is recorded there. So when you
are sizing the next claim from what the last one did, read
`namespace events`. Use `namespace claim events` for a claim which still
exists. To see the high water mark of a live claim, use `namespace claim
show` (see [namespace-claims.md](namespace-claims.md)).
