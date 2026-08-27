# Namespace capacity claims

A capacity claim is a namespace's declaration of how much aggregate
cluster capacity it expects to hold at once. The cluster accounts the
namespace's placements against the claim, and will not promise capacity
it does not have -- so creating or growing a claim can be refused when
the cluster is full.

Claims are administered with `sf-client namespace claim`, and are
cluster administrator operations.

> **In the release this API first ships in, exceeding a claim is
> recorded as an audit event rather than refused.** A claim is
> accounting, not enforcement. Do not treat it as a quota that will stop
> a namespace overrunning; treat it as the cluster's record of what the
> namespace asked to be able to hold.

## Creating a claim

```bash
sf-client namespace claim create ci \
    --cpus 40 --memory-mb 81920 --disk-gb 2000 --expires-in 86400
```

All four options are required, and the command prints the new claim's
UUID.

A namespace holds at most one active claim. Asking for a second is
refused with a conflict, even if the request would otherwise have fit --
delete the existing claim first.

### The expiry is a duration, not a time

`--expires-in` is seconds from now, not a timestamp, and that is
deliberate. The expiry is computed from the cluster's clock, which is
the only clock the expiry sweep ever compares against. Converting a
local time into a duration on your workstation would fold your clock
skew into the answer, producing a claim that expires at a different
moment than you asked for -- intermittently, and only on machines whose
clock has drifted.

There is no way to create a claim that never expires. This is the
server's constraint, not the CLI's.

## Reading claims

```bash
sf-client namespace claim list ci
sf-client namespace claim show ci <claim-uuid>
```

Both views report **two** separate states, and they answer different
questions:

| Field | Question it answers | Values |
| --- | --- | --- |
| `state` | Does the claim object exist? | `created`, `deleted` |
| `coverage_state` | Is the claim currently covering placements? | `active`, `expired` |

An expired claim reads as `state: created, coverage_state: expired`. It
still has a row, it is no longer covering anything, and **it does not go
away on its own** -- an operator has to delete it. That combination is
the only thing that explains why a namespace's placements stopped being
charged to a claim, which is why the CLI never collapses the two fields
into a single status column.

The list view also shows the drawdown in each dimension as `used /
limit`, so `12 / 40` is twelve of forty claimed vCPUs placed.

## Changing a claim

```bash
# Re-date a claim without touching its limits.
sf-client namespace claim update ci <claim-uuid> --expires-in 86400

# Grow one dimension, leaving the others alone.
sf-client namespace claim update ci <claim-uuid> --cpus 48
```

**Only the dimensions you name are changed.** The server reads the
request body as a field mask, so an update that names only an expiry is
a re-date and nothing more. Do not read a claim and send all four values
back: that turns a re-date into a resize, and races whatever else is
moving the claim.

A claim cannot be shrunk below what it is already using, and an expired
claim cannot be changed at all -- delete it and create a new one.

## Deleting a claim

```bash
sf-client namespace claim delete ci <claim-uuid>
```

The claim's capacity returns to the cluster immediately. Deletion is not
a soft delete: the row is removed, and the command returns the claim as
it was immediately before it went.

## What the refusals mean

Claims answer three refusal statuses, and telling them apart matters
because two of them are worth retrying and one is not.

| Status | Meaning | What to do |
| --- | --- | --- |
| 409 Conflict | The namespace already holds an active claim, the requested limit is below what the claim is already using, or the claim is no longer active | Change the request -- retrying it unchanged will fail the same way |
| 507 Insufficient Storage | The cluster does not have the capacity to promise this claim | Ask for less, or wait for capacity to be released |
| 503 Service Unavailable | The cluster capacity accounting has not been built yet, or the claim was being changed concurrently and the update gave up | Retry. Nothing about the request is wrong |

The `503` is the one to be careful about: it is a *transient* refusal,
and a caller that reads it as a durable one abandons a claim it could
have had a second later.

Note that an over-large claim against a namespace which already holds
one answers `409`, not `507` -- the "already holds a claim" check is
evaluated first. The `507` only appears for an over-large claim on a
namespace with no claim.

## Using the API directly

The `Client` class exposes the same five verbs:

```python
from shakenfist_client import apiclient

c = apiclient.Client()
claim = c.create_namespace_claim('ci', 40, 81920, 2000, 86400)

c.get_namespace_claims('ci')
c.get_namespace_claim('ci', claim['uuid'])
c.update_namespace_claim('ci', claim['uuid'], expires_in_seconds=86400)
c.delete_namespace_claim('ci', claim['uuid'])
```

`update_namespace_claim()` sends only the arguments you actually pass,
for the field mask reason above. Passing nothing sends an empty body,
which the server rejects -- the library will not guess at what you
meant.

A `503` raises `apiclient.ServiceUnavailableException`, so the retryable
refusals can be caught apart from durable ones:

```python
try:
    c.create_namespace_claim('ci', 40, 81920, 2000, 86400)
except apiclient.ServiceUnavailableException:
    ...  # transient, retry
except apiclient.InsufficientResourcesException:
    ...  # 507, the cluster is full
```

The client does not retry a `503` for you. Only the `406`
dependencies-not-ready case is retried internally, against the request
deadline.
