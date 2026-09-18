# Waiting out a transient capacity refusal

When the scheduler cannot find a home for an instance, `POST /instances`
answers `507 Insufficient Storage` and the client raises
`InsufficientResourcesException`. Some of those refusals are worth
waiting out -- the cluster is momentarily full, and another tenant's
instance is being torn down right now -- and some are not, because the
request will never fit on this cluster at all.

The server tells the two apart. A refusal it expects to pass carries:

- a `Retry-After` response header, and
- `"transient": true` and a `"stage"` naming where in scheduling the
  request was refused, in the JSON body.

Both are readable from the exception:
`APIException.headers['Retry-After']` and `json.loads(e.text)`.

## The opt-in retry

The client can wait for you:

```python
from shakenfist_client import apiclient

client = apiclient.Client(retry_transient_capacity=True)
```

The flag defaults to **off**, and off is the right default: a `507` is a
refusal, and a caller which has not asked to wait should hear about it
immediately rather than discover that a single call quietly spent a
minute inside the library.

With the flag on, a refused request is retried when, and only when, the
body says the refusal is transient. The gate is the server's marker, not
a list of endpoints the client keeps: any request the server marks is
retried. Today that means `create_instance()`, because instance creation
is what the scheduler refuses, but nothing here is restricted to it. In
particular:

- **An unmarked `507` is never retried.** An older server marks nothing,
  and a proxy's HTML error page parses as nothing. Neither is a promise
  that waiting will help, and the status code alone is not enough.
- **The wait comes from `Retry-After`**, clamped to between 1 and 60
  seconds so that a missing, broken or hostile header cannot park you.
  Only the integer seconds form of the header is understood; the HTTP
  date form falls back to the default of 15 seconds, which is what the
  Shaken Fist API sends anyway.
- **There are two bounds: five attempts, and the call's deadline.**
  Whichever is reached first raises the refusal the server sent, so you
  see the same exception you would have seen with the flag off. The
  attempt cap is there because each replay costs the cluster a discarded
  instance record (see the last bullet), and a deadline alone is a poor
  bound on that: an `ASYNC_BLOCK` client with no `timeout` has an hour,
  which at the server's fixed 15 seconds would be around 240 of them
  from one library call. Five is what a 60 second `ASYNC_PAUSE` budget
  could already spend, so it costs no waiting strategy an attempt. At
  the server's 15 seconds it means a call waits at most about a minute
  for capacity.
- **The deadline itself** is the `timeout` argument to
  `create_instance()` when you pass one, and otherwise comes from the
  client's async strategy: 3600 seconds for
  `ASYNC_BLOCK`, 60 for `ASYNC_PAUSE`, and none at all for
  `ASYNC_CONTINUE`, which means the caller is not waiting for anything
  and so never retries. The final wait is shortened so that no sleep
  overshoots the deadline -- though the attempt which follows it is a
  request like any other, so the call itself can still return a round
  trip after its deadline.
- **`create_instance()` spends one budget on both halves of the call.**
  Its `timeout` bounds waiting out the refusal *and* the subsequent wait
  for the instance to leave `initial`/`creating`, so a caller that wants
  both must size it to cover both. An exhausted budget does not raise
  there: the wait gives up and returns the instance while it is still in
  a transitional state, which a caller cannot tell apart from success.
  Enabling this flag makes that return more likely, because the capacity
  wait is spent before the instance exists at all. If that matters, size
  `timeout` for the capacity wait and follow the call with an explicit
  `await_instance_create()`, which is its own budget on its own
  condition. Note that `timeout=0` -- the value the `create_instance()`
  docstring recommends before an explicit await -- disables this retry
  too, since it makes the deadline the moment of the call.
- **The request is replayed exactly.** If your caller needs a different
  instance name on each attempt, or wants to record how long it waited,
  retry in your own code instead of setting this flag.
- **Each refused attempt costs a discarded instance record.** The server
  creates the instance object and allocates its addresses before the
  scheduler runs, so a refusal moves that object to an `-error` state
  and queues it for deletion asynchronously. A replay is a new instance
  with a new UUID, not a retry of the old one, so N attempts leave N-1
  errored instances behind for the cluster to reap -- at most four,
  given the attempt cap. Two consequences
  worth knowing: the replay cannot collide with its own earlier attempt
  (instance names need not be unique, and the UUID is the server's to
  assign), and an instance list taken during a long wait shows the
  failures.

  What those records do *not* hold is capacity. The server charges its
  capacity ledger in the same database transaction that records a
  placement, and a refusal rolls that transaction back, so an instance
  which was never placed has nothing to release and the next attempt
  faces the same cluster the last one did. Waiting is therefore not
  self-defeating -- but the records do hold their allocated addresses
  until the deletion is processed, so a long wait against a small
  network can exhaust its address space before it runs out of budget.
- **The server may only mark a refusal which had no partial effect.**
  The client replays the request byte for byte on the strength of the
  marker alone, and it does not restrict which endpoint may carry one.
  So `"transient": true` is a promise by the server that nothing the
  request asked for was committed before it was refused. Today only the
  scheduler refusal is marked, and that promise holds there. Anything
  which appends or mutates -- `send_upload()`, whose natural refusal is
  also a `507` -- must not be marked without making the replay safe
  first.

Nothing else changes: a refusal which outlasts the deadline raises
`InsufficientResourcesException` just as it would have without the flag.

## There is no CLI equivalent

This is a library flag. `main.py` builds its client without it, so
`sf-client instance create` reports `Insufficient Resources` and exits
non-zero on a transient refusal exactly as it always has. Run the
command again, or wait the refusal out from Python.
