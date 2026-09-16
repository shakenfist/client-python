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

With the flag on, a refused request is retried when, and only when,
the body says the refusal is transient. In practice that is
`create_instance()`, since instance creation is what the scheduler
refuses. In particular:

- **An unmarked `507` is never retried.** An older server marks nothing,
  and a proxy's HTML error page parses as nothing. Neither is a promise
  that waiting will help, and the status code alone is not enough.
- **The wait comes from `Retry-After`**, clamped to between 1 and 60
  seconds so that a missing, broken or hostile header cannot park you.
  Only the integer seconds form of the header is understood; the HTTP
  date form falls back to the default of 15 seconds, which is what the
  Shaken Fist API sends anyway.
- **The retry is bounded by the call's deadline**, which is the
  `timeout` argument to `create_instance()` when you pass one, and
  otherwise comes from the client's async strategy: 3600 seconds for
  `ASYNC_BLOCK`, 60 for `ASYNC_PAUSE`, and none at all for
  `ASYNC_CONTINUE`, which means the caller is not waiting for anything
  and so never retries. The final wait is shortened so it cannot
  overshoot the deadline.
- **The request is replayed exactly.** If your caller needs a different
  instance name on each attempt, or wants to record how long it waited,
  retry in your own code instead of setting this flag.

Nothing else changes: a refusal which outlasts the deadline raises
`InsufficientResourcesException` just as it would have without the flag.
