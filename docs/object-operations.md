# Pending operations on an object

Most changes to an instance, network or artifact are carried out by
cluster operations which run after the API call that asked for them has
returned. The server can tell you which of those operations target an
object, and whether any of them are still pending. The client exposes
both.

## Listing the operations targeting an object

```python
ops = client.get_instance_clusteroperations(instance_ref)
ops = client.get_network_clusteroperations(network_ref)
ops = client.get_artifact_clusteroperations(artifact_ref)
```

Each returns a list of operations, newest first. By default the list
holds only operations which are still queued, in preflight or
executing; pass `all=True` to include finished operations which the
server has not yet removed. Pass `namespace=` to look up an object in
another namespace, as the matching `get_*` call does.

Every entry carries an `observational` flag. An observational operation
only reads the object, such as a checksum verification or a billing
sweep, and never changes it. A caller deciding whether an object is
busy should ignore those entries.

Each method needs its server capability (`instance-clusteroperations`,
`network-clusteroperations` or `artifact-clusteroperations`) and
raises `IncapableException` against a server which lacks it.

## Waiting for an object to settle

The views returned by `get_instance()`, `get_network()` and
`get_artifact()` carry `has_pending_operations`, which is true while any
operation which changes the object is queued, in preflight or executing.
Observational operations never make it true. The server also reports
true when it cannot tell, so a database outage reads as busy rather than
idle.

To wait for an object to settle:

```python
view = client.await_no_pending_operations('instance', instance_ref)
```

`object_type` is `instance`, `network` or `artifact`. The call polls the
object's view every `interval` seconds (default 1) until
`has_pending_operations` is false, and returns that view.

- `timeout` defaults to the client's async deadline, set by
  `async_strategy`. A negative timeout returns the current view without
  waiting; zero checks once and raises if the object is pending.
- It raises `TimeoutException` if the object is still pending when the
  timeout elapses, and `ResourceNotFoundException` if the object is
  deleted while it waits.
- It needs the `object-pending-operations` capability, and raises
  `IncapableException` without it, or if a view does not report the
  field at all.

The field is meant for waiting on one object. On a list of objects, a
server-side problem makes every entry report pending at once.
