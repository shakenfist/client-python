# Reading a namespace key's metadata

A namespace key has carried an expiry, a scope list and a provenance
record for some time, and for a while none of it could be read back: the
key listing answered with names alone. A credential whose expiry and
scopes cannot be read cannot be audited or rotated on purpose, which is
what these reads are for.

The plan of record for this work is `PLAN-oidc-authentication.md` in the
`shakenfist` repository; this page describes what the client does with
it.

## The two reads

`get_namespace_keys(namespace)` returns one object per key, including
keys which have already expired. `get_namespace_key(namespace,
key_name)` returns the same object for a single key.

Each object carries `namespace`, `key_name`, `uuid`, `expiry` and
`scopes`. The key's secret is never returned.

`get_namespace_keynames(namespace)` is unchanged and still returns the
bare list of names, omitting expired keys. It is not gated, because it
predates the capability and works against any server.

## Provenance is served only to the system namespace

A key minted by a human login records the person it was minted for --
their subject identifier, the identifier of the token they presented and
the claims it satisfied. The audience which may read a namespace's keys
is wider than the namespace's own callers: it includes every namespace
the target has granted trust to. So the server serves `provenance` only
to a caller in the `system` namespace.

**A missing `provenance` describes the caller, not the key.** It means
the caller is not in `system`, and never that the key has no provenance.
Nothing in the client's output should be read as the latter.

## The capability gate

Both reads require the cluster to advertise `namespace-key-metadata`.
They fail closed: against an older server the methods raise
`IncapableException` rather than returning anything.

The gate matters more than it looks. A server without the capability
answers the same collection route with a list of names, because `detail`
is simply an unknown query parameter there -- so the request succeeds,
and a caller which did not check would be indexing into strings.

`sf-client namespace show` catches that refusal and prints the bare name
list instead, so the command keeps working against any cluster.

## Output

`namespace show` renders the metadata in all three output modes, with
one deliberate difference between them.

- **pretty** draws a table of name, expiry and scopes, with a
  `provenance` column when the field was served. An expired key says
  `(expired)` beside its timestamp, because that is the key an operator
  has to act on.
- **the default machine-readable output** names its columns
  (`keyname,name,expiry,scopes`) and gives the expiry as the raw unix
  timestamp the server sent, which carries no separator and which a
  script can compare against the current time without parsing a date.
  **Provenance is deliberately absent**: it is a nested object whose
  serialised form contains commas, and because it reaches some callers
  and not others its presence would change the number of columns
  depending on who asked. Read it with `--output json`.
- **json** adds a `key_details` field alongside the existing `keys`. The
  field is omitted entirely against a cluster without the capability,
  rather than appearing as a misleading empty list.
