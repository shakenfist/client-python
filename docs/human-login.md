# Logging in as a person

Shaken Fist authenticates a namespace and a key, and nothing else. A
person who has an identity at an identity provider the cluster trusts
can exchange that identity for the namespace keys it entitles them to,
instead of being handed a key by an operator and keeping it in a
configuration file. `sf-client login` runs that exchange, caches what
the cluster minted, and `sf-client logout` discards it again.

The plan of record for this work is `PLAN-oidc-authentication.md` in the
`shakenfist` repository; this page describes what the client does with
it.

## The two flows

A login happens at the provider, not here, and there are two ways to get
a person in front of it. `sf-client login` picks one and `--flow`
overrides the choice.

**The device authorization flow** (`--flow=device`) prints a short code
and a URL. You open the URL wherever you have a browser -- another
window, your phone -- enter the code, and the client polls the provider
until it says the login happened. It asks nothing of the machine running
`sf-client`: no browser, no listening port, no redirect registered at
the provider. That is why it is preferred wherever a provider offers it,
and why it is the flow that works over ssh and inside a container.

**The authorization code flow with PKCE** (`--flow=code`) opens a
browser here, listens on a loopback port for the redirect the provider
sends back, and exchanges the authorization code in it for an identity
token. It is the fallback for a provider which advertises no device
authorization endpoint, and it is nicer where a browser exists because
nobody has to transcribe anything.

The automatic choice prefers the device flow, falls back to the browser
flow where a provider offers only that, and refuses rather than opening
a browser which is not there. `--flow` is an override and not a hint: a
flow you asked for is attempted even where the automatic choice would
have avoided it, and if the browser cannot be opened the authorization
URL is printed for you to open yourself.

A cluster which trusts more than one login-capable provider asks which
one to use. A cluster which offers none says so and exits non-zero:
human login is something an operator configures, and an empty list is a
complete answer rather than a failure to look.

The browser flow's redirect is guarded in three ways worth knowing
about, because each of them can end a login that looked fine. The
listener binds `127.0.0.1` only and serves one request. Each login
generates a fresh `state` parameter and a redirect which does not carry
that exact value is refused, its contents unused -- so a stale browser
tab, a bookmarked redirect, or anything else sending redirects to that
port stops the login rather than completing it with somebody else's
authorization code. And the wait is bounded: five minutes for the
redirect to arrive, ten seconds for a connection which has arrived to
send a request.

Neither flow needs an existing credential, but both need to know which
cluster to talk to. Name it with `--apiurl` or `SHAKENFIST_API_URL` if
this host has no configuration at all.

## Where the credentials go

The cluster's answer to a login is every credential the identity
entitles its holder to, each with its own namespace, key name, expiry
and scopes. Each secret is returned once and never again, so the client
writes them to a cache:

```
$XDG_CONFIG_HOME/shakenfist/credentials.json
```

or `~/.config/shakenfist/credentials.json` where that variable is not
set. The file is mode `0600` and the directory `0700`. Both are created
with those modes rather than created and then corrected, because a
create-then-`chmod` leaves a window in which the credential is readable
by everybody on the machine.

What was written owner-only is checked again every time it is read.
Reading the cache refuses, with a message naming the file, rather than
quietly carrying on, when:

- the file's mode grants group or other *any* access -- read, write or
  execute;
- the directory holding it is group or other writable, which would let
  somebody else substitute a whole credential file of their own;
- the file is corrupt or truncated;
- the file declares a format version only a newer client understands,
  in which case it is left alone rather than overwritten, because the
  client which wrote it may be keeping credentials in fields this one
  cannot see.

Each of those is a refusal and not a fallback, because all of them are
indistinguishable from "nobody has logged in" to anything which only
checks whether a credential came back.

A credential whose expiry has passed is dropped when the cache is read
and is never presented. The cluster stops honouring a key the moment its
expiry passes, so offering one would turn a lapsed login into an
unexplained authorisation failure on the next command instead of a
prompt to log in again. Reading does not rewrite the file, so an expired
entry goes when the cache is next written: by another login, by
switching the selection, or by `logout`, which removes the file
outright.

A namespace key is scoped to the cluster which minted it, so each entry
records the API URL it came from, and the selection names a cluster as
well as a namespace. `ci` on two clusters is two different namespaces
holding two different keys. Logging in to one cluster replaces that
cluster's entries and leaves the others alone.

Nothing else is cached. The identity token is spent at the exchange and
kept nowhere, and neither the provider's access token nor its refresh
token is ever written down: those are credentials this cluster cannot
revoke, where a namespace key is one it can.

## Which credential a command uses

The client has always looked for its credentials in four places, in
order, and the cache is now the third of five:

1. command line flags (`--apiurl`, `--namespace`, `--key`);
2. environment variables (`SHAKENFIST_API_URL`,
   `SHAKENFIST_NAMESPACE`, `SHAKENFIST_KEY`);
3. the credential cache a login wrote;
4. `~/.shakenfist`;
5. `/etc/sf/shakenfist.json`.

As before, each source is consulted only while no API URL has been
established yet -- that is the pre-existing rule, and it is why the
cache records which cluster minted each credential, since a source which
could not supply the URL would have nowhere to join this chain.

**The cache beats both configuration files, and this is a change in
behaviour.** A person who has just logged in expects the next command to
use that login; if a `~/.shakenfist` written a year ago won instead, the
login would appear to have done nothing and the failure would be an
unexplained `Not authenticated` -- the cluster refuses a stale key with
a `401` -- rather than a message. The cost is deliberate and
worth naming: an operator who has a `~/.shakenfist` and logs in once
keeps using the login until they log out. `sf-client logout` restores
the previous behaviour exactly.

Flags and the environment still win, because they are equally explicit
and they are how a script or a CI job pins a credential. A job which
sets `SHAKENFIST_KEY` must not start running as somebody's personal
login because they happened to log in on the same machine.

Two things follow from that, and neither is quite what the gating rule
above would suggest on its own.

**The environment's namespace and key are read whether or not the
environment also names the cluster.** `SHAKENFIST_NAMESPACE` and
`SHAKENFIST_KEY` pin a credential; where the cluster comes from is a
separate question, and it may come from `--apiurl`, from a
configuration file or from the cache. The cache then supplies the
cluster and nothing else. This is what `sf-client` has always done,
because click injects both variables as arguments however the cluster
was named -- the point of saying it here is that a library caller such
as the ansible collection now behaves the same way. Before, an
automated caller which exported only those two variables was handed the
cache's identity instead of its own.

**Both halves of a credential have to be missing before any source
fills one in.** A caller or an environment which named a namespace or a
key is pinning a credential, and completing the other half of it from a
login would present a pair that nothing ever had. The visible
consequence is that a half-named credential stays half-named and the
command fails to authenticate, which is the honest outcome: the
alternative is one person's key presented against another person's
namespace. The two configuration files are the exception, and keep
their long-standing per-half behaviour.

A caller which named a cluster but no credential -- `--apiurl` or
`SHAKENFIST_API_URL` on its own -- used to reach none of the sources
above, because each of them is gated on there being no API URL yet, and
so had no credential at all. Such a caller now gets the environment's
credential if it has one, and otherwise the cache's, for that named
cluster only.

## Seeing and switching what you hold

```bash
sf-client credential list
sf-client credential list --offline
sf-client credential select <namespace> [--key-name <name>]
```

`credential list` prints each cached credential with its cluster,
namespace, key name, expiry and scopes, and marks the one in use. Where
the cluster advertises the key metadata capability, each row's expiry
and scopes are read live from the cluster rather than trusted from the
cache, and the last column says which was used and -- when it was the
cache -- why. `--offline` asks no cluster at all. No secret is printed,
in any of the three output modes.

`credential select` chooses which credential is presented by default.
Neither verb involves the cluster to do its own job, so both work with
it down.

A login which minted exactly one credential selects it. A login which
minted several selects the one in the namespace you asked for with
`--namespace`, and otherwise selects nothing and says what there is to
choose from: a command names an object rather than a namespace, so
there is no way to work out which credential it wanted, and guessing
would run it somewhere you did not ask for.

## Logging out

```bash
sf-client logout
```

`logout` removes the whole cache file -- not just the selection -- and
says how many credentials went with it. A credential which survives a
logout the person asked for is a secret still sitting on their disk and
still honoured by the cluster.

No cluster is involved: logging out is the removal of a local file, and
it makes no request. That is deliberate, and so is the other half of it.
`logout` works when the cache is in a state every other verb refuses --
corrupt, from a newer client, or with a mode that exposed a key --
because those are exactly the states it is the remedy for, and a logout
which needed to resolve a credential first would be unavailable
precisely when somebody needs it.

Keys the cluster minted are not revoked by logging out. They expire on
the cluster's own schedule, and an operator can delete them sooner.

## What the cluster records about a login

A key minted by a login is not anonymous. The cluster records the
provenance of each one: the subject identifier of the person who logged
in, the identifier of the token they presented, the mapping rule which
granted them the namespace, the issuer which signed the token, and the
claims that rule was satisfied by. That is the audit trail for a
credential nobody typed out by hand.

**You will not see it in your own namespace's key listing.** The
audience which may read a namespace's keys is wider than the namespace's
own callers, so the cluster serves `provenance` to a caller in the
`system` namespace alone -- which is to say, to an administrator, and
not to the person who logged in or to anyone else sharing their
namespace. A missing `provenance` field describes the caller and never
the key. See [namespace-keys.md](namespace-keys.md) for the fields those
reads return and the capability they are gated on.

## When a login cannot be completed

Both halves of human login are advertised separately by the cluster, and
the client checks both before sending anybody to their provider: a
cluster which can list identity providers but cannot exchange a token
for credentials is refused up front, rather than after a person has
authenticated and is holding a token nothing can spend.

Two refusals are worth recognising because they are not about you:

- A cluster which answers that this identity satisfies more of its
  mapping rules than one login may mint credentials for has minted
  none of them. Trying again will not help; an operator has to narrow
  the rules which match the identity, or raise the cluster's limit.
- A cluster which refuses the identity outright is saying either that
  no mapping rule grants it a namespace or that the token did not
  validate against the issuer it names. The login itself worked -- the
  provider issued a token -- so what is missing is on the cluster, and
  its log says which of the two it was.

No secret is printed or logged by any of this, at any log level or in
any output mode: not the identity token, not the device code, not the
PKCE verifier or the authorization code, and not the namespace keys the
cluster mints. The one deliberate exception is the authorization URL,
printed only where a browser could not be opened, which is what a
browser has to receive for the flow to happen at all.
