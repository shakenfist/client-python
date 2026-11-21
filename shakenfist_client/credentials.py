"""The on disk credential cache for human logins.

A human login mints namespace keys, and the person who ran it expects
the next command to use them without logging in again. So this client
has to write a credential to disk, which nothing in this package has
done before -- the rules in this module are the whole of what protects
it at rest.

The file is JSON at ``$XDG_CONFIG_HOME/shakenfist/credentials.json``,
or ``~/.config/shakenfist/credentials.json`` when that variable is
unset. It cannot live under ``~/.shakenfist``, because that path is
already a *file*: one of the client's configuration sources, which a
directory of the same name could not coexist with.

The format, in full::

    {
        "version": 1,
        "credentials": [
            {
                "apiurl": "https://sf.example.com/api",
                "namespace": "ci",
                "key_name": "corporate-aaaa",
                "key": "...the secret...",
                "expiry": 1763000000.0,
                "scopes": ["blob.read", "artifact.*"]
            }
        ],
        "current": {
            "apiurl": "https://sf.example.com/api",
            "namespace": "ci",
            "key_name": "corporate-aaaa"
        }
    }

A key's name is the issuer's name and a random suffix, which the cluster
chooses; the suffix is written plainly here because a credential scanner
reads this file, and a realistic-looking one reads as a leak whether or
not it ever unlocked anything.

An entry is a credential as the cluster's mint returned it, plus the
``apiurl`` it was minted against. That addition is load bearing twice
over. A namespace key is scoped to the cluster which issued it, so an
entry with no record of its cluster could be presented to a different
one; and the client's credential resolution is gated on not yet having
an API URL rather than on not yet having a key, so a cache which could
not supply the URL would have nowhere to join that resolution.

``current`` is the selection, and it names a cluster as well as a
namespace because a namespace name means nothing on its own -- "ci" on
two clusters is two different namespaces holding two different keys.
Nothing here infers a selection from a request: a request names an
object, not a namespace, so the choice has to be recorded.

``credentials`` may hold entries for several clusters at once. A fresh
login against one cluster replaces that cluster's entries and leaves
the others alone, because each secret the mint returns is returned
once and never again -- so the set is all there is, but only for the
cluster it came from.

What is never cached: an identity token, which is spent at the
exchange, and an IdP refresh token, which is a credential whose
revocation this cluster does not control.
"""

import json
import os
import stat
import tempfile
import time


# The format version written into the file. A file declaring a higher
# version was written by a newer client which may understand fields
# this one would silently drop on the next write, so reading one is a
# refusal rather than a best effort.
CACHE_FORMAT_VERSION = 1

# A credential at rest is readable only by its owner, and the
# directory holding it is traversable only by its owner. Both are
# created with these modes rather than created and then chmod'ed: a
# create-then-chmod leaves a window in which the file is readable by
# everyone, and closing that window is the entire point of the mode.
#
# A mode argument is masked by the process umask, which can only clear
# bits and never set them, so neither of these can come out more
# permissive than it reads here -- a hostile umask makes the result
# stricter (0077 is already a no-op against owner-only bits, and 0177
# would leave the file read only, which does not matter because it is
# written through a descriptor opened before the mode is visible).
# Nothing here relies on that reasoning alone: _write_atomically()
# re-reads the mode of the file it has just put in place and refuses
# if it is not owner-only.
CACHE_DIRECTORY_MODE = 0o700
CACHE_FILE_MODE = 0o600

# How a write names its temporary file. Shared by the write which
# creates one and the logout which sweeps up any left behind, because
# two spellings of this pattern would mean a logout which silently
# missed the files it exists to remove.
TEMPORARY_PREFIX = '.credentials-'
TEMPORARY_SUFFIX = '.tmp'

# Reading refuses a file which grants *any* of the six group or other
# bits, not just the read bits. A group readable file leaks the key; a
# group writable one lets someone else substitute a key of their
# choosing, which is worse; and an execute bit on a JSON file means
# the mode was not set by anything that understood what the file is.
# So all six are a refusal and only 0600, 0400 and 0000 pass.
UNSAFE_FILE_BITS = stat.S_IRWXG | stat.S_IRWXO

# The directory is held to a weaker standard than the file: only group
# or other *write* is refused, which is the bit that would let someone
# else replace the file wholesale. Group or other read and execute on
# the directory disclose a file name, not a secret, and a 0755
# directory is what a user's umask will have produced for anything
# else under ~/.config.
UNSAFE_DIRECTORY_BITS = stat.S_IWGRP | stat.S_IWOTH


class CredentialCacheException(Exception):
    """The cache exists but cannot be used, and saying so is the answer.

    Every refusal in this module is this exception carrying a message
    naming the file: an unsafe mode, a corrupt or truncated file, a
    file from a format version this client does not know. None of
    those may be handled by quietly carrying on without the cache,
    because all three are indistinguishable from "you are not logged
    in" to a caller which only checks whether a credential came back.
    """

    ...


class CredentialCacheUnusable(CredentialCacheException):
    """The cache holds nothing a login has to preserve.

    A corrupt or truncated file, one in a format version older than
    this client understands, or one whose mode exposed its contents to
    other users. Reading still refuses, because a caller asking what
    the cache holds must not be told "nothing" when the answer is
    "something unreadable". But a login which has just minted
    credentials may replace such a file: what was in it could not be
    used, or should not be trusted, and the secrets now in hand will
    never be issued again.

    A refusal which is not this one is not replaceable. A cache from a
    *newer* client is the case that matters -- it may hold credentials
    in fields this client cannot see, so overwriting it would discard
    them -- and a directory other users can write to is not made safe
    by putting a fresh secret into it.
    """

    ...


def credentials_path(path=None):
    """Where the cache lives.

    An explicit path wins, which is how a caller (and every test here)
    points this module somewhere other than a real home directory.
    """
    if path:
        return path

    config_home = os.environ.get('XDG_CONFIG_HOME')
    if not config_home:
        config_home = os.path.join(os.path.expanduser('~'), '.config')
    return os.path.join(config_home, 'shakenfist', 'credentials.json')


def _check_directory_mode(directory):
    """Refuse a directory anyone else could put a file into."""
    mode = stat.S_IMODE(os.stat(directory).st_mode)
    if mode & UNSAFE_DIRECTORY_BITS:
        raise CredentialCacheException(
            'Refusing to use the credential cache in %s: the directory\'s mode is %04o, which lets group or '
            'other replace the credentials in it. Run "chmod 700 %s".'
            % (directory, mode, directory))


def _ensure_directory(directory):
    """Create the cache's directory, owner-only, if it is missing.

    Only the last element is created with a restricted mode. The
    parents -- ~/.config, normally -- are shared with every other tool
    on the machine and are none of this client's business, so they are
    created with whatever the umask says if they are missing at all.
    """
    parent = os.path.dirname(directory)
    try:
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
        os.mkdir(directory, CACHE_DIRECTORY_MODE)
    except OSError as e:
        if not isinstance(e, FileExistsError):
            raise CredentialCacheException(
                'Could not create the credential cache directory %s: %s' % (directory, e))
        # Someone else's directory, or one from an earlier login. Its
        # mode is not ours to change, but it is ours to refuse.
        _check_directory_mode(directory)
        return

    mode = stat.S_IMODE(os.stat(directory).st_mode)
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise CredentialCacheException(
            'Refusing to use the credential cache in %s: the directory was created with mode %04o rather than '
            '0700.' % (directory, mode))


def _write_atomically(path, document):
    """Replace the cache with document, or leave it exactly as it was.

    The file is written to a temporary file in the same directory and
    then renamed over the real one, so a crash, a full disk or a
    killed process cannot leave a truncated or empty cache where a
    valid one was -- the rename is the only moment anything changes,
    and it either happens or does not. The temporary file is in the
    same directory because a rename across filesystems is not atomic,
    and mkstemp() creates it 0600 and exclusively, so the secret is
    never in a file anyone else could open.
    """
    directory = os.path.dirname(path)
    _ensure_directory(directory)

    temporary = None
    try:
        try:
            fd, temporary = tempfile.mkstemp(dir=directory, prefix=TEMPORARY_PREFIX,
                                             suffix=TEMPORARY_SUFFIX)
            with os.fdopen(fd, 'w') as f:
                json.dump(document, f, indent=4, sort_keys=True)
                f.write('\n')
                f.flush()
                os.fsync(f.fileno())

            # Verified rather than assumed. mkstemp() is documented to
            # create 0600, and the umask can only take bits away, but
            # this is the one file in the package whose mode is a
            # security boundary, and a stat() is free compared with
            # being wrong about it.
            mode = stat.S_IMODE(os.stat(temporary).st_mode)
            if mode & UNSAFE_FILE_BITS:
                raise CredentialCacheException(
                    'Refusing to write %s: the temporary file was created with mode %04o rather than 0600.'
                    % (path, mode))

            os.replace(temporary, path)
            temporary = None
        except OSError as e:
            raise CredentialCacheException(
                'Could not write the credential cache %s: %s. Any previous cache is unchanged.' % (path, e))

        # The rename is durable only once the directory entry is, and a
        # credential which survives a write but not a power cut is one the
        # person would have to mint again.
        #
        # Outside the refusal above, and deliberately. By this point the
        # replacement has happened, so a failure here is a write which
        # succeeded with a weaker durability guarantee than was asked for --
        # not a write which did not happen. Refusing would say "any previous
        # cache is unchanged", which at this one moment is the opposite of
        # the truth, and login turns that into telling the person that none
        # of their credentials were kept about secrets the cluster will
        # never issue again.
        try:
            dir_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            ...
    finally:
        if temporary:
            try:
                os.unlink(temporary)
            except OSError:
                ...


def _load(path):
    """Read and validate the cache, or return None if there is none.

    A missing file is not an error: it means nobody has logged in.
    Everything else which stops this returning a usable document is a
    CredentialCacheException naming the file.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
    except FileNotFoundError:
        return None
    except NotADirectoryError:
        # A parent of the path is a file, so the cache cannot exist.
        return None
    except OSError as e:
        raise CredentialCacheException(
            'Could not read the credential cache %s: %s' % (path, os.strerror(e.errno)))

    try:
        with os.fdopen(fd) as f:
            # Checked through the descriptor already open, rather than
            # with a separate stat() of the path, so that what is
            # checked is certainly what is read.
            mode = stat.S_IMODE(os.fstat(f.fileno()).st_mode)
            if mode & UNSAFE_FILE_BITS:
                raise CredentialCacheUnusable(
                    'Refusing to read the credential cache %s: its mode is %04o, which grants access to group or '
                    'other. The namespace keys in it are long lived secrets. Run "chmod 600 %s" if you still trust '
                    'them, or "sf-client logout" to discard them.' % (path, mode, path))
            # Checked on the way in as well as on the way out: a
            # directory group or other can write to is a directory
            # someone else can substitute a whole credential file in,
            # and the file they leave can perfectly well be 0600.
            _check_directory_mode(os.path.dirname(path) or '.')
            content = f.read()
    except OSError as e:
        raise CredentialCacheException(
            'Could not read the credential cache %s: %s' % (path, os.strerror(e.errno)))

    try:
        document = json.loads(content)
    except ValueError as e:
        raise CredentialCacheUnusable(
            'The credential cache %s is corrupt and was not used: %s. Run "sf-client logout" to discard it and log '
            'in again.' % (path, e))

    return _validate(path, document)


def _validate(path, document):
    """Reject anything which is not a cache this client can read."""
    if not isinstance(document, dict):
        raise CredentialCacheUnusable(
            'The credential cache %s is corrupt and was not used: it is not a JSON object.' % path)

    version = document.get('version')
    if not isinstance(version, int) or isinstance(version, bool):
        raise CredentialCacheUnusable(
            'The credential cache %s is corrupt and was not used: it declares no format version.' % path)
    if version > CACHE_FORMAT_VERSION:
        # Not overwritten, because the newer client which wrote it may
        # hold credentials in fields this one does not know about, and
        # rewriting the file would throw them away.
        raise CredentialCacheException(
            'The credential cache %s is in format version %d, and this client understands version %d. It was '
            'written by a newer client; upgrade this one, or run "sf-client logout" on the newer one to discard the '
            'cache.' % (path, version, CACHE_FORMAT_VERSION))
    if version != CACHE_FORMAT_VERSION:
        raise CredentialCacheUnusable(
            'The credential cache %s is in format version %d, which this client does not understand.'
            % (path, version))

    entries = document.get('credentials')
    if not isinstance(entries, list):
        raise CredentialCacheUnusable(
            'The credential cache %s is corrupt and was not used: it holds no credential list.' % path)
    for credential in entries:
        if not isinstance(credential, dict):
            raise CredentialCacheUnusable(
                'The credential cache %s is corrupt and was not used: one of its credentials is not an object.'
                % path)
        for field in ('apiurl', 'namespace', 'key_name', 'key'):
            if not credential.get(field):
                raise CredentialCacheUnusable(
                    'The credential cache %s is corrupt and was not used: one of its credentials has no %s.'
                    % (path, field))
        expiry = credential.get('expiry')
        if expiry is not None and (isinstance(expiry, bool) or not isinstance(expiry, (int, float))):
            raise CredentialCacheUnusable(
                'The credential cache %s is corrupt and was not used: the credential %s in namespace %s has an '
                'expiry which is not a time.' % (path, credential['key_name'], credential['namespace']))

    current = document.get('current')
    if current is not None and not isinstance(current, dict):
        raise CredentialCacheUnusable(
            'The credential cache %s is corrupt and was not used: its current selection is not an object.' % path)

    return document


def _has_expired(credential, now):
    """Has this credential's own expiry passed?

    An entry with no expiry has not expired: the mint always sends one,
    but a cluster which stops doing so is saying the key does not
    lapse, not that it already has. An expiry which is not a time at
    all never reaches here -- _validate() refuses the whole file.
    """
    expiry = credential.get('expiry')
    if expiry is None:
        return False
    return expiry <= now


def _matches(credential, selection):
    """Is this credential the one the selection names?

    key_name is optional in a selection so that a caller can select a
    namespace without having to name the key inside it, which is what
    a person switching namespaces actually means.
    """
    if credential['apiurl'] != selection.get('apiurl'):
        return False
    if credential['namespace'] != selection.get('namespace'):
        return False
    key_name = selection.get('key_name')
    if key_name and credential['key_name'] != key_name:
        return False
    return True


def _live_view(document, now):
    """The credentials which have not expired, and a selection to match.

    Both reading and purging need this, and they must agree: if they
    disagreed about what had expired, a purge would write back a
    different set from the one a read had just declined to offer. The
    selection goes when the credential it names does, because a
    selection pointing at a credential nothing will return is not a
    selection anything can use.
    """
    live = [c for c in document['credentials'] if not _has_expired(c, now)]
    current = document.get('current')
    if current and not any(_matches(c, current) for c in live):
        current = None
    return live, current


def read_credentials(path=None, now=None):
    """Read the cache, dropping anything which has expired.

    Returns None when there is no cache file, which means nobody has
    logged in. Otherwise returns a dict of the format version, the
    credential list and the current selection.

    An expired credential is dropped here rather than offered and
    allowed to fail: the cluster stops honouring a key the moment its
    expiry passes, and a cache which went on presenting one would turn
    a lapsed login into an unexplained authorisation failure on the
    next command instead of a prompt to log in again.

    Nothing is written -- reading does not repair the file. A dead
    secret therefore stays on disk until something writes the cache,
    and every path which writes it builds the new document from this
    function's output: a login, a change of selection and a logout each
    take the expired entries with them. There is deliberately no
    separate purge, because it would be a second copy of the expiry rule
    with nothing calling it, free to disagree with this one.

    The selection is dropped with the credential it named, for the
    same reason: a selection pointing at a credential this function
    would not return is not a selection anything can use.
    """
    path = credentials_path(path)
    document = _load(path)
    if document is None:
        return None

    if now is None:
        now = time.time()

    live, current = _live_view(document, now)

    return {
        'version': document['version'],
        'credentials': live,
        'current': current,
    }


def write_credentials(credentials, current=None, path=None):
    """Write the whole cache: these credentials and this selection.

    This replaces whatever was there. A caller recording the result of
    a login wants store_credential_set(), which keeps the other
    clusters' credentials; this is the primitive underneath it.
    """
    path = credentials_path(path)

    for credential in credentials:
        for field in ('apiurl', 'namespace', 'key_name', 'key'):
            if not credential.get(field):
                raise CredentialCacheException(
                    'Refusing to cache a credential with no %s: a credential which does not say which cluster, '
                    'namespace and key it is cannot be presented to anything.' % field)

    if current and not any(_matches(c, current) for c in credentials):
        raise CredentialCacheException(
            'Refusing to select a credential which is not in the cache: namespace %s on %s.'
            % (current.get('namespace'), current.get('apiurl')))

    document = {
        'version': CACHE_FORMAT_VERSION,
        'credentials': list(credentials),
        'current': current,
    }
    _write_atomically(path, document)
    return document


def store_credential_set(apiurl, credential_set, namespace=None, path=None, now=None):
    """Record what a login against apiurl minted, and select one.

    credential_set is what the cluster's mint answered with: a list of
    dicts carrying namespace, key_name, key, expiry and scopes. Each
    gets the API URL it was minted against recorded alongside it.

    This cluster's previous entries are replaced rather than added to.
    The mint returns every credential the identity is entitled to and
    returns each secret exactly once, so the new set is the complete
    answer for this cluster -- keeping the old entries would mean
    offering superseded keys. Other clusters' entries are untouched,
    because this login said nothing about them.

    The selection becomes the only member when there is one, or the
    member in the namespace the caller asked for, or nothing at all
    when there are several and the caller did not choose. Nothing is
    guessed: a caller which gets no selection back tells the person
    what there is to pick from.
    """
    path = credentials_path(path)

    try:
        existing = read_credentials(path=path, now=now)
    except CredentialCacheUnusable:
        # A cache which cannot be read cannot be merged with. The
        # person has just logged in and holds credentials which will
        # never be issued again, so losing them to a corrupt or
        # ill-moded file would be the worse outcome of the two; the
        # file is replaced, and what was in it was unusable anyway.
        #
        # Deliberately not every refusal. A cache written by a newer
        # client reaches here too, and is left alone: it may hold
        # credentials in fields this client cannot see, and replacing
        # it would throw them away.
        existing = None

    credentials = []
    if existing:
        credentials = [c for c in existing['credentials'] if c.get('apiurl') != apiurl]

    minted = []
    for credential in credential_set:
        entry = dict(credential)
        entry['apiurl'] = apiurl
        minted.append(entry)
    credentials.extend(minted)

    current = None
    if existing and existing.get('current'):
        # A selection against another cluster survives a login here.
        if existing['current'].get('apiurl') != apiurl:
            current = existing['current']

    if len(minted) == 1:
        chosen = minted[0]
    else:
        candidates = [c for c in minted if namespace and c['namespace'] == namespace]
        chosen = candidates[0] if len(candidates) == 1 else None

    if chosen:
        current = {
            'apiurl': apiurl,
            'namespace': chosen['namespace'],
            'key_name': chosen['key_name'],
        }

    write_credentials(credentials, current=current, path=path)
    return {'credentials': minted, 'current': current}


def credentials_for_apiurl(apiurl, path=None, now=None):
    """Every live cached credential minted against this cluster.

    The definite answer to "what did a login against this API URL
    leave me", for a caller which has an API URL and needs to know
    whether the cache can serve it. An empty list means the cache has
    nothing for this cluster, whatever it holds for others.
    """
    cache = read_credentials(path=path, now=now)
    if not cache:
        return []
    return [c for c in cache['credentials'] if c.get('apiurl') == apiurl]


def current_credential(apiurl=None, path=None, now=None):
    """The selected credential, in full, or None.

    With apiurl given, the selection is returned only if it was minted
    against that cluster. A namespace key is cluster scoped, so the
    selection made on one cluster is not an answer for another, and
    the caller asking is one which already knows which cluster it is
    talking to.
    """
    cache = read_credentials(path=path, now=now)
    if not cache or not cache['current']:
        return None

    if apiurl and cache['current'].get('apiurl') != apiurl:
        return None

    for credential in cache['credentials']:
        if _matches(credential, cache['current']):
            return credential
    return None


def select_current(apiurl, namespace, key_name=None, path=None, now=None):
    """Make the credential for this cluster and namespace the selected one.

    Returns the credential selected. Refuses rather than selecting
    anything if the cache holds no live credential matching, or holds
    more than one and key_name did not say which -- an ambiguous
    selection resolved by guessing is a command run against the wrong
    namespace.
    """
    path = credentials_path(path)
    cache = read_credentials(path=path, now=now)
    if not cache:
        raise CredentialCacheException(
            'There are no cached credentials to select from in %s.' % path)

    selection = {'apiurl': apiurl, 'namespace': namespace, 'key_name': key_name}
    matches = [c for c in cache['credentials'] if _matches(c, selection)]
    if not matches:
        raise CredentialCacheException(
            'There is no cached credential for namespace %s on %s.' % (namespace, apiurl))
    if len(matches) > 1:
        raise CredentialCacheException(
            'There are %d cached credentials for namespace %s on %s. Name one of %s.'
            % (len(matches), namespace, apiurl, ', '.join(sorted(c['key_name'] for c in matches))))

    chosen = matches[0]
    current = {
        'apiurl': chosen['apiurl'],
        'namespace': chosen['namespace'],
        'key_name': chosen['key_name'],
    }
    write_credentials(cache['credentials'], current=current, path=path)
    return chosen


def _remove_stale_temporaries(directory):
    """Remove the write temporaries a killed process left behind.

    _write_atomically() unlinks its temporary in a finally, but a
    process killed between mkstemp() and os.replace() never runs one --
    and that file holds the whole credential set, keys and all. A
    logout which removed only credentials.json would leave a secret the
    cluster still honours sitting on disk after the person asked for it
    to be discarded, which is the one thing that verb exists to
    prevent.

    Best effort and silent. These files are not what the person asked
    about, and a logout must not fail over one it could not reach when
    it has already discarded the cache itself. They are also not
    counted: the count answers "how many credentials did I just give
    up", and a temporary holds a copy of credentials already counted
    from the file it was going to replace.
    """
    try:
        names = os.listdir(directory)
    except OSError:
        return

    for name in names:
        if name.startswith(TEMPORARY_PREFIX) and name.endswith(TEMPORARY_SUFFIX):
            try:
                os.unlink(os.path.join(directory, name))
            except OSError:
                ...


def delete_credentials(path=None):
    """Delete the cache, and say how many credentials went with it.

    The whole file, not just the selection: a credential which
    survives a logout the person asked for is a secret still on their
    disk and still honoured by the cluster. That includes any write
    temporary a killed process left in the directory, which holds the
    same secrets under a different name.

    Returns the number of credentials discarded, or None when the file
    existed but could not be counted. Counting never stops the delete,
    and neither does an unsafe mode or a corrupt file -- a cache this
    module would refuse to read is the one most worth removing.
    """
    path = credentials_path(path)

    # Before anything which can return or raise: a directory holding
    # only a stale temporary, with no cache file at all, is the state a
    # process killed during its very first write leaves behind, and it
    # still has a key in it.
    _remove_stale_temporaries(os.path.dirname(path))

    discarded = None
    try:
        document = _load(path)
        if document is None:
            return 0
        discarded = len(document['credentials'])
    except CredentialCacheException:
        ...

    try:
        os.unlink(path)
    except FileNotFoundError:
        return 0
    except OSError as e:
        raise CredentialCacheException(
            'Could not remove the credential cache %s: %s' % (path, os.strerror(e.errno)))

    if discarded is None:
        return None
    return discarded
