"""Human login: the device authorization flow, and logging back out.

``sf-client login`` turns an identity at an external provider into Shaken
Fist credentials, and ``sf-client logout`` discards them again. The flow
implemented here is the device authorization grant of RFC 8628: the
client asks the provider for a short code, the person types that code
into a browser wherever they have one, and the client polls until the
provider says the login happened. It needs no browser, no loopback
listener and no redirect registration on this host, which is why it is
the flow that works over ssh and inside a container.

Three properties of this module are deliberate and are easy to undo by
accident:

* No secret is ever printed or logged, in any output mode or at any log
  level. That covers the identity token the provider issues, the device
  code the flow polls with, and the namespace keys the cluster mints.
  The redaction filter ``sf-client`` installs over its own logging
  matches a JWT, which a namespace key is not, so nothing here may rely
  on it.
* The instructions, warnings and refusals go to the error stream, and
  standard output carries only the command's own document. A login talks
  to a human in the middle of a command whose output may be being parsed
  by whatever asked for ``--json``.
* Neither verb needs an existing credential. ``logout`` builds no client
  at all, because logging out is the removal of a local file and the
  states it exists to clear -- a cache which is corrupt, or whose mode
  exposed a key -- are states in which constructing a client raises.
  ``login`` builds its own from an API URL alone.
"""

import datetime
import json
import sys
import time
from urllib.parse import urlparse

import click
import requests
from prettytable import PrettyTable

from shakenfist_client import apiclient
from shakenfist_client import credentials


# RFC 8628 section 3.4: what a client posts to the token endpoint while
# it waits for the person to finish.
DEVICE_CODE_GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:device_code'

# Section 3.5 again: a client which was not given an interval polls every
# five seconds, and a provider which answers slow_down wants five seconds
# more between polls than it was getting.
DEFAULT_POLL_INTERVAL = 5
SLOW_DOWN_INCREMENT = 5

# An interval below this is treated as absent rather than honoured. The
# field is the provider's way of asking not to be polled hard, so a zero
# -- or a sub-second value, or a string -- is a broken answer and not an
# invitation to spin on somebody else's token endpoint.
MINIMUM_POLL_INTERVAL = 1

# How long the flow runs for when the provider does not say. RFC 8628
# requires expires_in, so this is a broken provider; but the alternative
# to picking a bound is polling forever, and a person whose login is cut
# short can run the command again.
DEFAULT_EXPIRES_IN = 300

# Connect and read timeout for every request to the identity provider.
# Without one a provider which accepts the socket and then says nothing
# would park the terminal indefinitely, which is the same failure the
# poll loop's deadline exists to prevent.
ISSUER_REQUEST_TIMEOUT = 30


class LoginFailed(Exception):
    """A login cannot be completed, and this is what to tell the person.

    Carried as an exception rather than printed where it is discovered,
    so that one place decides how a refusal reaches the terminal and so
    that the flow can be tested without capturing output. The message
    never carries a token: a refusal is the most likely thing in a login
    to be pasted into a bug report.
    """

    ...


def _tell(message):
    """Say something to the person, on the error stream.

    Standard output belongs to the command's document -- a table, a
    comma separated listing, or JSON -- and ``--json`` exists so that
    something else can parse it. A login still has to talk to a human
    while it runs, so everything it says goes to stderr.
    """
    click.echo(message, err=True)


def _json_body(response):
    """The provider's JSON body, or an empty one.

    Every error path here has to work on a body which is not JSON at
    all: an HTML error page from a proxy in front of the provider is the
    common case, and a flow which raised a JSON decoding error from
    inside the poll loop would report the proxy's content type instead
    of what went wrong.
    """
    try:
        document = response.json()
    except ValueError:
        return {}
    if not isinstance(document, dict):
        return {}
    return document


def _oauth_error(response, document):
    """Render an OAuth error for a person, and nothing else from the body.

    The error code and its description are the only two fields worth
    showing and the only two which are not a credential: a token
    response carries tokens, so a refusal which echoed the body back
    would be a way for one to reach the terminal.
    """
    error = document.get('error')
    description = document.get('error_description')
    if error and description:
        return 'HTTP %s, %s (%s)' % (response.status_code, error, description)
    if error:
        return 'HTTP %s, %s' % (response.status_code, error)
    return 'HTTP %s' % response.status_code


def _warn_about_plaintext(issuer, tell):
    """Say so when a provider endpoint is not https.

    An identity token crosses this connection, so a plaintext endpoint
    hands it to anybody on the path between here and the provider. This
    warns rather than refuses because the operator chose these endpoints
    and a test provider on a loopback address is a legitimate thing to
    log in to -- but it must not be silent.
    """
    for field in ('device_authorization_endpoint', 'token_endpoint'):
        endpoint = issuer.get(field)
        if endpoint and urlparse(endpoint).scheme != 'https':
            tell('Warning: the %s at %s is not https, so the identity token this '
                 'login sends will cross the network in plain text.'
                 % (field.replace('_', ' '), endpoint))


def _request_device_code(issuer):
    """Ask the provider to start a device authorization.

    The body is form encoded, which is what RFC 8628 specifies. The
    scopes arrive from the cluster as a single space separated string,
    which is exactly the format the scope parameter takes, so they are
    passed through rather than parsed and rejoined.

    The device code in the answer is itself a credential -- it is what
    the poll loop presents -- so nothing here renders the body, not even
    on a refusal.
    """
    endpoint = issuer['device_authorization_endpoint']
    body = {'client_id': issuer['client_id']}
    if issuer.get('oidc_scopes'):
        body['scope'] = issuer['oidc_scopes']

    try:
        r = requests.post(endpoint, data=body, timeout=ISSUER_REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as e:
        raise LoginFailed('Could not reach the identity provider at %s: %s'
                          % (endpoint, e))

    document = _json_body(r)
    if r.status_code != 200:
        raise LoginFailed(
            'The identity provider would not start a login: %s. The cluster may be '
            'registered at the provider under a client identifier the provider does '
            'not know, or for scopes it will not grant.'
            % _oauth_error(r, document))

    for field in ('device_code', 'user_code', 'verification_uri'):
        if not document.get(field):
            raise LoginFailed(
                'The identity provider started a login but did not say what the %s is, '
                'so there is no way to continue. This is a fault at the provider.'
                % field)
    return document


def _request_token(issuer, device_code):
    """Ask the provider once whether the person has finished.

    Returns the response for the caller to read, because every answer
    other than a token is a rule about what to do next rather than a
    failure.
    """
    endpoint = issuer['token_endpoint']
    body = {
        'grant_type': DEVICE_CODE_GRANT_TYPE,
        'device_code': device_code,
        'client_id': issuer['client_id'],
    }

    try:
        return requests.post(endpoint, data=body,
                             timeout=ISSUER_REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as e:
        raise LoginFailed('Could not reach the identity provider at %s: %s'
                          % (endpoint, e))


def _poll_interval(document):
    """How long to wait between polls, as the provider asked.

    RFC 8628 makes interval optional and says a client which was not
    given one waits five seconds. A provider which sends something
    unusable gets the same default rather than a busy loop against its
    own token endpoint.
    """
    try:
        interval = float(document.get('interval'))
    except (TypeError, ValueError):
        return DEFAULT_POLL_INTERVAL
    if interval < MINIMUM_POLL_INTERVAL:
        return DEFAULT_POLL_INTERVAL
    return interval


def _expires_in(document):
    """How long the whole flow has, in seconds.

    The provider decides this, because it is the provider which will
    stop honouring the code. A missing or unusable value falls back to a
    bound of our own: being wrong about when to stop is recoverable by
    running the command again, and having no bound at all is a terminal
    which never comes back.
    """
    try:
        expires_in = float(document.get('expires_in'))
    except (TypeError, ValueError):
        return DEFAULT_EXPIRES_IN
    if expires_in <= 0:
        return DEFAULT_EXPIRES_IN
    return expires_in


def _show_the_code(started, tell):
    """Tell the person where to go and what to type.

    verification_uri_complete has the code embedded in it already, so a
    provider which sends one has saved the person transcribing it. The
    code is still printed beside the link, because the provider is
    expected to display it for confirmation and somebody who followed a
    link should be able to check that the two match.
    """
    tell('')
    if started.get('verification_uri_complete'):
        tell('To log in, open %s' % started['verification_uri_complete'])
        tell('and confirm the code %s.' % started['user_code'])
    else:
        tell('To log in, open %s' % started['verification_uri'])
        tell('and enter the code %s.' % started['user_code'])
    tell('')
    tell('Waiting for the login to be completed...')


def device_authorization_login(issuer, sleep=None, clock=None, tell=None):
    """Run a device authorization flow, and return an identity token.

    The identity token is the only thing this returns, deliberately. The
    provider's access and refresh tokens are credentials this cluster
    cannot revoke, so nothing here keeps them, and the cluster's mint
    validates an identity token's audience against the client identifier
    it registered.

    The loop obeys four rules from RFC 8628 and giving up on any of them
    makes this client the badly behaved OAuth client in somebody else's
    logs: wait the interval the provider asked for, add to that interval
    when it answers slow_down, keep waiting while it answers
    authorization_pending, and stop at expires_in rather than polling
    forever.

    sleep and clock are injected so a test can run the whole loop
    without waiting. The clock is monotonic because this measures a
    deadline rather than telling the time: a login must not be cut short
    or extended by the system clock being corrected underneath it.
    """
    sleep = sleep or time.sleep
    clock = clock or time.monotonic
    tell = tell or _tell

    _warn_about_plaintext(issuer, tell)
    started = _request_device_code(issuer)
    _show_the_code(started, tell)

    interval = _poll_interval(started)
    deadline = clock() + _expires_in(started)

    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            raise LoginFailed(
                'The login was not completed in time and the code %s has expired. '
                'Run the login again to get a new one.' % started['user_code'])

        # Waiting before the first poll as well as between later ones.
        # Nobody can have typed the code yet, so an immediate poll is a
        # request which is certain to be answered authorization_pending.
        sleep(min(interval, remaining))

        r = _request_token(issuer, started['device_code'])
        document = _json_body(r)

        if r.status_code == 200:
            token = document.get('id_token')
            if not token:
                raise LoginFailed(
                    'The identity provider completed the login but returned no identity '
                    'token, so there is nothing to present to the cluster. The provider '
                    'may not be configured to issue one for the scopes this cluster asks '
                    'for.')
            return token

        error = document.get('error')
        if error == 'authorization_pending':
            continue
        if error == 'slow_down':
            interval += SLOW_DOWN_INCREMENT
            continue
        if error == 'expired_token':
            raise LoginFailed(
                'The identity provider says the code %s has expired. Run the login '
                'again to get a new one.' % started['user_code'])
        if error == 'access_denied':
            raise LoginFailed('The login was refused at the identity provider.')
        raise LoginFailed('The identity provider refused the login: %s'
                          % _oauth_error(r, document))


def _issuers_for_this_flow(issuers):
    """The issuers whose metadata says a device login can begin there.

    The cluster sends an endpoint only when it has one, deliberately, so
    the absence of a device authorization endpoint is the answer to "can
    this provider do the device flow here". A client which posted to it
    anyway would be sending a client identifier to a URL it invented.
    """
    return [issuer for issuer in issuers
            if issuer.get('device_authorization_endpoint')]


def _prompt_for_issuer(issuers, tell):
    """Ask which provider to log in through.

    Numbered rather than named, because a provider name is an operator's
    label and may be long. The prompt is on the error stream with the
    rest of the conversation.
    """
    tell('This cluster offers more than one identity provider:')
    for number, issuer in enumerate(issuers, start=1):
        tell('  %d) %s (%s)' % (number, issuer['name'], issuer['issuer_url']))
    choice = click.prompt('Which one would you like to log in through',
                          type=click.IntRange(1, len(issuers)), err=True)
    return issuers[choice - 1]


def _choose_issuer(issuers, tell=None, prompt=None):
    """Pick the provider to log in through.

    An empty list is a complete answer rather than a fault: the cluster
    is saying it offers no human login, and the only thing to do with
    that is say so.
    """
    tell = tell or _tell
    prompt = prompt or _prompt_for_issuer

    if not issuers:
        raise LoginFailed('This cluster offers no human login.')

    usable = sorted(_issuers_for_this_flow(issuers), key=lambda i: i['name'])
    if not usable:
        raise LoginFailed(
            'This cluster offers human login through %s, but none of those providers '
            'advertises a device authorization endpoint, so there is no login this '
            'client can run against them.'
            % ', '.join(sorted(issuer['name'] for issuer in issuers)))

    if len(usable) == 1:
        return usable[0]
    return prompt(usable, tell)


def _client_for_login(apiurl, verbose=False):
    """A client for the two public login routes, and nothing else.

    Somebody logging in for the first time has no credential, no
    configuration file and no cache, so this must not require any of
    them. Where the cluster was named -- on the command line or in the
    environment -- the credential resolution is skipped altogether: it
    is the only part of constructing a client which can fail for
    somebody who has not logged in yet, and a login has no use for its
    result, because both routes it calls are public.

    Where the cluster was not named, the resolution runs, purely to find
    out which cluster this host is configured for. Both of its failures
    are translated rather than raised: each has something the person can
    do about it, and neither is a reason the login itself cannot
    proceed.

    --verbose is passed on explicitly. The group hands the client it
    builds its own logger, which --verbose has already set to DEBUG, and
    a client built here has no access to that logger; apiclient pins its
    own to INFO at import, so without this a verbose login would print
    nothing at all about the requests it made -- on the one verb where
    "why was I refused" is the usual question.
    """
    if apiurl:
        return apiclient.Client(base_url=apiurl, verbose=verbose,
                                suppress_configuration_lookup=True)

    try:
        return apiclient.Client(verbose=verbose)
    except apiclient.UnconfiguredException:
        raise LoginFailed(
            'There is nothing here that says which cluster to log in to. Name one '
            'with --apiurl, or set SHAKENFIST_API_URL in the environment.')
    except credentials.CredentialCacheException as e:
        # The cache was being read only to find out which cluster this host
        # talks to, which is a question --apiurl answers. So this adds that
        # escape and leaves the rest of the advice to the message, which
        # knows whether the cache can be repaired or only discarded.
        raise LoginFailed(
            '%s A login does not need the cache: name the cluster with --apiurl to '
            'log in without reading it.' % e)


def _login_issuers(client):
    """The issuer list, with the refusals that route can answer."""
    try:
        return client.get_login_issuers()
    except apiclient.IncapableException as e:
        raise LoginFailed('%s, so there is no human login to run here.' % e)
    except apiclient.ServiceUnavailableException:
        raise LoginFailed(
            'The cluster could not say which identity providers it offers just now. '
            'That is temporary rather than a refusal: try again in a moment.')
    except apiclient.APIException as e:
        if e.status_code == 429:
            raise LoginFailed(
                'The cluster is rate limiting requests for its login configuration '
                'from this address. Wait a minute and try again.')
        raise


def _check_the_cluster_can_mint(client):
    """Refuse before the flow starts if the cluster cannot mint.

    The two halves of human login are advertised separately, and a
    cluster can offer the issuer list without the exchange. Asking now,
    rather than letting the exchange's own gate raise, is the difference
    between a message and sending somebody to their identity provider,
    waiting while they authenticate, and refusing them afterwards with an
    identity token in hand that nothing can spend.
    """
    if not client.check_capability('human-login'):
        raise LoginFailed(
            'This cluster can list identity providers but cannot exchange an identity '
            'token for credentials, so there is no point starting a login against it.')


def _mint_credentials(client, token):
    """Exchange the identity token for the credentials it entitles.

    Each refusal the cluster can answer with gets a message somebody can
    act on, by exception class where this client has one and by status
    code where it does not. None of them renders the token or the
    server's own body: the first is a credential, and the second is
    deliberately thin on a route anybody can reach.
    """
    try:
        return client.login(token)
    except apiclient.IncapableException as e:
        raise LoginFailed('%s.' % e)
    except apiclient.UnauthenticatedException:
        raise LoginFailed(
            'The cluster refused this identity. The login itself worked, because the '
            'identity provider issued a token, so what is missing is on the cluster: '
            'either no mapping rule grants this identity a namespace, or the token did '
            'not validate against the issuer it names. The cluster log says which, and '
            'an operator can read it.')
    except apiclient.InsufficientResourcesException:
        # The cluster caps how many credentials one login may mint, and
        # refuses the whole set rather than truncating it. Nothing about
        # this host, this person or this moment will change the answer,
        # so it must not read as the capacity problem that status code
        # usually means -- and the client must not be built to retry it.
        raise LoginFailed(
            'This identity satisfies more of the cluster mapping rules than one login '
            'is allowed to mint credentials for, so the cluster minted none of them. '
            'Nothing is wrong with the identity or with this host, and trying again '
            'will not help: an operator has to narrow the rules which match this '
            'identity, or raise the cluster limit on how many one login may mint.')
    except apiclient.ServiceUnavailableException:
        raise LoginFailed(
            'The cluster could not check this login just now. That is temporary rather '
            'than a refusal: try again in a moment.')
    except apiclient.APIException as e:
        if e.status_code == 429:
            raise LoginFailed(
                'The cluster is rate limiting logins from this address. Wait a minute '
                'and try again.')
        if e.status_code == 413:
            raise LoginFailed(
                'The cluster refused the identity token because it is larger than the '
                'cluster will accept. A token carrying every group a large directory '
                'knows about can do that; an operator can either raise the cluster '
                'limit or ask the provider for fewer claims.')
        raise


def _store(apiurl, minted, namespace):
    """Write what was minted to the cache, and say what was selected.

    The secrets are in hand and will never be issued again, so a cache
    which cannot be written loses them. That is recoverable -- another
    login mints another set -- but only if the person is told, which is
    why this refuses with an instruction rather than letting the cache
    exception reach the terminal on its own.
    """
    try:
        return credentials.store_credential_set(apiurl, minted,
                                                namespace=namespace)
    except credentials.CredentialCacheException as e:
        raise LoginFailed(
            'The login worked but the credentials could not be saved, so none of them '
            'were kept: %s Fix that and run the login again -- another login mints '
            'another set.' % e)


def _expiry(expiry):
    """Render an expiry timestamp for a person, not for a script.

    The comma separated and JSON outputs carry the raw unix timestamp
    the cluster sent, as the namespace key listing does and for the same
    reason: it cannot contain a separator, and something reading it can
    compare it with the current time without parsing a date.
    """
    if not expiry:
        return ''
    return str(datetime.datetime.fromtimestamp(expiry))


def _minted_rows(stored):
    """What a login minted, with every secret left out.

    The key is the one field the exchange returns which must never be
    rendered: it is a long lived credential, and the filter sf-client
    installs over its own logging redacts a JWT, which a namespace key
    is not. So each row is built field by field from a fixed list rather
    than by copying the credential and deleting what should not be there
    -- a field the cluster adds later then has to be named here before
    it can be printed, which is the right way round.
    """
    current = stored.get('current') or {}
    rows = []
    for credential in stored['credentials']:
        rows.append({
            'apiurl': credential['apiurl'],
            'namespace': credential['namespace'],
            'key_name': credential['key_name'],
            'expiry': credential.get('expiry'),
            'scopes': credential.get('scopes') or [],
            'current': (credential['apiurl'] == current.get('apiurl') and
                        credential['namespace'] == current.get('namespace') and
                        credential['key_name'] == current.get('key_name')),
        })
    return rows


def _show_what_was_minted(ctx, stored):
    rows = _minted_rows(stored)
    output = ctx.obj.get('OUTPUT')

    if output == 'json':
        print(json.dumps(rows, indent=4, sort_keys=True))

    elif output == 'simple':
        print('namespace,key_name,expiry,scopes,current')
        for row in rows:
            print(','.join([
                row['namespace'], row['key_name'],
                '' if not row['expiry'] else repr(row['expiry']),
                ' '.join(row['scopes']),
                'yes' if row['current'] else 'no']))

    else:
        t = PrettyTable(['namespace', 'key name', 'expiry', 'scopes', 'current'])
        t.align = 'l'
        for row in rows:
            t.add_row([row['namespace'], row['key_name'], _expiry(row['expiry']),
                       ' '.join(row['scopes']),
                       'yes' if row['current'] else 'no'])
        print(t)

    # An empty set is not something the cluster answers -- it refuses a
    # login which matches no mapping rule rather than minting nothing --
    # but there is nothing to say about one either way, and an advice
    # line naming the namespaces would have no namespace to name.
    if rows and not any(row['current'] for row in rows):
        # Asked of these rows rather than of the selection, because a
        # selection which survived this login names a different cluster.
        # That is not the same as nothing being selected, and it is worse:
        # the next command would resolve it and talk to that other cluster
        # as that other namespace, so this login would look as though it
        # had done nothing at all.
        #
        # Nothing is guessed in either case. A command addresses an object
        # rather than a namespace, so there is no way to work out which of
        # these credentials it wanted, and picking one would run it
        # somewhere the person did not ask for.
        _tell('')
        _tell('This login minted credentials in more than one namespace and none of '
              'them is selected, so none of them will be presented by default.')

        elsewhere = stored.get('current')
        if elsewhere:
            _tell('A command which names no cluster would use the credential selected '
                  'earlier for namespace %s on %s instead.'
                  % (elsewhere['namespace'], elsewhere['apiurl']))

        _tell('Choose one of these with "sf-client --namespace=%s login".'
              % sorted(row['namespace'] for row in rows)[0])


@click.command(name='login',
               help='Log in through an identity provider')
@click.pass_context
def login(ctx):
    """Log in to a cluster through one of the identity providers it trusts.

    Builds its own client, from an API URL alone, because the group
    cannot build one for somebody who has no credential yet -- which is
    the normal state for the first command a new user runs.
    """
    try:
        client = _client_for_login(ctx.obj.get('APIURL'),
                                   verbose=bool(ctx.obj.get('VERBOSE')))
        issuers = _login_issuers(client)
        _check_the_cluster_can_mint(client)
        issuer = _choose_issuer(issuers)

        _tell('Logging in to %s through %s.' % (client.base_url, issuer['name']))
        token = device_authorization_login(issuer)

        # The token is spent here and kept nowhere. What is cached is
        # what the cluster minted from it, which is a credential this
        # cluster can revoke.
        minted = _mint_credentials(client, token)
        stored = _store(client.base_url, minted, ctx.obj.get('NAMESPACE'))

    except LoginFailed as e:
        _tell(str(e))
        sys.exit(1)

    _show_what_was_minted(ctx, stored)


@click.command(name='logout',
               help='Discard the credentials a login cached on this host')
@click.pass_context
def logout(ctx):
    """Remove the credential cache. No cluster is involved.

    Builds no client and makes no request, deliberately. Logging out is
    the removal of a local file, and the states it exists to clear -- a
    cache which is corrupt, or from a newer client, or whose mode
    exposed a key -- are exactly the states in which resolving a
    credential raises. A logout which needed a client would be
    unavailable precisely when it is the command somebody needs.
    """
    path = credentials.credentials_path()
    discarded = credentials.delete_credentials()
    output = ctx.obj.get('OUTPUT')

    if output == 'json':
        print(json.dumps({'path': path, 'discarded': discarded},
                         indent=4, sort_keys=True))

    elif output == 'simple':
        print('discarded')
        print('unknown' if discarded is None else discarded)

    elif discarded is None:
        # The file was there and was removed, but could not be read to
        # count what was in it. Saying so is better than reporting a
        # count of zero, which would read as "there was nothing to
        # discard" when the truth is "there was something unreadable".
        print('Discarded the credential cache %s. It could not be read, so there is '
              'no count of what was in it.' % path)

    elif not discarded:
        print('There were no cached credentials to discard.')

    else:
        print('Discarded %d cached credential%s from %s.'
              % (discarded, '' if discarded == 1 else 's', path))


# The verbs the CLI group must not build an API client for; each
# command's docstring says why it cannot have one. Derived from the
# commands themselves so that renaming one cannot leave this behind.
COMMANDS_WITHOUT_A_CLIENT = frozenset([login.name, logout.name])
