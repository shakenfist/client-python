"""Human login: two interactive flows, and logging back out.

``sf-client login`` turns an identity at an external provider into Shaken
Fist credentials, and ``sf-client logout`` discards them again. Two
flows are implemented here and ``login`` runs whichever one the
cluster's providers and this host can both manage, with ``--flow``
overriding that choice:

* The device authorization grant of RFC 8628: the client asks the
  provider for a short code, the person types that code into a browser
  wherever they have one, and the client polls until the provider says
  the login happened. It needs no browser, no loopback listener and no
  redirect registration on this host, which is why it is the flow that
  works over ssh and inside a container, and why it is preferred
  wherever a provider offers it.
* The authorization code grant with PKCE, RFC 6749 section 4.1 and RFC
  7636: the client opens a browser at the provider, waits on a loopback
  port for the redirect which comes back, and exchanges the
  authorization code that redirect carries for an identity token. It is
  the fallback for a provider which advertises no device authorization
  endpoint, and it is better where a browser exists because nobody has
  to transcribe a code.

Three properties of this module are deliberate and are easy to undo by
accident:

* No secret is ever printed or logged, in any output mode or at any log
  level. That covers the identity token the provider issues, the device
  code the device flow polls with, the PKCE verifier and the
  authorization code the browser flow exchanges, and the namespace keys
  the cluster mints. The redaction filter ``sf-client`` installs over
  its own logging matches a JWT, which none of those are, so nothing
  here may rely on it. The single deliberate exception is the
  authorization URL, printed only where a browser could not be opened:
  that URL is what the person's browser has to receive for the flow to
  happen at all, it carries neither the verifier nor the code, and the
  ``state`` in it is protected by the verifier rather than by secrecy.
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

import base64
import datetime
import hashlib
import json
import secrets
import sys
import time
import webbrowser
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlparse
from urllib.parse import urlunparse

import click
import requests
from prettytable import PrettyTable

from shakenfist_client import apiclient
from shakenfist_client import credentials


# The two flows a login can run, spelled as --flow takes them.
FLOW_DEVICE = 'device'
FLOW_CODE = 'code'

# What each flow needs from the issuer metadata the cluster serves. The
# cluster sends an endpoint only when it has one, deliberately, so a
# missing endpoint is the answer to "can this flow begin at this
# provider" rather than something to work around by guessing a URL.
FLOW_ENDPOINTS = {
    FLOW_DEVICE: ('device_authorization_endpoint', 'token_endpoint'),
    FLOW_CODE: ('authorization_endpoint', 'token_endpoint'),
}

# How to describe a flow, and what it needs, to somebody who has to go
# and ask an operator to configure the missing half.
FLOW_DESCRIPTIONS = {
    FLOW_DEVICE: 'a device authorization login',
    FLOW_CODE: 'an authorization code login',
}
FLOW_REQUIREMENTS = {
    FLOW_DEVICE: 'a device authorization endpoint and a token endpoint',
    FLOW_CODE: 'an authorization endpoint and a token endpoint',
}

# Said in two places, because the flow choice and the provider choice
# both reach it, and an empty issuer list has exactly one meaning.
NO_HUMAN_LOGIN = 'This cluster offers no human login.'

# RFC 8628 section 3.4: what a client posts to the token endpoint while
# it waits for the person to finish.
DEVICE_CODE_GRANT_TYPE = 'urn:ietf:params:oauth:grant-type:device_code'

# RFC 6749 section 4.1: what the browser flow asks the authorization
# endpoint for, and the grant it presents to the token endpoint
# afterwards.
AUTHORIZATION_RESPONSE_TYPE = 'code'
AUTHORIZATION_CODE_GRANT_TYPE = 'authorization_code'

# RFC 7636 section 4.2. S256 rather than the plain method the same
# section allows: under plain the challenge *is* the verifier, so the
# secret which proves this client started the flow would cross the
# authorization endpoint's connection and sit in a browser's history.
PKCE_CHALLENGE_METHOD = 'S256'

# Bytes of randomness behind the verifier and the state. The verifier
# becomes 86 characters of base64url, inside the 43 to 128 RFC 7636
# allows, and both are far past guessing.
PKCE_VERIFIER_BYTES = 64
STATE_BYTES = 32

# The loopback interface, spelled as an address rather than as a name.
# RFC 8252 section 7.3 recommends the literal: "localhost" is whatever
# this host's resolver says it is, which may be ::1, where this listener
# is not bound. It must never be 0.0.0.0, which would offer a redirect
# -- and the authorization code in it -- to anything that can reach this
# machine.
LOOPBACK_ADDRESS = '127.0.0.1'

# How long the listener waits for the provider to send the person back.
# Long enough to find a password manager and answer a second factor, and
# bounded for the same reason the device flow stops at expires_in: the
# alternative is a terminal which never comes back.
REDIRECT_TIMEOUT = 300

# How long one connection has to send its request line before the
# listener gives up on it and goes back to waiting. The timeout above
# bounds waiting for a connection and nothing else: once something has
# connected, reading its request is a separate wait, and without this one
# anything which opens the port and then says nothing holds a login open
# for as long as it likes. A browser making a speculative connection does
# exactly that by accident, and any other process on this host can do it
# on purpose.
REDIRECT_READ_TIMEOUT = 10

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


def _warn_about_plaintext(issuer, tell, flow=FLOW_DEVICE):
    """Say so when an endpoint this flow will use is not https.

    Every one of them carries a credential: an identity token comes back
    from the token endpoint, the device endpoint hands out the code the
    poll loop presents, and the browser flow's authorization endpoint
    takes the person's password at the provider and returns an
    authorization code through the redirect. A plaintext endpoint offers
    whichever of those it handles to anybody on the path.

    Only the endpoints the chosen flow will actually use are checked.
    The cluster may have an endpoint configured for a flow this login is
    not running, and warning about a connection nothing will open would
    teach a person to ignore the warning.

    This warns rather than refuses because the operator chose these
    endpoints and a test provider on a loopback address is a legitimate
    thing to log in to -- but it must not be silent.
    """
    for field in FLOW_ENDPOINTS[flow]:
        endpoint = issuer.get(field)
        if endpoint and urlparse(endpoint).scheme != 'https':
            tell('Warning: the %s at %s is not https, so the credentials this login '
                 'exchanges with it will cross the network in plain text.'
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

    _warn_about_plaintext(issuer, tell, flow=FLOW_DEVICE)
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


def _base64url(raw):
    """base64url with the padding stripped, which is what RFC 7636 uses."""
    return base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')


def _pkce_pair():
    """A PKCE verifier and the challenge derived from it.

    The verifier is the secret which proves that whatever asks the token
    endpoint for a token is whatever asked the authorization endpoint
    for the code. It never leaves this process until it goes to the
    token endpoint: what the browser carries is the challenge, which is
    its SHA-256 digest, so an authorization request read off the wire or
    out of a browser history cannot be turned into a token request.

    That is also what makes the state parameter safe to print. Somebody
    who learns the state can send this listener a redirect carrying an
    authorization code of their own, but the exchange of that code
    requires a verifier generated here and never shown to them.
    """
    verifier = _base64url(secrets.token_bytes(PKCE_VERIFIER_BYTES))
    challenge = _base64url(hashlib.sha256(verifier.encode('ascii')).digest())
    return verifier, challenge


def _new_state():
    """A fresh state parameter for one authorization request.

    The state ties the redirect this client receives to the
    authorization request it sent. A redirect carrying anything else is
    somebody else's: a stale browser tab, a bookmarked redirect, or an
    attempt to hand this client an authorization code belonging to
    another identity.
    """
    return secrets.token_urlsafe(STATE_BYTES)


def _states_match(expected, received):
    """Whether a redirect carries the state this login generated.

    secrets.compare_digest rather than ``==``, and not because a timing
    attack is available as this is written: the listener serves exactly
    one request, so there is no second measurement for anybody to take.
    It is the comparison which stays correct if that ever changes, and
    it costs nothing to make now.

    A redirect with no state, or with a state carrying anything outside
    ASCII, is refused here rather than being allowed to raise out of the
    request handler -- where the traceback would carry the request line,
    and the request line is the authorization code.
    """
    if not expected or not received:
        return False
    try:
        return secrets.compare_digest(expected, received)
    except TypeError:
        return False


# What the browser is shown when the redirect arrives, served from here
# rather than fetched: a login must not depend on reaching anything the
# provider is not, and this page is the last thing the person sees, so a
# blank tab would read as a flow which hung. Each kind of redirect gets
# its own page and its own status, and none of them renders anything out
# of the request -- the authorization code and the state are in there.
REDIRECT_PAGE = """<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>Shaken Fist login</title></head>
<body style="font-family: sans-serif; margin: 4em auto; max-width: 34em;">
<h1>%s</h1>
<p>%s</p>
</body>
</html>
"""

REDIRECT_PAGES = {
    'code': (200, 'Login complete',
             'Shaken Fist has what it needs. You can close this tab and go back to '
             'the terminal.'),
    'state': (400, 'Login refused',
              'This redirect does not match the login this client started, so nothing '
              'in it was used. Start the login again from the terminal.'),
    'error': (400, 'Login refused',
              'The identity provider refused this login. The terminal says what it '
              'answered.'),
    'other': (404, 'Nothing to do here',
              'This port is a Shaken Fist client waiting for one login redirect, and '
              'this request was not one.'),
}


class _RedirectHandler(BaseHTTPRequestHandler):
    """Answer one OAuth redirect, and record for the flow what it was.

    The result is recorded on the server rather than returned, because a
    handler is constructed per request and the flow outlives it.

    Nothing here logs. The base class writes the request line to stderr
    for every request it serves, and in this flow the request line is
    the authorization code and the state: a credential and the nonce
    protecting it, printed on the terminal and into whatever is
    capturing it.

    The timeout is the one the stream handler puts on the connection
    socket, and it is the only thing bounding the read of a request which
    has connected but not spoken. The server's own timeout does not cover
    that, so without this the listener waits on such a connection
    forever.
    """

    def setup(self):
        """Bound the read of this connection before anything reads it.

        The stream handler applies its own class level timeout to the
        socket, and a class attribute belongs to every login at once, so
        the value this login was given is put on the instance first. A
        test can then bound the read by constructing a listener rather
        than by patching the class -- which would override the bound it
        was trying to prove was there.
        """
        self.timeout = self.server.read_timeout
        super().setup()

    def do_GET(self):
        params = dict(parse_qsl(urlparse(self.path).query, keep_blank_values=True))
        result = self._classify(params)
        self.server.result = result
        self._answer(result)

    def _classify(self, params):
        """What this request is, in the order the checks have to happen.

        The state is compared before the code is read, so that the code
        from a redirect which does not match this login is never put
        anywhere the flow can reach it. Doing it the other way round
        would leave the refusal correct and the code one edit away from
        being used.
        """
        if not any(params.get(field) for field in ('state', 'code', 'error')):
            return {'kind': 'other'}

        if not _states_match(self.server.login_state, params.get('state')):
            return {'kind': 'state'}

        if params.get('error'):
            return {'kind': 'error', 'error': params['error'],
                    'description': params.get('error_description')}

        if not params.get('code'):
            return {'kind': 'other'}

        return {'kind': 'code', 'code': params['code']}

    def _answer(self, result):
        status, heading, body = REDIRECT_PAGES[result['kind']]
        page = (REDIRECT_PAGE % (heading, body)).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(page)))
        self.end_headers()
        self.wfile.write(page)

    def log_message(self, format, *args):
        """Log nothing at all. See this class's docstring for why."""
        ...


class _RedirectServer(HTTPServer):
    """An HTTP server which exists to receive one redirect.

    Subclassed for three reasons. The state a handler compares against
    and the result it records belong to the login rather than to one
    request, and a handler is built per request. Whether anything
    connected at all is a different answer from what it sent, and the
    flow needs to tell a provider which never came back from one which
    came back with something unusable. And a failure inside a handler
    must not reach the terminal: the default prints a traceback, which
    for this server may carry the request line the handler was parsing.
    """

    def __init__(self, address, state, read_timeout=REDIRECT_READ_TIMEOUT):
        super().__init__(address, _RedirectHandler)
        self.login_state = state
        self.read_timeout = read_timeout
        self.result = None
        self.served = False

    def process_request(self, request, client_address):
        self.served = True
        super().process_request(request, client_address)

    def handle_error(self, request, client_address):
        """Swallow a handler failure rather than printing it.

        The result stays None, which the flow reports as a request that
        arrived and could not be used. That is the whole of what a
        person can act on, and the traceback the base class would print
        here is the one which carries the authorization code.
        """
        ...


class LoopbackRedirectListener:
    """A loopback listener which receives one authorization redirect.

    Three properties are load bearing, and each of them is one edit away
    from being lost:

    * It binds the loopback address and nothing else. A redirect carries
      an authorization code, so a listener on 0.0.0.0 would offer
      anything which can reach this machine the chance to race the
      provider's browser to the port, or to hand this client a code of
      its own.
    * The port is ephemeral, chosen by the kernel. Nothing has to be
      reserved for a login and two logins can run side by side.
    * It serves exactly one request and then stops. The redirect is the
      first thing the provider's browser sends here, so a second request
      is not part of this login -- and serving one request is also what
      denies anybody the repeated attempts that guessing the state would
      take.

    What has to be registered at the provider: RFC 8252 section 7.3
    describes this listener, and requires an authorization server to
    allow any port for a loopback redirect, comparing only the scheme,
    the host and the path. So one registration covers every login this
    client will ever run, and it is ``http://127.0.0.1/`` against the
    client identifier the cluster publishes. Where that is missing, or
    where the provider was given a fixed port or the name
    ``localhost``, the provider refuses the authorization request in the
    browser and redirects nothing here: the failure happens there and
    nothing arrives here to explain it, which is why the timeout refusal
    names the URI it was waiting on and says where to look.
    """

    def __init__(self, state, timeout=REDIRECT_TIMEOUT, address=LOOPBACK_ADDRESS,
                 clock=None, read_timeout=REDIRECT_READ_TIMEOUT):
        self._timeout = timeout
        self._clock = clock or time.monotonic
        self._server = _RedirectServer((address, 0), state,
                                       read_timeout=read_timeout)
        # handle_request() reads this off the server, and without it the
        # one request this listener serves is waited for forever.
        self._server.timeout = timeout

    @property
    def redirect_uri(self):
        """Where the provider is asked to send the person back to."""
        host, port = self._server.server_address[:2]
        return 'http://%s:%d/' % (host, port)

    def wait_for_redirect(self):
        """Wait for the one redirect, and return the code it carried.

        Every outcome other than a code is a refusal with something in
        it a person can act on, and none of them renders the request.

        One *request* is served, which is not the same as one connection.
        A connection which never sends a request line is not an attempt at
        this login -- a browser's speculative connection is the ordinary
        way that happens -- and treating it as one would let it decide the
        outcome of a login it was not part of. So such a connection is
        abandoned and the wait resumes with whatever is left of the
        deadline, while the first request which does arrive settles the
        matter whatever it says. That is what keeps the single attempt
        property: a redirect carrying the wrong state still stops the
        login, rather than buying another guess.
        """
        deadline = self._clock() + self._timeout
        while self._server.result is None:
            remaining = deadline - self._clock()
            if remaining <= 0:
                break
            self._server.timeout = remaining
            self._server.handle_request()

        if not self._server.served:
            raise LoginFailed(
                'The identity provider did not send the login back to %s within %d '
                'seconds, so no credentials were minted. If the browser showed an error '
                'at the provider rather than a login page, then this redirect is not '
                'registered there for this cluster: a provider has to accept '
                'http://%s/ with whatever port each login picks, which it is required '
                'to allow for a loopback address.'
                % (self.redirect_uri, self._timeout, LOOPBACK_ADDRESS))

        result = self._server.result
        if result is None or result['kind'] == 'other':
            raise LoginFailed(
                'Something reached %s which was not a login redirect, so this login has '
                'nothing to continue with. Run it again.' % self.redirect_uri)

        if result['kind'] == 'state':
            raise LoginFailed(
                'The login came back to this host carrying the wrong state, so it was '
                'refused and nothing in it was used. The redirect did not belong to the '
                'login this client started: run the login again, and if it keeps '
                'happening then something other than your browser is sending redirects '
                'to this port.')

        if result['kind'] == 'error':
            raise LoginFailed('The identity provider refused the login: %s'
                              % _redirect_error(result))

        return result['code']

    def close(self):
        """Give up the port. Safe to call more than once."""
        self._server.server_close()


def _redirect_error(result):
    """Render the error a redirect carried, and nothing else from it.

    The same two fields the token endpoint's refusals render, for the
    same reason: they are the only ones which say what went wrong and
    the only ones which are not part of a credential.
    """
    if result.get('description'):
        return '%s (%s)' % (result['error'], result['description'])
    return result['error']


def _authorization_url(issuer, redirect_uri, challenge, state):
    """The URL to send the person's browser to.

    Built on top of whatever query the endpoint already carries rather
    than by appending a question mark and hoping: an authorization
    endpoint with parameters of its own is a real thing -- a tenant
    identifier, say -- and replacing them would send the person
    somewhere that does not know this cluster.

    The scopes arrive from the cluster as one space separated string,
    which is already the format the scope parameter takes.
    """
    parsed = urlparse(issuer['authorization_endpoint'])
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params.update({
        'response_type': AUTHORIZATION_RESPONSE_TYPE,
        'client_id': issuer['client_id'],
        'redirect_uri': redirect_uri,
        'state': state,
        'code_challenge': challenge,
        'code_challenge_method': PKCE_CHALLENGE_METHOD,
    })
    if issuer.get('oidc_scopes'):
        params['scope'] = issuer['oidc_scopes']
    return urlunparse(parsed._replace(query=urlencode(params)))


def _open_browser(url):
    """Open the person's browser at the provider, and say whether it worked.

    webbrowser.open() answers False where it has no browser to open and
    raises where it found one it could not start. Both are the same
    answer here, and neither may end the login: the URL is printed
    instead, which is the only thing somebody on a host without a
    browser can act on.
    """
    try:
        return bool(webbrowser.open(url))
    except (webbrowser.Error, OSError):
        return False


def _browser_is_available():
    """Whether this host has a browser at all.

    Asked when the flow is being chosen rather than after one has
    started, because the browser flow cannot be completed on a host
    without one. The redirect comes back to a listener here, so a URL
    printed here and opened on another machine sends the provider's
    redirect to that machine's own loopback address, where nothing is
    listening.

    webbrowser.get() raises where it has nothing registered, which is
    the state of a container, or of an ssh session with no display and
    no terminal browser installed.
    """
    try:
        webbrowser.get()
        return True
    except webbrowser.Error:
        return False


def _send_the_person_to_the_provider(url, issuer, open_browser, tell):
    """Get the authorization request in front of the person, somehow.

    The URL is printed only where the browser could not be opened. It
    carries the state and the challenge rather than any secret the flow
    depends on -- the verifier is the secret, and it stays here -- but it
    is still the one thing this module prints which somebody could paste
    somewhere, so it is printed when it is the only way forward and not
    otherwise.
    """
    tell('')
    if open_browser(url):
        tell('A browser has been opened at your identity provider. Sign in there and '
             'this login will carry on by itself.')
    else:
        tell('There is no browser here to open, so open this URL yourself, in a browser '
             'on this machine -- the login comes back to a listener on this host, so a '
             'browser elsewhere cannot complete it:')
        tell('')
        tell(url)
        if issuer.get('device_authorization_endpoint'):
            tell('')
            tell('This provider also offers a login which needs no browser at all: '
                 'interrupt this and run it again with --flow=%s.' % FLOW_DEVICE)
    tell('')
    tell('Waiting for the login to be completed...')


def _exchange_code(issuer, code, verifier, redirect_uri):
    """Exchange the authorization code for an identity token.

    The redirect URI goes with it, as RFC 6749 section 4.1.3 requires:
    the provider compares it with the one the authorization request
    carried, and that comparison is what stops a code issued for one
    redirect being spent against another.

    Nothing here renders the body or the code. The body is a token
    response, and the code is a live credential until the provider
    answers.
    """
    endpoint = issuer['token_endpoint']
    body = {
        'grant_type': AUTHORIZATION_CODE_GRANT_TYPE,
        'code': code,
        'redirect_uri': redirect_uri,
        'client_id': issuer['client_id'],
        'code_verifier': verifier,
    }

    try:
        r = requests.post(endpoint, data=body, timeout=ISSUER_REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as e:
        raise LoginFailed('Could not reach the identity provider at %s: %s'
                          % (endpoint, e))

    document = _json_body(r)
    if r.status_code != 200:
        raise LoginFailed(
            'The identity provider would not complete the login: %s. The login itself '
            'happened -- the provider sent this client back a code -- so what it refused '
            'is the exchange of that code. The cluster may be registered at the provider '
            'as a client which has to send a secret, which a client running on a '
            'workstation has no way to keep.' % _oauth_error(r, document))

    token = document.get('id_token')
    if not token:
        raise LoginFailed(
            'The identity provider completed the login but returned no identity token, '
            'so there is nothing to present to the cluster. The provider may not be '
            'configured to issue one for the scopes this cluster asks for.')
    return token


def authorization_code_login(issuer, listener_factory=None, open_browser=None,
                             tell=None, timeout=REDIRECT_TIMEOUT):
    """Run an authorization code flow with PKCE, and return an identity token.

    The identity token is the only thing this returns, for the same
    reason the device flow returns only that: the provider's access and
    refresh tokens are credentials this cluster cannot revoke, so
    nothing here keeps them.

    The order matters. The listener is bound before the browser is
    opened, because the redirect URI has to name a port something is
    already listening on -- a browser which arrived first would be
    refused by the kernel. The port is given up before the code is
    exchanged, because it has done its job the moment the redirect
    arrives and an open port is something else's opportunity.

    listener_factory, open_browser and tell are injected so that a test
    can run the flow without a browser and without reaching a provider.
    The listener a test supplies is a real one on a real loopback port,
    because the socket is the thing worth testing.
    """
    tell = tell or _tell
    open_browser = open_browser or _open_browser
    listener_factory = listener_factory or LoopbackRedirectListener

    _warn_about_plaintext(issuer, tell, flow=FLOW_CODE)

    verifier, challenge = _pkce_pair()
    state = _new_state()

    listener = listener_factory(state, timeout=timeout)
    redirect_uri = listener.redirect_uri
    try:
        url = _authorization_url(issuer, redirect_uri, challenge, state)
        _send_the_person_to_the_provider(url, issuer, open_browser, tell)
        code = listener.wait_for_redirect()
    finally:
        listener.close()

    return _exchange_code(issuer, code, verifier, redirect_uri)


def _issuers_for_this_flow(issuers, flow=FLOW_DEVICE):
    """The issuers whose metadata says this flow can begin there.

    The cluster sends an endpoint only when it has one, deliberately, so
    the absence of one is the answer to "can this provider do this flow
    here". A client which posted to it anyway would be sending a client
    identifier to a URL it invented.

    Both of a flow's endpoints are required, not only the one which
    names it. The device flow polls the token endpoint and the browser
    flow exchanges its code there, so a provider with no token endpoint
    cannot finish either -- and it would fail after the person had
    authenticated rather than before they started.
    """
    required = FLOW_ENDPOINTS[flow]
    return [issuer for issuer in issuers
            if all(issuer.get(field) for field in required)]


def _issuer_names(issuers):
    """The provider names, for a refusal which has to name them."""
    return ', '.join(sorted(issuer['name'] for issuer in issuers))


def _the_other_flow(issuers, flow):
    """What to say about the flow the person did not ask for.

    Said only where it would work. A provider which advertises what the
    other flow needs is the difference between "this cluster cannot do
    this" and "ask for the other one", and nobody at the terminal can
    read the issuer metadata to work that out for themselves.
    """
    other = FLOW_CODE if flow == FLOW_DEVICE else FLOW_DEVICE
    usable = _issuers_for_this_flow(issuers, other)
    if not usable:
        return ''
    return (' %s advertises what %s needs, so "--flow=%s" would run here.'
            % (_issuer_names(usable), FLOW_DESCRIPTIONS[other], other))


def _choose_flow(issuers, requested=None, tell=None, browser_available=None):
    """Pick which flow to run, from what is advertised and what is here.

    The device flow is preferred wherever a provider offers it, because
    it asks nothing of this host: no browser, no loopback listener, no
    redirect registered at the provider. The browser flow is the
    fallback, for a provider which advertises no device authorization
    endpoint.

    An automatic choice will not land on the browser flow on a host with
    no browser, because the redirect has to come back to a listener
    here. A flow the person asked for is run regardless: they know
    things this cannot -- that a port is forwarded, or that the browser
    here is the one their identity is signed in to -- and the flow
    prints the URL rather than hanging when the browser will not open.
    """
    tell = tell or _tell
    browser_available = browser_available or _browser_is_available

    if not issuers:
        raise LoginFailed(NO_HUMAN_LOGIN)

    if requested:
        # Deliberately not checked against what the providers advertise
        # here. The provider choice reports that, and it can name which
        # providers would have suited the other flow instead.
        return requested

    if _issuers_for_this_flow(issuers, FLOW_DEVICE):
        return FLOW_DEVICE

    if _issuers_for_this_flow(issuers, FLOW_CODE):
        if browser_available():
            # Said out loud, because a browser opening by itself is
            # surprising on a verb which usually just prints a code.
            tell('No provider here offers a login which works without a browser, so '
                 'this login will open one.')
            return FLOW_CODE
        raise LoginFailed(
            'The only login %s offers needs a browser on this host to send you to the '
            'provider and a listener here for the answer to come back to, and there is '
            'no browser here. Log in from a host which has one, or ask an operator to '
            'configure a device authorization endpoint for this provider, which is the '
            'login that needs no browser. "--flow=%s" runs this one anyway and prints '
            'the URL for you to open.' % (_issuer_names(issuers), FLOW_CODE))

    raise LoginFailed(
        'This cluster offers human login through %s, but none of those providers '
        'advertises what either of the logins this client can run needs: %s, or %s. An '
        'operator configures those on the cluster trusted issuer.'
        % (_issuer_names(issuers), FLOW_REQUIREMENTS[FLOW_DEVICE],
           FLOW_REQUIREMENTS[FLOW_CODE]))


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


def _choose_issuer(issuers, tell=None, prompt=None, flow=FLOW_DEVICE):
    """Pick the provider to log in through, for the flow being run.

    An empty list is a complete answer rather than a fault: the cluster
    is saying it offers no human login, and the only thing to do with
    that is say so.

    Only the providers which can run this flow are offered, and where
    none of them can the refusal names what the flow needed. That
    refusal is also where a flow the person asked for meets a cluster
    which cannot run it, so it says whether the other flow would have
    worked instead.
    """
    tell = tell or _tell
    prompt = prompt or _prompt_for_issuer

    if not issuers:
        raise LoginFailed(NO_HUMAN_LOGIN)

    usable = sorted(_issuers_for_this_flow(issuers, flow), key=lambda i: i['name'])
    if not usable:
        raise LoginFailed(
            'This cluster offers human login through %s, but none of those providers '
            'advertises %s, which is what %s needs, so there is no login this client '
            'can run against them.%s'
            % (_issuer_names(issuers), FLOW_REQUIREMENTS[flow],
               FLOW_DESCRIPTIONS[flow], _the_other_flow(issuers, flow)))

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
@click.option('--flow', 'flow', default=None,
              type=click.Choice([FLOW_DEVICE, FLOW_CODE]),
              help=('Run a particular login flow instead of choosing one from what '
                    'the cluster advertises and what this host can do'))
@click.pass_context
def login(ctx, flow):
    """Log in to a cluster through one of the identity providers it trusts.

    Builds its own client, from an API URL alone, because the group
    cannot build one for somebody who has no credential yet -- which is
    the normal state for the first command a new user runs.

    The flow is chosen before the provider, because which providers can
    be used depends on which flow is being run. --flow is an override
    rather than a hint: a flow the person asked for is attempted even
    where the automatic choice would have avoided it.
    """
    try:
        client = _client_for_login(ctx.obj.get('APIURL'),
                                   verbose=bool(ctx.obj.get('VERBOSE')))
        issuers = _login_issuers(client)
        _check_the_cluster_can_mint(client)
        flow = _choose_flow(issuers, requested=flow)
        issuer = _choose_issuer(issuers, flow=flow)

        _tell('Logging in to %s through %s.' % (client.base_url, issuer['name']))
        if flow == FLOW_CODE:
            token = authorization_code_login(issuer)
        else:
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
