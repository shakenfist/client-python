import base64
import hashlib
import json
import os
import shutil
import socket
import tempfile
import threading
import time
import urllib.error
import urllib.request
from unittest import mock
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlparse

import requests
import testtools
from click.testing import CliRunner

from shakenfist_client import apiclient
from shakenfist_client import credentials
from shakenfist_client.commandline import auth


# Every secret in this file is deliberately unconvincing, and none of
# them is shaped like the thing it stands in for: a credential scanner
# runs over this repository's history with a positive control, so a
# fixture which looks like a real namespace key or a real JWT is a
# finding whether or not it ever unlocked anything. They are still
# distinctive strings, because the assertion several of these tests
# make is that none of them reaches the terminal.
CLUSTER = 'http://localhost:13000'
OTHER_CLUSTER = 'http://localhost:13001'
IDENTITY_TOKEN = 'not-an-identity-token-1'
PROVIDER_ACCESS_TOKEN = 'not-an-access-token-1'
DEVICE_CODE = 'not-a-device-code-1'
AUTHORIZATION_CODE = 'not-an-authorization-code-1'
OTHER_AUTHORIZATION_CODE = 'not-an-authorization-code-2'
REDIRECT_STATE = 'not-a-state-1'
WRONG_STATE = 'not-the-state-this-login-generated'
MINTED_KEY = 'not-a-key-1'
OTHER_MINTED_KEY = 'not-a-key-2'

# Stands in the place of a state value in a redirect a test asks for,
# and is replaced with whatever state the authorization request actually
# carried. A provider echoes the state back rather than inventing one,
# and a test which pinned a state of its own would be testing a
# comparison against itself.
ECHO_THE_STATE = object()

ISSUER = {
    'name': 'corporate',
    'issuer_url': 'https://idp.example.com/',
    'client_id': 'shakenfist',
    'oidc_scopes': 'openid profile groups',
    'authorization_endpoint': 'https://idp.example.com/authorize',
    'token_endpoint': 'https://idp.example.com/token',
    'device_authorization_endpoint': 'https://idp.example.com/device',
}

STARTED = {
    'device_code': DEVICE_CODE,
    'user_code': 'WDJB-MJHT',
    'verification_uri': 'https://idp.example.com/activate',
    'expires_in': 1800,
    'interval': 5,
}


def issuer(**kwargs):
    """One issuer as GET /auth/login serves it.

    A keyword set to None removes the field, because the endpoints are
    sent only when the cluster has them and their absence is what a
    client is meant to read.
    """
    entry = dict(ISSUER)
    entry.update(kwargs)
    return {k: v for k, v in entry.items() if v is not None}


def minted(namespace='ci', key_name='corporate-aaaa', key=MINTED_KEY,
           expiry=None, scopes=None):
    """One credential as POST /auth/login returns it."""
    return {
        'namespace': namespace,
        'key_name': key_name,
        'key': key,
        'expiry': expiry,
        'scopes': scopes if scopes is not None else ['blob.read'],
    }


def response(status_code=200, body=None):
    """One HTTP response from the identity provider.

    body of None stands for a body which is not JSON at all -- an error
    page from a proxy in front of the provider, which every error path
    here has to cope with.
    """
    r = mock.MagicMock()
    r.status_code = status_code
    if body is None:
        r.json.side_effect = ValueError('not json')
    else:
        r.json.return_value = body
    return r


def token_response(**kwargs):
    """A successful token response, carrying tokens we must not render."""
    body = {
        'id_token': IDENTITY_TOKEN,
        'access_token': PROVIDER_ACCESS_TOKEN,
        'token_type': 'Bearer',
    }
    body.update(kwargs)
    return response(200, body)


class FakeClock:
    """A monotonic clock which only moves when something sleeps.

    The poll loop measures its deadline with a monotonic clock and waits
    with sleep, so replacing both runs the whole flow in no time at all.
    The list of waits is then the assertion: the interval rules are the
    part of the device flow which is easy to get wrong and impossible to
    see from the outside.
    """

    def __init__(self):
        self.time = 1000.0
        self.waits = []

    def now(self):
        return self.time

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.time += seconds


class Teller:
    """Collects everything the flow says to the person."""

    def __init__(self):
        self.said = []

    def __call__(self, message):
        self.said.append(message)

    @property
    def text(self):
        return '\n'.join(self.said)


def unused_port():
    """A loopback port nothing is listening on.

    Bound and released, which is the only way to be told a port is free.
    Nothing in this process will take it afterwards, and a test which
    used it is asserting that a redirect sent there is lost rather than
    that the port stays free.
    """
    probe = socket.socket()
    try:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]
    finally:
        probe.close()


class Redirect:
    """The request the provider's browser makes to the loopback listener.

    Sent from a thread, because the two halves of a redirect are a
    server waiting and a browser arriving: the listener blocks the
    thread which called it, so a test which sent the request first would
    be testing a socket backlog rather than a listener.

    A real connection to the real port, which is the only honest test of
    a listener. It goes to the loopback address and nowhere else, and
    the thread is bounded by a timeout of its own so that a listener
    which never answers cannot hang the suite.

    It never goes through a proxy. A browser sends a loopback address
    straight to the loopback interface whatever proxy it is configured
    with, but urllib follows http_proxy for every host: on a runner
    which sets one, the request went to the proxy instead, the listener
    waited out its window, and a request to a port nothing listened on
    came back with the proxy's answer rather than a refused connection.
    """

    _opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def __init__(self, url):
        self.url = url
        self.status = None
        self.page = None
        self.error = None
        self._thread = threading.Thread(target=self._go)
        self._thread.daemon = True

    def start(self):
        self._thread.start()
        return self

    def _go(self):
        try:
            with self._opener.open(self.url, timeout=5) as r:
                self.status = r.getcode()
                self.page = r.read().decode('utf-8')
        except urllib.error.HTTPError as e:
            # A refusal is a page as much as a success is: it is what
            # the person is left looking at.
            self.status = e.code
            self.page = e.read().decode('utf-8')
        except Exception as e:
            self.error = e

    def wait(self):
        self._thread.join(10)
        return self


class DeviceAuthorizationFlowTestCase(testtools.TestCase):
    """The RFC 8628 flow itself, with the provider and the clock faked."""

    def setUp(self):
        super().setUp()
        self.post = mock.patch(
            'shakenfist_client.commandline.auth.requests.post').start()
        self.addCleanup(mock.patch.stopall)
        self.clock = FakeClock()
        self.teller = Teller()

    def _run(self, responses, issuer_metadata=None):
        self.post.side_effect = responses
        return auth.device_authorization_login(
            issuer_metadata or ISSUER, sleep=self.clock.sleep,
            clock=self.clock.now, tell=self.teller)

    def test_a_completed_login_returns_the_identity_token(self):
        token = self._run([response(200, dict(STARTED)), token_response()])

        self.assertEqual(IDENTITY_TOKEN, token)

    def test_the_device_request_sends_the_client_id_and_the_scopes(self):
        # oidc_scopes arrives from the cluster as one space separated
        # string, which is already the format the scope parameter takes.
        self._run([response(200, dict(STARTED)), token_response()])

        self.assertEqual(ISSUER['device_authorization_endpoint'],
                         self.post.call_args_list[0][0][0])
        self.assertEqual({'client_id': 'shakenfist',
                          'scope': 'openid profile groups'},
                         self.post.call_args_list[0][1]['data'])

    def test_the_device_request_has_a_timeout(self):
        # A provider which accepts the socket and then says nothing would
        # otherwise park the terminal for as long as it liked, which is
        # the failure the poll deadline exists to prevent on the other
        # side of the flow.
        self._run([response(200, dict(STARTED)), token_response()])

        for call in self.post.call_args_list:
            self.assertEqual(auth.ISSUER_REQUEST_TIMEOUT,
                             call[1]['timeout'])

    def test_the_poll_sends_the_device_code_grant(self):
        self._run([response(200, dict(STARTED)), token_response()])

        self.assertEqual(ISSUER['token_endpoint'],
                         self.post.call_args_list[1][0][0])
        self.assertEqual(
            {'grant_type': auth.DEVICE_CODE_GRANT_TYPE,
             'device_code': DEVICE_CODE,
             'client_id': 'shakenfist'},
            self.post.call_args_list[1][1]['data'])

    def test_the_person_is_told_the_code_and_the_uri(self):
        self._run([response(200, dict(STARTED)), token_response()])

        self.assertIn('WDJB-MJHT', self.teller.text)
        self.assertIn('https://idp.example.com/activate', self.teller.text)

    def test_a_complete_verification_uri_is_preferred(self):
        started = dict(STARTED)
        started['verification_uri_complete'] = \
            'https://idp.example.com/activate?user_code=WDJB-MJHT'
        self._run([response(200, started), token_response()])

        self.assertIn('activate?user_code=WDJB-MJHT', self.teller.text)
        # The code is still shown, because the provider displays it for
        # confirmation and the person should be able to check they match.
        self.assertIn('WDJB-MJHT', self.teller.text)

    def test_nothing_the_person_is_told_is_a_secret(self):
        self._run([response(200, dict(STARTED)), token_response()])

        self.assertNotIn(IDENTITY_TOKEN, self.teller.text)
        self.assertNotIn(PROVIDER_ACCESS_TOKEN, self.teller.text)
        self.assertNotIn(DEVICE_CODE, self.teller.text)

    def test_a_plaintext_endpoint_is_called_out(self):
        self._run([response(200, dict(STARTED)), token_response()],
                  issuer_metadata=issuer(
                      device_authorization_endpoint='http://idp.example.com/device',
                      token_endpoint='http://idp.example.com/token'))

        self.assertIn('not https', self.teller.text)

    def test_an_https_endpoint_is_not_called_out(self):
        self._run([response(200, dict(STARTED)), token_response()])

        self.assertNotIn('not https', self.teller.text)

    # The four polling rules. Each of these is a rule this client would
    # be breaking in somebody else's logs if it stopped obeying it.
    def test_the_providers_interval_is_waited_out(self):
        started = dict(STARTED)
        started['interval'] = 10
        self._run([response(200, started),
                   response(400, {'error': 'authorization_pending'}),
                   token_response()])

        self.assertEqual([10, 10], self.clock.waits)

    def test_a_missing_interval_falls_back_to_five_seconds(self):
        started = dict(STARTED)
        del started['interval']
        self._run([response(200, started), token_response()])

        self.assertEqual([auth.DEFAULT_POLL_INTERVAL], self.clock.waits)

    def test_an_unusable_interval_falls_back_to_five_seconds(self):
        # A zero is a broken answer rather than an invitation to spin on
        # somebody else's token endpoint.
        started = dict(STARTED)
        started['interval'] = 0
        self._run([response(200, started), token_response()])

        self.assertEqual([auth.DEFAULT_POLL_INTERVAL], self.clock.waits)

    def test_authorization_pending_keeps_waiting(self):
        token = self._run([
            response(200, dict(STARTED)),
            response(400, {'error': 'authorization_pending'}),
            response(400, {'error': 'authorization_pending'}),
            token_response()])

        self.assertEqual(IDENTITY_TOKEN, token)
        self.assertEqual([5, 5, 5], self.clock.waits)

    def test_slow_down_lengthens_the_interval(self):
        token = self._run([
            response(200, dict(STARTED)),
            response(400, {'error': 'slow_down'}),
            response(400, {'error': 'slow_down'}),
            token_response()])

        self.assertEqual(IDENTITY_TOKEN, token)
        self.assertEqual([5, 10, 15], self.clock.waits)

    def test_the_flow_gives_up_at_the_expiry(self):
        started = dict(STARTED)
        started['expires_in'] = 12
        pending = response(400, {'error': 'authorization_pending'})
        self.post.side_effect = [response(200, started)] + [pending] * 100

        e = self.assertRaises(
            auth.LoginFailed, auth.device_authorization_login, ISSUER,
            sleep=self.clock.sleep, clock=self.clock.now, tell=self.teller)

        # Three polls at five seconds and then a stop, rather than a
        # fourth which the provider has already stopped honouring.
        self.assertEqual([5, 5, 2], self.clock.waits)
        self.assertIn('not completed in time', str(e))
        self.assertIn('WDJB-MJHT', str(e))

    def test_a_missing_expiry_is_still_bounded(self):
        # The field is required, so a provider which omits it is broken;
        # but the alternative to a bound of our own is a terminal which
        # never comes back.
        started = dict(STARTED)
        del started['expires_in']
        pending = response(400, {'error': 'authorization_pending'})
        self.post.side_effect = [response(200, started)] + [pending] * 1000

        self.assertRaises(
            auth.LoginFailed, auth.device_authorization_login, ISSUER,
            sleep=self.clock.sleep, clock=self.clock.now, tell=self.teller)

        self.assertEqual(auth.DEFAULT_EXPIRES_IN, sum(self.clock.waits))

    def test_an_unusable_expiry_is_still_bounded(self):
        # A zero or a negative expiry is as unbounded as a missing one if
        # it is taken at face value, and this is the guard which stops a
        # provider sending one from parking the terminal.
        started = dict(STARTED)
        started['expires_in'] = 0
        pending = response(400, {'error': 'authorization_pending'})
        self.post.side_effect = [response(200, started)] + [pending] * 1000

        self.assertRaises(
            auth.LoginFailed, auth.device_authorization_login, ISSUER,
            sleep=self.clock.sleep, clock=self.clock.now, tell=self.teller)

        self.assertEqual(auth.DEFAULT_EXPIRES_IN, sum(self.clock.waits))

    def test_expired_token_stops_the_flow(self):
        e = self.assertRaises(
            auth.LoginFailed, self._run,
            [response(200, dict(STARTED)),
             response(400, {'error': 'expired_token'})])

        self.assertIn('expired', str(e))

    def test_access_denied_stops_the_flow(self):
        e = self.assertRaises(
            auth.LoginFailed, self._run,
            [response(200, dict(STARTED)),
             response(400, {'error': 'access_denied'})])

        self.assertIn('refused at the identity provider', str(e))

    def test_an_unknown_oauth_error_stops_the_flow(self):
        e = self.assertRaises(
            auth.LoginFailed, self._run,
            [response(200, dict(STARTED)),
             response(400, {'error': 'invalid_client',
                            'error_description': 'no such client'})])

        self.assertIn('invalid_client', str(e))
        self.assertIn('no such client', str(e))

    def test_a_body_which_is_not_json_stops_the_flow(self):
        # A proxy in front of the provider answering with an error page.
        e = self.assertRaises(
            auth.LoginFailed, self._run,
            [response(200, dict(STARTED)), response(502)])

        self.assertIn('HTTP 502', str(e))

    def test_a_token_response_with_no_identity_token_is_refused(self):
        # The cluster validates an identity token's audience against the
        # client identifier it registered, so an access token is not a
        # substitute for one. The refusal must not render the access
        # token the provider did send.
        e = self.assertRaises(
            auth.LoginFailed, self._run,
            [response(200, dict(STARTED)),
             response(200, {'access_token': PROVIDER_ACCESS_TOKEN})])

        self.assertIn('no identity token', str(e))
        self.assertNotIn(PROVIDER_ACCESS_TOKEN, str(e))

    def test_a_refused_device_request_stops_the_flow(self):
        e = self.assertRaises(
            auth.LoginFailed, self._run,
            [response(400, {'error': 'invalid_scope'})])

        self.assertIn('invalid_scope', str(e))
        self.assertEqual(1, self.post.call_count)

    def test_a_device_response_missing_a_field_stops_the_flow(self):
        started = dict(STARTED)
        del started['user_code']

        e = self.assertRaises(
            auth.LoginFailed, self._run, [response(200, started)])

        self.assertIn('user_code', str(e))

    def test_an_unreachable_provider_stops_the_flow(self):
        self.post.side_effect = apiclient.requests.exceptions.ConnectTimeout(
            'timed out')

        e = self.assertRaises(
            auth.LoginFailed, auth.device_authorization_login, ISSUER,
            sleep=self.clock.sleep, clock=self.clock.now, tell=self.teller)

        self.assertIn('Could not reach the identity provider', str(e))


class PkceTestCase(testtools.TestCase):
    """The verifier, the challenge and the state the browser flow makes."""

    def test_the_verifier_is_a_legal_pkce_verifier(self):
        verifier, _ = auth._pkce_pair()

        # RFC 7636 section 4.1: 43 to 128 characters, and only the
        # unreserved set, which is what base64url without padding is.
        self.assertTrue(43 <= len(verifier) <= 128, len(verifier))
        self.assertEqual('', verifier.strip('ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                                            'abcdefghijklmnopqrstuvwxyz'
                                            '0123456789-._~'))

    def test_the_challenge_is_the_s256_digest_of_the_verifier(self):
        # The whole point of S256: what the browser carries is a digest,
        # so reading the authorization request does not hand anybody the
        # secret which completes the exchange.
        verifier, challenge = auth._pkce_pair()

        expected = base64.urlsafe_b64encode(
            hashlib.sha256(verifier.encode('ascii')).digest()).rstrip(b'=')
        self.assertEqual(expected.decode('ascii'), challenge)
        self.assertEqual('S256', auth.PKCE_CHALLENGE_METHOD)

    def test_no_two_logins_share_a_verifier_or_a_state(self):
        verifiers = {auth._pkce_pair()[0] for _ in range(20)}
        states = {auth._new_state() for _ in range(20)}

        self.assertEqual(20, len(verifiers))
        self.assertEqual(20, len(states))
        self.assertTrue(len(states.pop()) >= 32)

    def test_the_state_comparison_refuses_everything_but_a_match(self):
        self.assertTrue(auth._states_match(REDIRECT_STATE, REDIRECT_STATE))
        self.assertFalse(auth._states_match(REDIRECT_STATE, WRONG_STATE))
        # Same length, one character different: a comparison which
        # stopped at the length would pass this.
        self.assertFalse(auth._states_match(REDIRECT_STATE,
                                            REDIRECT_STATE[:-1] + 'X'))
        self.assertFalse(auth._states_match(REDIRECT_STATE, None))
        self.assertFalse(auth._states_match(REDIRECT_STATE, ''))
        self.assertFalse(auth._states_match('', ''))
        # compare_digest raises on a string which is not ASCII, and that
        # must be a refusal rather than a traceback out of the handler.
        self.assertFalse(auth._states_match(REDIRECT_STATE, 'stäte'))


class LoopbackListenerTestCase(testtools.TestCase):
    """The listener, on a real loopback port, with real connections.

    A socket is the thing worth testing, so these bind one. Every
    listener here is given a timeout measured in fractions of a second:
    a listener with no timeout at all hangs by design, so these tests
    are also what stops that reaching the suite.
    """

    def _listener(self, state=REDIRECT_STATE, timeout=10, read_timeout=5):
        """A listener for one test, bounded so a failure cannot hang the suite.

        Both bounds are generous by default and short only in the tests
        which are about a bound being reached. A test which expects the
        redirect to arrive never waits for either of these, so a large
        value costs it nothing -- while a small one costs it everything on
        a machine slower or busier than the one it was written on, which
        is how twenty-four of these first failed in CI while passing here.
        """
        listener = auth.LoopbackRedirectListener(state, timeout=timeout,
                                                 read_timeout=read_timeout)
        self.addCleanup(listener.close)
        return listener

    def _redirect(self, listener, **params):
        """Send the redirect a provider's browser would send."""
        query = dict(params)
        return Redirect('%s?%s' % (listener.redirect_uri, urlencode(query))).start()

    def _queued_redirect(self, listener, **params):
        """A redirect connected from this thread, ahead of any started later.

        Redirect connects from a thread of its own, which is right
        wherever the order does not matter. It does matter for a test
        which queues two of them and asserts which one was served: two
        threads racing to connect settle that by chance, and the test
        passes or fails with the race. connect() has returned by the time
        this does, so this connection is ahead of anything started after
        it in the listener's accept queue, and its request is already
        written for the listener to read.

        The response is read with _answer_to() rather than here, because
        nothing answers this connection until the listener runs.
        """
        sock = socket.create_connection(listener._server.server_address[:2])
        self.addCleanup(sock.close)
        sock.settimeout(5)
        sock.sendall(('GET /?%s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n'
                      % (urlencode(dict(params)), auth.LOOPBACK_ADDRESS)).encode('ascii'))
        return sock

    def _answer_to(self, sock):
        """Everything the listener wrote back on a raw connection."""
        answer = b''
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            answer += chunk
        return answer.decode('utf-8', 'replace')

    def test_it_binds_the_loopback_address_and_an_ephemeral_port(self):
        # Never 0.0.0.0: a redirect carries an authorization code, so a
        # listener reachable from the network is a code anybody on it
        # can race the browser for.
        first = self._listener()
        second = self._listener()

        self.assertEqual('127.0.0.1', auth.LOOPBACK_ADDRESS)
        self.assertEqual('127.0.0.1', first._server.server_address[0])
        self.assertNotEqual(0, first._server.server_address[1])
        self.assertNotEqual(first._server.server_address[1],
                            second._server.server_address[1])
        self.assertTrue(first.redirect_uri.startswith('http://127.0.0.1:'))

    def _stalled_connection(self, listener):
        """A connection which says nothing, as a browser's preconnect does."""
        sock = socket.create_connection(listener._server.server_address[:2])
        self.addCleanup(sock.close)
        return sock

    def test_a_connection_which_says_nothing_does_not_decide_the_login(self):
        # The server's own timeout bounds waiting for a connection and
        # nothing else, so reading the request is a second wait: a
        # connection which opens the port and then says nothing used to
        # hold the login open for as long as it liked. A browser making a
        # speculative connection does that by accident, and any other
        # process on this host can do it on purpose.
        #
        # The read bound is short because this test needs it reached, and
        # the overall one is long because this test must not reach that.
        listener = self._listener(timeout=10, read_timeout=0.2)

        # The silent connection is accepted first and the real redirect
        # waits in the backlog behind it, so this does not depend on the
        # two racing: the listener has to give up on the first before it
        # can reach the second.
        self._stalled_connection(listener)
        delivered = self._redirect(listener, code=AUTHORIZATION_CODE,
                                   state=REDIRECT_STATE)

        self.assertEqual(AUTHORIZATION_CODE, listener.wait_for_redirect())
        delivered.wait()

    def test_a_connection_which_says_nothing_is_still_bounded(self):
        # And when nothing else ever arrives, the wait ends rather than
        # lasting as long as whatever is holding the socket. Short bounds
        # here on purpose: this test is about reaching them.
        listener = self._listener(timeout=0.5, read_timeout=0.2)
        self._stalled_connection(listener)

        e = self.assertRaises(auth.LoginFailed, listener.wait_for_redirect)

        self.assertIn('not a login redirect', str(e))

    def test_the_read_is_bounded_by_default(self):
        # Every test here injects a short bound, so none of them would
        # notice a default of None -- which is what the stream handler
        # uses when nothing sets one, and which is unbounded.
        listener = auth.LoopbackRedirectListener(REDIRECT_STATE)
        self.addCleanup(listener.close)

        self.assertIsNotNone(auth.REDIRECT_READ_TIMEOUT)
        self.assertGreater(auth.REDIRECT_READ_TIMEOUT, 0)
        self.assertEqual(auth.REDIRECT_READ_TIMEOUT,
                         listener._server.read_timeout)

    def test_a_wrong_state_ends_the_login_rather_than_buying_a_guess(self):
        # Waiting out a connection which said nothing must not have turned
        # the single attempt into a retry loop. A redirect which carries
        # the wrong state is an answer, so the login stops on it -- and
        # whoever sent it does not get to try again with another state.
        #
        # Both redirects are queued before the listener runs, so the
        # second is there to be served if anything is willing to serve it.
        # A listener which answered it would hand this login a code from a
        # redirect that arrived after it had already been told no.
        #
        # The refused one is connected from this thread so that it is
        # certainly the first in the accept queue. Started as a thread of
        # its own it raced the second, and the test asserted the outcome
        # of that race rather than the listener's behaviour.
        listener = self._listener(timeout=5)
        refused = self._queued_redirect(listener, code=AUTHORIZATION_CODE,
                                        state='not-the-state-this-login-sent')
        second = self._redirect(listener, code=OTHER_AUTHORIZATION_CODE,
                                state=REDIRECT_STATE)

        e = self.assertRaises(auth.LoginFailed, listener.wait_for_redirect)

        self.assertIn('wrong state', str(e))
        answer = self._answer_to(refused)
        self.assertIn('400', answer.splitlines()[0])
        self.assertNotIn(AUTHORIZATION_CODE, answer)

        # The second redirect is deliberately never waited on. Its job was
        # to be sitting there ready to be served, and a listener which did
        # the right thing leaves it unanswered -- so waiting would mean
        # waiting out its own timeout for nothing. Its thread is a daemon
        # and the cleanup closes the port under it.
        self.assertIsNone(second.status)

    def test_a_matching_redirect_hands_over_the_code(self):
        listener = self._listener()
        delivered = self._redirect(listener, code=AUTHORIZATION_CODE,
                                   state=REDIRECT_STATE)

        self.assertEqual(AUTHORIZATION_CODE, listener.wait_for_redirect())

        delivered.wait()
        self.assertEqual(200, delivered.status)
        self.assertIn('close this tab', delivered.page)
        # The page is the last thing the person sees, and it must not be
        # the code they just exchanged.
        self.assertNotIn(AUTHORIZATION_CODE, delivered.page)
        self.assertNotIn(REDIRECT_STATE, delivered.page)

    def test_a_mismatched_state_is_refused_and_the_code_discarded(self):
        listener = self._listener()
        delivered = self._redirect(listener, code=AUTHORIZATION_CODE,
                                   state=WRONG_STATE)

        e = self.assertRaises(auth.LoginFailed, listener.wait_for_redirect)

        self.assertIn('wrong state', str(e))
        self.assertNotIn(AUTHORIZATION_CODE, str(e))
        # Checked before the code is read, so the code from a redirect
        # which does not match this login is never recorded anywhere the
        # flow could reach it.
        self.assertEqual({'kind': 'state'}, listener._server.result)
        delivered.wait()
        self.assertEqual(400, delivered.status)
        self.assertIn('does not match', delivered.page)

    def test_a_redirect_with_no_state_at_all_is_refused(self):
        listener = self._listener()
        self._redirect(listener, code=AUTHORIZATION_CODE)

        e = self.assertRaises(auth.LoginFailed, listener.wait_for_redirect)

        self.assertIn('wrong state', str(e))
        self.assertEqual({'kind': 'state'}, listener._server.result)

    def test_a_redirect_to_the_wrong_port_is_never_received(self):
        # The port is in the redirect URI the authorization request
        # carried, and a provider registered with a fixed port instead
        # sends the person somewhere nothing is listening. The listener
        # holds one port, does not hear that, and ends in its timeout
        # rather than accepting a redirect meant for elsewhere.
        listener = self._listener(timeout=0.25)
        elsewhere = unused_port()
        self.assertNotEqual(elsewhere, listener._server.server_address[1])

        delivered = Redirect(
            'http://127.0.0.1:%d/?%s'
            % (elsewhere, urlencode({'code': AUTHORIZATION_CODE,
                                     'state': REDIRECT_STATE}))).start().wait()

        e = self.assertRaises(auth.LoginFailed, listener.wait_for_redirect)

        self.assertIsInstance(delivered.error, urllib.error.URLError)
        self.assertIn('did not send the login back', str(e))
        self.assertIn(listener.redirect_uri, str(e))

    def test_it_times_out_rather_than_waiting_forever(self):
        listener = self._listener(timeout=0.25)

        started = time.monotonic()
        e = self.assertRaises(auth.LoginFailed, listener.wait_for_redirect)
        waited = time.monotonic() - started

        self.assertTrue(waited < 10, waited)
        self.assertIn('did not send the login back', str(e))
        # The failure happens at the provider when the redirect is not
        # registered there, and nothing arrives here to say so, which is
        # why the refusal points at the registration.
        self.assertIn('not registered there', str(e))
        self.assertIn('127.0.0.1', str(e))

    def test_a_provider_error_in_the_redirect_is_reported(self):
        listener = self._listener()
        delivered = self._redirect(listener, state=REDIRECT_STATE,
                                   error='access_denied',
                                   error_description='user said no')

        e = self.assertRaises(auth.LoginFailed, listener.wait_for_redirect)

        self.assertIn('access_denied', str(e))
        self.assertIn('user said no', str(e))
        delivered.wait()
        self.assertEqual(400, delivered.status)

    def test_a_request_which_is_not_a_redirect_ends_the_wait(self):
        # The listener serves exactly one request, so anything else
        # which reaches the port ends this login rather than being
        # ignored while it keeps waiting. On a loopback port with an
        # ephemeral number and a lifetime of seconds, that is a trade
        # worth making: the redirect is the first thing the provider's
        # browser sends here.
        listener = self._listener()
        delivered = Redirect(listener.redirect_uri).start()

        e = self.assertRaises(auth.LoginFailed, listener.wait_for_redirect)

        self.assertIn('not a login redirect', str(e))
        delivered.wait()
        self.assertEqual(404, delivered.status)
        self.assertIn('waiting for one login redirect', delivered.page)

    def test_a_request_the_handler_will_not_answer_ends_the_wait(self):
        # Something connected and made a request this listener cannot use,
        # which is a different answer from nothing having connected at
        # all: the refusal says so rather than reporting a timeout which
        # did not happen.
        #
        # And it says so *now*. The base class answers an unsupported
        # method itself, without ever reaching the handler's own method,
        # so a listener which only watched for a recorded result would
        # wait out the rest of its window on one stray request. That is
        # the whole difference between this and a connection which sends
        # nothing: this one arrived.
        listener = self._listener(timeout=10)
        delivered = Redirect(urllib.request.Request(
            listener.redirect_uri, data=b'', method='POST')).start()

        started = time.monotonic()
        e = self.assertRaises(auth.LoginFailed, listener.wait_for_redirect)
        waited = time.monotonic() - started

        self.assertIn('not a login redirect', str(e))
        self.assertTrue(listener._server.served)
        self.assertLess(waited, 5, 'waited out the window: %.1fs' % waited)
        delivered.wait()

    def test_the_port_does_not_outlive_the_login(self):
        listener = self._listener()
        self._redirect(listener, code=AUTHORIZATION_CODE, state=REDIRECT_STATE)
        listener.wait_for_redirect()

        listener.close()

        after = Redirect('%s?%s' % (listener.redirect_uri,
                                    urlencode({'code': AUTHORIZATION_CODE,
                                               'state': REDIRECT_STATE}))).start().wait()
        self.assertIsInstance(after.error, urllib.error.URLError)

    def test_closing_twice_is_not_an_error(self):
        # The flow closes the listener in a finally, and a test or a
        # caller which also closes it must not turn a failed login into
        # a traceback.
        listener = self._listener()

        listener.close()
        listener.close()


class AuthorizationCodeFlowTestCase(testtools.TestCase):
    """The browser flow end to end, over a real loopback socket.

    The browser and the provider's token endpoint are faked; the
    listener is not. The redirect is sent from inside the fake browser
    because that is the moment a real one would send it -- the port is
    bound and the authorization URL exists -- and the state it echoes
    back is read out of that URL, as a provider reads it.
    """

    def setUp(self):
        super().setUp()
        self.post = mock.patch(
            'shakenfist_client.commandline.auth.requests.post').start()
        self.addCleanup(mock.patch.stopall)
        self.post.return_value = token_response()
        self.teller = Teller()
        self.opened = []
        self.browser_works = True
        self.answer = {'code': AUTHORIZATION_CODE, 'state': ECHO_THE_STATE}
        # Long enough that a slow or busy machine does not read a redirect
        # which is on its way as a redirect which never came. The two
        # tests which are about the timeout being reached set their own.
        self.timeout = 10
        self.listener = None
        self.delivered = None

    def _open_browser(self, url):
        self.opened.append(url)
        if self.answer is not None:
            params = dict(self.answer)
            if params.get('state') is ECHO_THE_STATE:
                params['state'] = dict(parse_qsl(urlparse(url).query))['state']
            self.delivered = Redirect(
                '%s?%s' % (self.listener.redirect_uri, urlencode(params))).start()
        return self.browser_works

    def _listener_factory(self, state, timeout=None):
        self.listener = auth.LoopbackRedirectListener(state, timeout=self.timeout)
        self.addCleanup(self.listener.close)
        return self.listener

    def _run(self, issuer_metadata=None):
        return auth.authorization_code_login(
            issuer_metadata or ISSUER, listener_factory=self._listener_factory,
            open_browser=self._open_browser, tell=self.teller)

    def _authorization_request(self):
        return dict(parse_qsl(urlparse(self.opened[0]).query))

    def test_a_completed_login_returns_the_identity_token(self):
        self.assertEqual(IDENTITY_TOKEN, self._run())

    def test_the_authorization_request_carries_pkce_and_the_state(self):
        self._run()

        asked = self._authorization_request()
        self.assertEqual(ISSUER['authorization_endpoint'],
                         self.opened[0].split('?')[0])
        self.assertEqual('code', asked['response_type'])
        self.assertEqual('shakenfist', asked['client_id'])
        self.assertEqual('openid profile groups', asked['scope'])
        self.assertEqual('S256', asked['code_challenge_method'])
        self.assertEqual(self.listener.redirect_uri, asked['redirect_uri'])
        self.assertTrue(len(asked['state']) >= 32)

        # The verifier is the secret which completes the exchange, and
        # the browser must never see it: what it carries is the digest.
        verifier = self.post.call_args[1]['data']['code_verifier']
        self.assertNotIn(verifier, self.opened[0])
        self.assertEqual(
            base64.urlsafe_b64encode(
                hashlib.sha256(verifier.encode('ascii')).digest()
            ).rstrip(b'=').decode('ascii'),
            asked['code_challenge'])

    def test_the_endpoints_own_query_parameters_survive(self):
        # An authorization endpoint with a tenant in its query is a real
        # thing, and replacing it would send the person somewhere which
        # does not know this cluster.
        self._run(issuer_metadata=issuer(
            authorization_endpoint='https://idp.example.com/authorize?tenant=42'))

        self.assertEqual('42', self._authorization_request()['tenant'])
        self.assertEqual('code', self._authorization_request()['response_type'])

    def test_the_exchange_presents_the_code_and_the_verifier(self):
        self._run()

        self.assertEqual(ISSUER['token_endpoint'], self.post.call_args[0][0])
        body = self.post.call_args[1]['data']
        self.assertEqual(auth.AUTHORIZATION_CODE_GRANT_TYPE, body['grant_type'])
        self.assertEqual(AUTHORIZATION_CODE, body['code'])
        self.assertEqual('shakenfist', body['client_id'])
        # RFC 6749 section 4.1.3 requires the redirect URI again: the
        # provider compares it with the one the authorization request
        # carried, which is what stops a code being spent elsewhere.
        self.assertEqual(self.listener.redirect_uri, body['redirect_uri'])
        self.assertEqual(auth.ISSUER_REQUEST_TIMEOUT,
                         self.post.call_args[1]['timeout'])

    def test_nothing_the_person_is_told_is_a_secret(self):
        self._run()

        verifier = self.post.call_args[1]['data']['code_verifier']
        state = self._authorization_request()['state']
        for secret in (IDENTITY_TOKEN, PROVIDER_ACCESS_TOKEN,
                       AUTHORIZATION_CODE, verifier, state):
            self.assertNotIn(secret, self.teller.text)
        # The negative control: the conversation did happen.
        self.assertIn('Waiting for the login', self.teller.text)

    def test_no_browser_prints_the_url_instead_of_hanging(self):
        self.browser_works = False

        self.assertEqual(IDENTITY_TOKEN, self._run())

        # The URL is the one thing printed, because it is the only thing
        # somebody on a host with no browser can act on. It carries the
        # state and the challenge; it carries neither the verifier nor
        # the code.
        self.assertIn(self.opened[0], self.teller.text)
        self.assertNotIn(self.post.call_args[1]['data']['code_verifier'],
                         self.teller.text)
        self.assertIn('no browser here', self.teller.text)
        # This provider can also do the flow which needs no browser, so
        # the better answer is offered rather than left to be found.
        self.assertIn('--flow=device', self.teller.text)

    def test_a_browser_which_opened_is_not_asked_to_print_the_url(self):
        self._run()

        self.assertNotIn(self.opened[0], self.teller.text)
        self.assertIn('browser has been opened', self.teller.text)

    def test_no_browser_and_no_device_endpoint_offers_no_alternative(self):
        self.browser_works = False

        self._run(issuer_metadata=issuer(device_authorization_endpoint=None))

        self.assertIn(self.opened[0], self.teller.text)
        self.assertNotIn('--flow=device', self.teller.text)

    def test_a_mismatched_state_stops_the_flow_before_the_exchange(self):
        self.answer = {'code': AUTHORIZATION_CODE, 'state': WRONG_STATE}

        e = self.assertRaises(auth.LoginFailed, self._run)

        self.assertIn('wrong state', str(e))
        # The code in that redirect is never presented to the provider,
        # which is the whole purpose of the check.
        self.post.assert_not_called()

    def test_the_flow_times_out_rather_than_waiting_forever(self):
        self.answer = None
        self.timeout = 0.25

        started = time.monotonic()
        e = self.assertRaises(auth.LoginFailed, self._run)
        waited = time.monotonic() - started

        self.assertTrue(waited < 10, waited)
        self.assertIn('did not send the login back', str(e))
        self.post.assert_not_called()

    def test_the_port_is_given_up_even_when_the_login_fails(self):
        self.answer = None
        self.timeout = 0.25

        self.assertRaises(auth.LoginFailed, self._run)

        after = Redirect(self.listener.redirect_uri).start().wait()
        self.assertIsInstance(after.error, urllib.error.URLError)

    def test_a_plaintext_authorization_endpoint_is_called_out(self):
        self._run(issuer_metadata=issuer(
            authorization_endpoint='http://idp.example.com/authorize'))

        self.assertIn('not https', self.teller.text)
        self.assertIn('authorization endpoint', self.teller.text)

    def test_a_plaintext_endpoint_this_flow_will_not_use_is_not_called_out(self):
        # Warning about a connection this login will never open teaches
        # a person to ignore the warning.
        self._run(issuer_metadata=issuer(
            device_authorization_endpoint='http://idp.example.com/device'))

        self.assertNotIn('not https', self.teller.text)

    def test_a_refused_exchange_says_the_login_itself_worked(self):
        self.post.return_value = response(400, {'error': 'invalid_grant',
                                                'error_description': 'bad code'})

        e = self.assertRaises(auth.LoginFailed, self._run)

        self.assertIn('invalid_grant', str(e))
        self.assertIn('bad code', str(e))
        self.assertIn('would not complete the login', str(e))
        self.assertNotIn(AUTHORIZATION_CODE, str(e))

    def test_an_exchange_which_returns_no_identity_token_is_refused(self):
        self.post.return_value = response(
            200, {'access_token': PROVIDER_ACCESS_TOKEN})

        e = self.assertRaises(auth.LoginFailed, self._run)

        self.assertIn('no identity token', str(e))
        self.assertNotIn(PROVIDER_ACCESS_TOKEN, str(e))

    def test_an_unreachable_token_endpoint_stops_the_flow(self):
        self.post.side_effect = apiclient.requests.exceptions.ConnectTimeout(
            'timed out')

        e = self.assertRaises(auth.LoginFailed, self._run)

        self.assertIn('Could not reach the identity provider', str(e))


class BrowserTestCase(testtools.TestCase):
    """Opening a browser, and finding out whether there is one."""

    def test_a_browser_which_opens_is_reported_as_working(self):
        with mock.patch('shakenfist_client.commandline.auth.webbrowser.open',
                        return_value=True) as opened:
            self.assertTrue(auth._open_browser('https://idp.example.com/authorize'))

        opened.assert_called_once_with('https://idp.example.com/authorize')

    def test_no_browser_is_not_an_exception(self):
        # webbrowser answers False where it has nothing to open, and
        # raises where it found something it could not start. Neither
        # may end the login: the URL is printed instead.
        with mock.patch('shakenfist_client.commandline.auth.webbrowser.open',
                        return_value=False):
            self.assertFalse(auth._open_browser('https://idp.example.com/'))

        with mock.patch('shakenfist_client.commandline.auth.webbrowser.open',
                        side_effect=auth.webbrowser.Error('no browser')):
            self.assertFalse(auth._open_browser('https://idp.example.com/'))

        with mock.patch('shakenfist_client.commandline.auth.webbrowser.open',
                        side_effect=OSError('xdg-open died')):
            self.assertFalse(auth._open_browser('https://idp.example.com/'))

    def test_a_host_with_no_browser_registered_says_so(self):
        with mock.patch('shakenfist_client.commandline.auth.webbrowser.get',
                        side_effect=auth.webbrowser.Error('nothing here')):
            self.assertFalse(auth._browser_is_available())

        with mock.patch('shakenfist_client.commandline.auth.webbrowser.get',
                        return_value=object()):
            self.assertTrue(auth._browser_is_available())


class ChooseFlowTestCase(testtools.TestCase):
    """Which flow runs, given what is advertised and what is here."""

    def setUp(self):
        super().setUp()
        self.teller = Teller()
        self.browser = True
        self.asked_about_the_browser = 0

    def _browser_available(self):
        self.asked_about_the_browser += 1
        return self.browser

    def _choose(self, issuers, requested=None):
        return auth._choose_flow(issuers, requested=requested, tell=self.teller,
                                 browser_available=self._browser_available)

    def test_an_empty_list_is_a_complete_answer(self):
        e = self.assertRaises(auth.LoginFailed, self._choose, [])

        self.assertIn('no human login', str(e))

    def test_the_device_flow_is_preferred(self):
        # It needs no browser, no listener and no redirect registered at
        # the provider, which is why it wins wherever it is offered.
        self.assertEqual(auth.FLOW_DEVICE, self._choose([issuer()]))
        self.assertEqual(0, self.asked_about_the_browser)

    def test_the_browser_flow_is_the_fallback(self):
        chosen = self._choose([issuer(device_authorization_endpoint=None)])

        self.assertEqual(auth.FLOW_CODE, chosen)
        # Said out loud, because a browser opening by itself is
        # surprising on a verb which usually prints a code.
        self.assertIn('will open one', self.teller.text)

    def test_the_browser_flow_is_not_chosen_without_a_browser(self):
        # The redirect comes back to a listener on this host, so a URL
        # opened on another machine cannot complete the login.
        self.browser = False

        e = self.assertRaises(
            auth.LoginFailed, self._choose,
            [issuer(name='legacy', device_authorization_endpoint=None)])

        self.assertIn('no browser here', str(e))
        self.assertIn('legacy', str(e))
        self.assertIn('--flow=code', str(e))

    def test_a_requested_flow_is_run_without_a_browser(self):
        # The person at the terminal knows things this cannot: that a
        # port is forwarded, or which browser holds their identity.
        self.browser = False

        self.assertEqual(auth.FLOW_CODE,
                         self._choose([issuer()], requested=auth.FLOW_CODE))
        self.assertEqual(0, self.asked_about_the_browser)

    def test_a_requested_flow_overrides_the_preference(self):
        self.assertEqual(auth.FLOW_DEVICE,
                         self._choose([issuer()], requested=auth.FLOW_DEVICE))
        self.assertEqual(auth.FLOW_CODE,
                         self._choose([issuer()], requested=auth.FLOW_CODE))

    def test_one_provider_for_each_flow_still_prefers_the_device_one(self):
        chosen = self._choose([issuer(name='corporate',
                                      authorization_endpoint=None),
                               issuer(name='partner',
                                      device_authorization_endpoint=None)])

        self.assertEqual(auth.FLOW_DEVICE, chosen)

    def test_a_cluster_which_can_run_neither_flow_names_both(self):
        # No token endpoint, so neither flow can finish -- and it would
        # fail after the person had authenticated rather than before.
        e = self.assertRaises(auth.LoginFailed, self._choose,
                              [issuer(name='legacy', token_endpoint=None)])

        self.assertIn('legacy', str(e))
        self.assertIn('device authorization endpoint', str(e))
        self.assertIn('an authorization endpoint and a token endpoint', str(e))


class ChooseIssuerTestCase(testtools.TestCase):
    def setUp(self):
        super().setUp()
        self.teller = Teller()
        self.prompted = []

    def _prompt(self, issuers, tell):
        self.prompted.append(issuers)
        return issuers[0]

    def _choose(self, issuers, flow=auth.FLOW_DEVICE):
        return auth._choose_issuer(issuers, tell=self.teller,
                                   prompt=self._prompt, flow=flow)

    def test_an_empty_list_is_a_complete_answer(self):
        # The cluster documents an empty list as "this cluster offers no
        # human login", so there is nothing to attempt.
        e = self.assertRaises(auth.LoginFailed, self._choose, [])

        self.assertIn('no human login', str(e))
        self.assertEqual([], self.prompted)

    def test_one_issuer_is_chosen_without_asking(self):
        self.assertEqual(issuer(), self._choose([issuer()]))
        self.assertEqual([], self.prompted)

    def test_several_issuers_are_prompted_between(self):
        issuers = [issuer(name='corporate'), issuer(name='partner')]

        chosen = self._choose(issuers)

        self.assertEqual(1, len(self.prompted))
        self.assertEqual(['corporate', 'partner'],
                         [i['name'] for i in self.prompted[0]])
        self.assertEqual('corporate', chosen['name'])

    def test_an_issuer_without_the_endpoint_is_not_offered(self):
        # The cluster sends an endpoint only when it has one, so its
        # absence is the provider saying this flow cannot begin there.
        issuers = [issuer(name='corporate'),
                   issuer(name='legacy', device_authorization_endpoint=None)]

        chosen = self._choose(issuers)

        self.assertEqual('corporate', chosen['name'])
        self.assertEqual([], self.prompted)

    def test_no_issuer_can_do_the_flow_says_so(self):
        e = self.assertRaises(
            auth.LoginFailed, self._choose,
            [issuer(name='legacy', device_authorization_endpoint=None,
                    authorization_endpoint=None)])

        self.assertIn('device authorization endpoint', str(e))
        self.assertIn('legacy', str(e))

    def test_the_flow_decides_which_providers_are_offered(self):
        issuers = [issuer(name='corporate', authorization_endpoint=None),
                   issuer(name='partner', device_authorization_endpoint=None)]

        self.assertEqual('corporate',
                         self._choose(issuers, flow=auth.FLOW_DEVICE)['name'])
        self.assertEqual('partner',
                         self._choose(issuers, flow=auth.FLOW_CODE)['name'])
        self.assertEqual([], self.prompted)

    def test_a_provider_with_no_token_endpoint_can_do_neither_flow(self):
        # Both flows finish at the token endpoint, so a provider without
        # one would fail after the person had authenticated.
        for flow in (auth.FLOW_DEVICE, auth.FLOW_CODE):
            e = self.assertRaises(auth.LoginFailed, self._choose,
                                  [issuer(name='legacy', token_endpoint=None)],
                                  flow)

            self.assertIn('token endpoint', str(e))

    def test_a_flow_no_provider_offers_names_the_one_that_would_work(self):
        # The person cannot read the issuer metadata, so this is the
        # difference between "this cluster cannot do this" and "ask for
        # the other one".
        e = self.assertRaises(
            auth.LoginFailed, self._choose,
            [issuer(name='corporate', authorization_endpoint=None)],
            auth.FLOW_CODE)

        self.assertIn('an authorization endpoint', str(e))
        self.assertIn('corporate advertises what a device authorization login '
                      'needs', str(e))
        self.assertIn('"--flow=device"', str(e))


class RefusalTestCase(testtools.TestCase):
    """The refusals each login route can answer with.

    Two of them do not arrive as an exception class of their own: the
    status code map in apiclient has no entry for 413 or 429, so they
    arrive as a bare APIException and have to be told apart by status
    code.
    """

    def _api_exception(self, status_code, text='{}'):
        return apiclient.APIException(
            'API request failed', 'POST', CLUSTER + '/auth/login',
            status_code, text)

    def _mint_refusal(self, exception):
        client = mock.MagicMock()
        client.login.side_effect = exception
        return self.assertRaises(auth.LoginFailed, auth._mint_credentials,
                                 client, IDENTITY_TOKEN)

    def _issuers_refusal(self, exception):
        client = mock.MagicMock()
        client.get_login_issuers.side_effect = exception
        return self.assertRaises(auth.LoginFailed, auth._login_issuers,
                                 client)

    def test_the_mint_refusing_the_identity(self):
        e = self._mint_refusal(apiclient.UnauthenticatedException(
            'API request failed', 'POST', CLUSTER, 401,
            '{"error": "login refused"}'))

        self.assertIn('refused this identity', str(e))
        self.assertIn('mapping rule', str(e))

    def test_the_mint_rate_limiting(self):
        e = self._mint_refusal(self._api_exception(429))

        self.assertIn('rate limiting', str(e))

    def test_the_mint_refusing_an_oversized_token(self):
        e = self._mint_refusal(self._api_exception(413))

        self.assertIn('larger than the cluster will accept', str(e))

    def test_too_many_mapping_rules_does_not_read_as_capacity(self):
        # 507 from a login is permanent: the identity satisfies more
        # mapping rules than one login may mint for, which is an
        # operator's configuration and not a busy cluster. It must not
        # read as "try again later", and the client must not be built to
        # retry it.
        e = self._mint_refusal(apiclient.InsufficientResourcesException(
            'API request failed', 'POST', CLUSTER, 507, '{}'))

        self.assertIn('mapping rules', str(e))
        self.assertIn('trying again will not help', str(e).lower())
        self.assertNotIn('retry', str(e).lower())
        self.assertNotIn('capacity', str(e).lower())

    def test_the_mint_being_unavailable_reads_as_temporary(self):
        e = self._mint_refusal(apiclient.ServiceUnavailableException(
            'API request failed', 'POST', CLUSTER, 503, '{}'))

        self.assertIn('temporary', str(e))

    def test_a_cluster_which_cannot_mint(self):
        e = self._mint_refusal(apiclient.IncapableException(
            'Cluster does not support logging in with an identity token'))

        self.assertIn('does not support logging in', str(e))

    def test_an_unknown_status_from_the_mint_is_not_swallowed(self):
        # There is no fallback for a status nobody has taught this verb
        # about: the CLI's own handler reports it, or it is loud.
        client = mock.MagicMock()
        client.login.side_effect = self._api_exception(418, 'teapot')

        self.assertRaises(apiclient.APIException, auth._mint_credentials,
                          client, IDENTITY_TOKEN)

    def test_no_refusal_renders_the_identity_token(self):
        for exception in (
                apiclient.UnauthenticatedException(
                    'API request failed', 'POST', CLUSTER, 401, '{}'),
                apiclient.InsufficientResourcesException(
                    'API request failed', 'POST', CLUSTER, 507, '{}'),
                self._api_exception(413),
                self._api_exception(429)):
            self.assertNotIn(IDENTITY_TOKEN, str(self._mint_refusal(exception)))

    def test_the_issuer_list_rate_limiting(self):
        e = self._issuers_refusal(self._api_exception(429))

        self.assertIn('rate limiting', str(e))

    def test_the_issuer_list_being_unavailable_reads_as_temporary(self):
        e = self._issuers_refusal(apiclient.ServiceUnavailableException(
            'API request failed', 'GET', CLUSTER, 503, '{}'))

        self.assertIn('temporary', str(e))

    def test_a_cluster_with_no_human_login_at_all(self):
        e = self._issuers_refusal(apiclient.IncapableException(
            'Cluster does not support listing human login issuers'))

        self.assertIn('no human login to run here', str(e))

    def test_a_cluster_which_lists_issuers_but_cannot_mint(self):
        # Asked before the flow starts, because the alternative is
        # sending somebody to their identity provider, waiting while they
        # authenticate, and refusing them afterwards.
        client = mock.MagicMock()
        client.check_capability.return_value = False

        e = self.assertRaises(auth.LoginFailed,
                              auth._check_the_cluster_can_mint, client)

        self.assertIn('cannot exchange an identity token', str(e))
        client.check_capability.assert_called_once_with('human-login')


class CacheIsolatedTestCase(testtools.TestCase):
    """Base class which keeps the tests off the real credential cache.

    The cache module is asked where its file is rather than being told,
    so pointing that one function at a temporary directory is the only
    seam a test needs: a login by whoever is running the tests cannot
    then change what these assert, and these cannot touch theirs.
    """

    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir, True)
        self.path = os.path.join(self.tempdir, 'shakenfist',
                                 'credentials.json')
        mock.patch('shakenfist_client.credentials.credentials_path',
                   side_effect=lambda path=None: path or self.path).start()
        self.addCleanup(mock.patch.stopall)


class LoginCommandTestCase(CacheIsolatedTestCase):
    def setUp(self):
        super().setUp()
        self.client = mock.MagicMock()
        self.client.base_url = CLUSTER
        self.client.get_login_issuers.return_value = [issuer()]
        self.client.check_capability.return_value = True
        self.client.login.return_value = [minted()]
        mock.patch('shakenfist_client.commandline.auth._client_for_login',
                   return_value=self.client).start()
        self.flow = mock.patch(
            'shakenfist_client.commandline.auth.device_authorization_login',
            return_value=IDENTITY_TOKEN).start()
        self.code_flow = mock.patch(
            'shakenfist_client.commandline.auth.authorization_code_login',
            return_value=IDENTITY_TOKEN).start()
        # Nothing in these tests may consult the host for a browser: the
        # answer would differ between a workstation and a CI container.
        self.browser = mock.patch(
            'shakenfist_client.commandline.auth._browser_is_available',
            return_value=True).start()

    def _invoke(self, output='pretty', namespace=None, apiurl=CLUSTER, args=None):
        return CliRunner().invoke(
            auth.login, args or [], obj={'OUTPUT': output, 'APIURL': apiurl,
                                         'NAMESPACE': namespace},
            catch_exceptions=False)

    def test_a_login_caches_what_it_minted(self):
        result = self._invoke()

        self.assertEqual(0, result.exit_code)
        self.client.login.assert_called_once_with(IDENTITY_TOKEN)
        cached = credentials.read_credentials(path=self.path)
        self.assertEqual(1, len(cached['credentials']))
        self.assertEqual('ci', cached['credentials'][0]['namespace'])
        self.assertEqual(CLUSTER, cached['credentials'][0]['apiurl'])
        # The single member of the set is selected without being asked
        # about, because there is nothing to choose between.
        self.assertEqual('ci', cached['current']['namespace'])

    def test_the_cache_file_is_owner_only(self):
        self._invoke()

        self.assertEqual(0o600, os.stat(self.path).st_mode & 0o777)
        self.assertEqual(0o700,
                         os.stat(os.path.dirname(self.path)).st_mode & 0o777)

    def test_the_named_namespace_is_the_one_selected(self):
        self.client.login.return_value = [
            minted(namespace='ci'),
            minted(namespace='dev', key_name='corporate-bbbb',
                   key=OTHER_MINTED_KEY)]

        result = self._invoke(namespace='dev')

        self.assertEqual(0, result.exit_code)
        cached = credentials.read_credentials(path=self.path)
        self.assertEqual('dev', cached['current']['namespace'])

    def test_several_credentials_and_no_choice_selects_nothing(self):
        self.client.login.return_value = [
            minted(namespace='ci'),
            minted(namespace='dev', key_name='corporate-bbbb',
                   key=OTHER_MINTED_KEY)]

        result = self._invoke()

        self.assertEqual(0, result.exit_code)
        cached = credentials.read_credentials(path=self.path)
        self.assertIsNone(cached['current'])
        self.assertIn('none of them is selected', result.output)

    def test_a_selection_on_another_cluster_is_called_out(self):
        # A selection against a different cluster survives a login here, so
        # this login leaves nothing of its own selected while the cache
        # still has a selection. Saying nothing would be the worst of the
        # three outcomes: the next command resolves that other selection
        # and talks to the other cluster, and the login which just
        # succeeded looks as though it did nothing.
        credentials.store_credential_set(
            OTHER_CLUSTER,
            [minted(namespace='elsewhere', key_name='corporate-cccc',
                    key=OTHER_MINTED_KEY)],
            path=self.path)
        self.client.login.return_value = [
            minted(namespace='ci'),
            minted(namespace='dev', key_name='corporate-bbbb',
                   key=OTHER_MINTED_KEY)]

        result = self._invoke()

        self.assertEqual(0, result.exit_code)
        cached = credentials.read_credentials(path=self.path)
        self.assertEqual(OTHER_CLUSTER, cached['current']['apiurl'])
        self.assertIn('none of them is selected', result.output)
        self.assertIn('elsewhere', result.output)
        self.assertIn(OTHER_CLUSTER, result.output)

    def test_an_empty_credential_set_does_not_crash(self):
        # Not something the cluster answers -- a login which matches no
        # mapping rule is refused rather than minting nothing -- but a
        # verb which rendered an advice line naming the namespaces would
        # have had no namespace to name.
        self.client.login.return_value = []

        result = self._invoke()

        self.assertEqual(0, result.exit_code)

    def test_the_minted_keys_are_never_printed(self):
        self.client.login.return_value = [
            minted(namespace='ci', expiry=1763000000.0),
            minted(namespace='dev', key_name='corporate-bbbb',
                   key=OTHER_MINTED_KEY)]

        for output in ('pretty', 'simple', 'json'):
            result = self._invoke(output=output)

            self.assertEqual(0, result.exit_code)
            self.assertNotIn(MINTED_KEY, result.output)
            self.assertNotIn(OTHER_MINTED_KEY, result.output)
            self.assertNotIn(IDENTITY_TOKEN, result.output)
            # The negative control: something from the credential set
            # did reach the terminal, so the assertions above are not
            # passing on an empty string.
            self.assertIn('corporate-aaaa', result.output)

    def test_the_json_output_carries_no_key_field_at_all(self):
        result = self._invoke(output='json')

        rows = json.loads(result.stdout)
        self.assertEqual(
            ['apiurl', 'current', 'expiry', 'key_name', 'namespace', 'scopes'],
            sorted(rows[0].keys()))

    def test_the_conversation_does_not_go_to_standard_output(self):
        # --json exists so that something else can parse standard output,
        # and a login talks to a human in the middle of the command.
        result = self._invoke(output='json')

        self.assertIn('Logging in to', result.stderr)
        self.assertNotIn('Logging in to', result.stdout)

    def test_the_simple_output_has_a_header_row(self):
        result = self._invoke(output='simple')

        lines = result.stdout.strip().split('\n')
        self.assertEqual('namespace,key_name,expiry,scopes,current', lines[0])
        self.assertEqual('ci,corporate-aaaa,,blob.read,yes', lines[1])

    def test_an_empty_issuer_list_exits_without_a_flow(self):
        self.client.get_login_issuers.return_value = []

        result = self._invoke()

        self.assertEqual(1, result.exit_code)
        self.assertIn('no human login', result.output)
        self.flow.assert_not_called()
        self.client.login.assert_not_called()
        self.assertFalse(os.path.exists(self.path))

    def test_a_refusal_from_the_mint_exits_non_zero(self):
        self.client.login.side_effect = apiclient.InsufficientResourcesException(
            'API request failed', 'POST', CLUSTER, 507, '{}')

        result = self._invoke()

        self.assertEqual(1, result.exit_code)
        self.assertIn('mapping rules', result.output)
        self.assertFalse(os.path.exists(self.path))

    def test_the_device_flow_runs_where_a_provider_offers_it(self):
        result = self._invoke()

        self.assertEqual(0, result.exit_code)
        self.flow.assert_called_once_with(self.client.get_login_issuers()[0])
        self.code_flow.assert_not_called()

    def test_the_flow_override_runs_the_browser_flow_instead(self):
        result = self._invoke(args=['--flow=code'])

        self.assertEqual(0, result.exit_code)
        self.code_flow.assert_called_once_with(
            self.client.get_login_issuers()[0])
        self.flow.assert_not_called()
        self.client.login.assert_called_once_with(IDENTITY_TOKEN)
        cached = credentials.read_credentials(path=self.path)
        self.assertEqual('ci', cached['credentials'][0]['namespace'])

    def test_the_browser_flow_runs_where_the_device_one_is_not_offered(self):
        self.client.get_login_issuers.return_value = [
            issuer(device_authorization_endpoint=None)]

        result = self._invoke()

        self.assertEqual(0, result.exit_code)
        self.code_flow.assert_called_once()
        self.flow.assert_not_called()

    def test_a_flow_the_cluster_cannot_run_exits_with_the_alternative(self):
        self.client.get_login_issuers.return_value = [
            issuer(authorization_endpoint=None)]

        result = self._invoke(args=['--flow=code'])

        self.assertEqual(1, result.exit_code)
        self.assertIn('--flow=device', result.output)
        self.code_flow.assert_not_called()
        self.flow.assert_not_called()
        self.assertFalse(os.path.exists(self.path))

    def test_an_unknown_flow_is_refused_by_the_command_line(self):
        result = CliRunner().invoke(
            auth.login, ['--flow=carrier-pigeon'],
            obj={'OUTPUT': 'pretty', 'APIURL': CLUSTER, 'NAMESPACE': None})

        self.assertEqual(2, result.exit_code)
        self.flow.assert_not_called()
        self.code_flow.assert_not_called()

    def test_the_help_names_both_flows(self):
        result = self._invoke(args=['--help'])

        self.assertEqual(0, result.exit_code)
        self.assertIn('--flow', result.output)
        self.assertIn('device', result.output)
        self.assertIn('code', result.output)

    def test_a_cache_which_cannot_be_written_says_so(self):
        # The secrets are in hand and will never be issued again, so the
        # person has to be told they were not kept and that another login
        # mints another set.
        os.makedirs(os.path.dirname(self.path))
        os.chmod(os.path.dirname(self.path), 0o777)

        result = self._invoke()

        self.assertEqual(1, result.exit_code)
        self.assertIn('could not be saved', result.output)
        self.assertIn('run the login again', result.output)
        self.assertNotIn(MINTED_KEY, result.output)


class ClientForLoginTestCase(testtools.TestCase):
    """What a login needs before it can run, and what it says without it.

    The group callback cannot build a client for a login -- somebody
    logging in for the first time has no credential for it to resolve --
    so the verb builds its own, and these are the three answers it can
    get back.
    """

    def setUp(self):
        super().setUp()
        self.construct = mock.patch(
            'shakenfist_client.apiclient.Client').start()
        self.addCleanup(mock.patch.stopall)

    def test_a_named_cluster_skips_the_credential_lookup_entirely(self):
        # It is the only part of constructing a client which can fail for
        # somebody who has not logged in yet, and a login has no use for
        # its result: both of the routes it calls are public.
        auth._client_for_login(CLUSTER)

        self.construct.assert_called_once_with(
            base_url=CLUSTER, verbose=False,
            suppress_configuration_lookup=True)

    def test_no_cluster_anywhere_says_how_to_name_one(self):
        self.construct.side_effect = apiclient.UnconfiguredException(
            'You have not specified the server to communicate with')

        e = self.assertRaises(auth.LoginFailed, auth._client_for_login, None)

        self.assertIn('--apiurl', str(e))
        self.assertIn('SHAKENFIST_API_URL', str(e))

    def test_an_unreadable_cache_offers_the_way_around_it(self):
        self.construct.side_effect = credentials.CredentialCacheException(
            'Refusing to read the credential cache /tmp/nope.json: its mode '
            'is 0644.')

        e = self.assertRaises(auth.LoginFailed, auth._client_for_login, None)

        self.assertIn('0644', str(e))
        self.assertIn('--apiurl', str(e))


class LogoutCommandTestCase(CacheIsolatedTestCase):
    def _invoke(self, output='pretty'):
        return CliRunner().invoke(auth.logout, [], obj={'OUTPUT': output},
                                  catch_exceptions=False)

    def _write_cache(self, count=1, mode=0o600, content=None):
        os.makedirs(os.path.dirname(self.path), mode=0o700, exist_ok=True)
        if content is None:
            content = json.dumps({
                'version': 1,
                'credentials': [
                    minted(key_name='corporate-%d' % n, key=MINTED_KEY)
                    for n in range(count)],
                'current': None})
            document = json.loads(content)
            for credential in document['credentials']:
                credential['apiurl'] = CLUSTER
            content = json.dumps(document)
        with open(self.path, 'w') as f:
            f.write(content)
        os.chmod(self.path, mode)

    def test_the_cache_file_is_removed(self):
        self._write_cache(count=2)

        result = self._invoke()

        self.assertEqual(0, result.exit_code)
        self.assertFalse(os.path.exists(self.path))
        self.assertIn('Discarded 2 cached credentials', result.stdout)

    def test_no_cache_is_not_a_failure(self):
        result = self._invoke()

        self.assertEqual(0, result.exit_code)
        self.assertIn('no cached credentials', result.stdout)

    def test_a_cache_which_cannot_be_read_is_still_removed(self):
        # The whole point of this verb. A cache whose mode exposed a key,
        # or which is corrupt, makes every other verb refuse -- including
        # the one which would fix it, if it needed a client.
        self._write_cache(mode=0o644)

        result = self._invoke()

        self.assertEqual(0, result.exit_code)
        self.assertFalse(os.path.exists(self.path))
        self.assertIn('could not be read', result.stdout)

    def test_a_corrupt_cache_is_still_removed(self):
        self._write_cache(content='{this is not json')

        result = self._invoke()

        self.assertEqual(0, result.exit_code)
        self.assertFalse(os.path.exists(self.path))

    def test_no_key_is_printed_in_any_output_mode(self):
        for output in ('pretty', 'simple', 'json'):
            self._write_cache()

            result = self._invoke(output=output)

            self.assertEqual(0, result.exit_code)
            self.assertNotIn(MINTED_KEY, result.output)

    def test_the_json_output_names_the_file_and_the_count(self):
        self._write_cache(count=3)

        result = self._invoke(output='json')

        self.assertEqual({'path': self.path, 'discarded': 3},
                         json.loads(result.stdout))

    def test_the_simple_output_has_a_header_row(self):
        self._write_cache(count=3)

        result = self._invoke(output='simple')

        self.assertEqual(['discarded', '3'], result.stdout.strip().split('\n'))


class CredentialCommandTestCase(CacheIsolatedTestCase):
    """The verbs which show what is held, and choose among it."""

    def setUp(self):
        super().setUp()
        self.live = {}
        self.attempts = []
        self.clients = mock.patch(
            'shakenfist_client.commandline.auth._client_for_credential',
            side_effect=self._client_for).start()

    def _client_for(self, held):
        self.attempts.append((held['apiurl'], held['namespace'], held['key_name']))
        client = mock.MagicMock()

        def keys(namespace):
            answer = self.live[(held['apiurl'], held['namespace'], held['key_name'])]
            if isinstance(answer, Exception):
                raise answer
            return answer

        client.get_namespace_keys.side_effect = keys
        return client

    def _write_cache(self, credentials_, current=None, mode=0o600):
        os.makedirs(os.path.dirname(self.path), mode=0o700, exist_ok=True)
        with open(self.path, 'w') as f:
            json.dump({'version': 1, 'credentials': credentials_, 'current': current}, f)
        os.chmod(self.path, mode)

    def _held(self, namespace, key_name, apiurl=CLUSTER, key=MINTED_KEY, expiry=None, scopes=None):
        entry = minted(namespace=namespace, key_name=key_name, key=key,
                       expiry=expiry or time.time() + 3600, scopes=scopes)
        entry['apiurl'] = apiurl
        return entry

    def _list(self, output='pretty', args=None, apiurl=None):
        return CliRunner().invoke(
            auth.credential, ['list'] + (args or []),
            obj={'OUTPUT': output, 'APIURL': apiurl}, catch_exceptions=False)

    def _select(self, args, apiurl=None):
        return CliRunner().invoke(
            auth.credential, ['select'] + args,
            obj={'OUTPUT': 'pretty', 'APIURL': apiurl}, catch_exceptions=False)

    def _two_namespaces(self):
        a = self._held('ci', 'one', key=MINTED_KEY)
        b = self._held('dev', 'two', key=OTHER_MINTED_KEY)
        self._write_cache([a, b], current={'apiurl': CLUSTER, 'namespace': 'ci', 'key_name': 'one'})
        return a, b

    def test_the_current_credential_is_marked(self):
        self._two_namespaces()

        rows = json.loads(self._list(output='json', args=['--offline']).stdout)

        self.assertEqual({'one': True, 'two': False}, {r['key_name']: r['current'] for r in rows})

    def test_live_metadata_replaces_the_caches_copy_and_says_so(self):
        a, b = self._two_namespaces()
        self.live[(CLUSTER, 'ci', 'one')] = [{'key_name': 'one', 'expiry': 5000.0, 'scopes': ['live.scope']}]
        self.live[(CLUSTER, 'dev', 'two')] = [{'key_name': 'two', 'expiry': 6000.0, 'scopes': []}]

        rows = json.loads(self._list(output='json').stdout)

        self.assertEqual(['live', 'live'], [r['source'] for r in rows])
        self.assertEqual(5000.0, rows[0]['expiry'])
        self.assertEqual(['live.scope'], rows[0]['scopes'])

    def test_one_request_per_namespace_not_per_key(self):
        a = self._held('ci', 'one')
        b = self._held('ci', 'two', key=OTHER_MINTED_KEY)
        self._write_cache([a, b])
        listing = [{'key_name': 'one', 'expiry': 1.0, 'scopes': []},
                   {'key_name': 'two', 'expiry': 2.0, 'scopes': []}]
        self.live[(CLUSTER, 'ci', 'one')] = listing

        self._list(output='json')

        self.assertEqual([(CLUSTER, 'ci', 'one')], self.attempts)

    def test_a_namespace_which_cannot_be_read_does_not_stop_the_others(self):
        self._two_namespaces()
        self.live[(CLUSTER, 'ci', 'one')] = apiclient.UnauthenticatedException('no', 'POST', 'u', 401, '')
        self.live[(CLUSTER, 'dev', 'two')] = [{'key_name': 'two', 'expiry': 6000.0, 'scopes': ['x']}]

        result = self._list(output='json')
        rows = json.loads(result.stdout)

        self.assertEqual(['cache', 'live'], [r['source'] for r in rows])
        self.assertEqual(auth.REASON_REFUSED, rows[0]['reason'])
        self.assertIn('1 of 2', result.stderr)

    def test_the_next_key_for_a_namespace_is_tried_when_one_is_refused(self):
        a = self._held('ci', 'one')
        b = self._held('ci', 'two', key=OTHER_MINTED_KEY)
        self._write_cache([a, b])
        self.live[(CLUSTER, 'ci', 'one')] = apiclient.UnauthenticatedException('no', 'POST', 'u', 401, '')
        self.live[(CLUSTER, 'ci', 'two')] = [{'key_name': 'two', 'expiry': 2.0, 'scopes': []}]

        rows = json.loads(self._list(output='json').stdout)

        self.assertEqual(['cache', 'live'], [r['source'] for r in rows])
        self.assertEqual(auth.REASON_NOT_LISTED, rows[0]['reason'])

    def test_an_unreachable_cluster_is_asked_once_and_the_cache_answers(self):
        self._two_namespaces()
        self.live[(CLUSTER, 'ci', 'one')] = requests.exceptions.ConnectionError('down')

        result = self._list(output='json')
        rows = json.loads(result.stdout)

        self.assertEqual(0, result.exit_code)
        self.assertEqual([(CLUSTER, 'ci', 'one')], self.attempts)
        self.assertEqual({'cache'}, {r['source'] for r in rows})
        self.assertEqual({auth.REASON_UNREACHABLE}, {r['reason'] for r in rows})

    def test_a_cluster_without_the_capability_is_reported_from_the_cache(self):
        self._two_namespaces()
        self.live[(CLUSTER, 'ci', 'one')] = apiclient.IncapableException('no')

        rows = json.loads(self._list(output='json').stdout)

        self.assertEqual({auth.REASON_INCAPABLE}, {r['reason'] for r in rows})

    def test_an_unexpected_failure_is_not_a_traceback(self):
        self._two_namespaces()
        self.live[(CLUSTER, 'ci', 'one')] = KeyError(MINTED_KEY)
        self.live[(CLUSTER, 'dev', 'two')] = ValueError(OTHER_MINTED_KEY)

        for output in ('pretty', 'simple', 'json'):
            result = self._list(output=output)

            self.assertEqual(0, result.exit_code)
            self.assertNotIn(MINTED_KEY, result.output)
            self.assertNotIn(OTHER_MINTED_KEY, result.output)

    def test_a_key_the_cluster_no_longer_lists_is_said_to_be_missing(self):
        self._write_cache([self._held('ci', 'one')])
        self.live[(CLUSTER, 'ci', 'one')] = [{'key_name': 'other', 'expiry': 1.0, 'scopes': []}]

        rows = json.loads(self._list(output='json').stdout)

        self.assertEqual('cache', rows[0]['source'])
        self.assertEqual(auth.REASON_NOT_LISTED, rows[0]['reason'])

    def test_offline_asks_no_cluster_at_all(self):
        self._two_namespaces()

        result = self._list(output='json', args=['--offline'])

        self.assertEqual([], self.attempts)
        self.assertEqual({auth.REASON_NOT_ASKED}, {r['reason'] for r in json.loads(result.stdout)})

    def test_no_key_is_printed_in_any_output_mode(self):
        self._two_namespaces()
        self.live[(CLUSTER, 'ci', 'one')] = [{'key_name': 'one', 'key': MINTED_KEY, 'expiry': 1.0,
                                              'scopes': ['s']}]
        self.live[(CLUSTER, 'dev', 'two')] = requests.exceptions.ConnectionError(OTHER_MINTED_KEY)

        for output in ('pretty', 'simple', 'json'):
            for args in ([], ['--offline']):
                result = self._list(output=output, args=args)

                self.assertEqual(0, result.exit_code)
                self.assertNotIn(MINTED_KEY, result.output)
                self.assertNotIn(OTHER_MINTED_KEY, result.output)

    def test_a_field_the_cluster_adds_is_not_printed(self):
        self._write_cache([self._held('ci', 'one')])
        self.live[(CLUSTER, 'ci', 'one')] = [{'key_name': 'one', 'expiry': 1.0, 'scopes': [],
                                              'new_secret': 'not-a-field-we-name'}]

        result = self._list(output='json')

        self.assertNotIn('not-a-field-we-name', result.output)

    def test_the_simple_output_has_a_header_and_no_serialised_objects(self):
        self._two_namespaces()

        lines = self._list(output='simple', args=['--offline']).stdout.strip().split('\n')

        self.assertEqual('apiurl,namespace,key_name,expiry,scopes,current,source,reason', lines[0])
        self.assertEqual(3, len(lines))
        for line in lines:
            self.assertEqual(8, len(line.split(',')))
            self.assertNotIn('{', line)
            self.assertNotIn('[', line)

    def test_the_conversation_goes_to_standard_error(self):
        self._two_namespaces()

        result = self._list(output='json', args=['--offline'])

        json.loads(result.stdout)
        self.assertIn('no cluster was asked', result.stderr)

    def test_an_empty_cache_is_not_a_failure(self):
        for output in ('pretty', 'simple', 'json'):
            result = self._list(output=output)

            self.assertEqual(0, result.exit_code)

        self.assertEqual([], json.loads(self._list(output='json').stdout))

    def test_a_cache_which_cannot_be_read_says_so(self):
        self._write_cache([self._held('ci', 'one')], mode=0o644)

        result = self._list()

        self.assertEqual(1, result.exit_code)
        self.assertIn(self.path, result.stderr)
        self.assertNotIn(MINTED_KEY, result.output)

    def test_an_expired_credential_is_not_listed(self):
        self._write_cache([self._held('ci', 'one', expiry=time.time() - 10)])

        self.assertEqual([], json.loads(self._list(output='json', args=['--offline']).stdout))

    def test_selecting_changes_the_current_credential(self):
        self._two_namespaces()

        result = self._select(['dev'])

        self.assertEqual(0, result.exit_code)
        self.assertEqual('two', credentials.current_credential()['key_name'])
        self.assertEqual([], self.attempts)
        self.assertNotIn(OTHER_MINTED_KEY, result.output)

    def test_selecting_what_is_not_held_is_refused_with_the_caches_words(self):
        self._two_namespaces()

        result = self._select(['nowhere'])

        self.assertEqual(1, result.exit_code)
        self.assertIn('There is no cached credential for namespace nowhere', result.stderr)
        self.assertEqual('one', credentials.current_credential()['key_name'])

    def test_an_ambiguous_selection_is_refused_not_guessed(self):
        self._write_cache([self._held('ci', 'one'), self._held('ci', 'two', key=OTHER_MINTED_KEY)])

        result = self._select(['ci'])

        self.assertEqual(1, result.exit_code)
        self.assertIn('Name one of one, two', result.stderr)
        self.assertIsNone(credentials.current_credential())

        result = self._select(['ci', '--key-name', 'two'])

        self.assertEqual(0, result.exit_code)
        self.assertEqual('two', credentials.current_credential()['key_name'])

    def test_several_clusters_and_no_selection_asks_which(self):
        self._write_cache([self._held('ci', 'one'), self._held('ci', 'two', apiurl=OTHER_CLUSTER)])

        result = self._select(['ci'])

        self.assertEqual(1, result.exit_code)
        self.assertIn(CLUSTER, result.stderr)
        self.assertIn(OTHER_CLUSTER, result.stderr)

        result = self._select(['ci'], apiurl=OTHER_CLUSTER)

        self.assertEqual(0, result.exit_code)
        self.assertEqual('two', credentials.current_credential()['key_name'])

    def test_the_cluster_of_the_current_selection_is_the_default(self):
        self._write_cache([self._held('ci', 'one'), self._held('ci', 'two', apiurl=OTHER_CLUSTER),
                           self._held('dev', 'three', apiurl=OTHER_CLUSTER)],
                          current={'apiurl': OTHER_CLUSTER, 'namespace': 'dev', 'key_name': 'three'})

        result = self._select(['ci'])

        self.assertEqual(0, result.exit_code)
        self.assertEqual('two', credentials.current_credential()['key_name'])

    def test_selecting_with_nothing_cached_says_so(self):
        result = self._select(['ci'])

        self.assertEqual(1, result.exit_code)
        self.assertIn('no cached credentials', result.stderr)

    def test_the_group_is_kept_out_of_client_construction(self):
        self.assertIn(auth.credential.name, auth.COMMANDS_WITHOUT_A_CLIENT)
