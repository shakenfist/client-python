import json
import os
import shutil
import tempfile
from unittest import mock

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
MINTED_KEY = 'not-a-key-1'
OTHER_MINTED_KEY = 'not-a-key-2'

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


class ChooseIssuerTestCase(testtools.TestCase):
    def setUp(self):
        super().setUp()
        self.teller = Teller()
        self.prompted = []

    def _prompt(self, issuers, tell):
        self.prompted.append(issuers)
        return issuers[0]

    def _choose(self, issuers):
        return auth._choose_issuer(issuers, tell=self.teller,
                                   prompt=self._prompt)

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
            [issuer(name='legacy', device_authorization_endpoint=None)])

        self.assertIn('device authorization endpoint', str(e))
        self.assertIn('legacy', str(e))


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

    def _invoke(self, output='pretty', namespace=None, apiurl=CLUSTER):
        return CliRunner().invoke(
            auth.login, [], obj={'OUTPUT': output, 'APIURL': apiurl,
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
