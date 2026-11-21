import copy
import io
import json
import os
import shutil
import tempfile
import time
import uuid
from unittest import mock

import requests
import testtools

from shakenfist_client import apiclient
from shakenfist_client import credentials


class ApiClientTestCase(testtools.TestCase):
    def setUp(self):
        super().setUp()

        self.request_url = mock.patch(
            'shakenfist_client.apiclient.Client._request_url')
        self.mock_request = self.request_url.start()
        self.addCleanup(self.request_url.stop)

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

        self.sleep = mock.patch('time.sleep')
        self.mock_sleep = self.sleep.start()
        self.addCleanup(self.sleep.stop)

    def test_get_instances(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        list(client.get_instances())

        self.mock_request.assert_called_with(
            'GET', '/instances', data={'all': False})

    def test_get_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_instance('notreallyauuid')

        self.mock_request.assert_called_with(
            'GET', '/instances/notreallyauuid', data=None)

    def test_get_instance_interfaces(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_instance_interfaces('notreallyauuid')

        self.mock_request.assert_called_with(
            'GET', '/instances/notreallyauuid/interfaces')

    def test_await_instance_create_created(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        with mock.patch.object(client, 'get_instance',
                               return_value={'state': 'created'}):
            client.await_instance_create('notreallyauuid')

    def test_await_instance_create_error_state(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        with mock.patch.object(client, 'get_instance',
                               return_value={'state': 'error'}):
            self.assertRaises(
                apiclient.InstanceWillNeverBeReady,
                client.await_instance_create, 'notreallyauuid')

    def test_await_instance_create_transitional_error_state(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        with mock.patch.object(client, 'get_instance',
                               return_value={'state': 'creating-error'}):
            self.assertRaises(
                apiclient.InstanceWillNeverBeReady,
                client.await_instance_create, 'notreallyauuid')

    def test_instance_await_sanity_check_error_states(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        for state in ['error', 'creating-error']:
            self.assertRaises(
                apiclient.InstanceWillNeverBeReady,
                client._instance_await_sanity_check,
                {'state': state, 'side_channels': ['sf-agent2']})

    def test_instance_await_sanity_check_healthy(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client._instance_await_sanity_check(
            {'state': 'created', 'side_channels': ['sf-agent2']})

    def test_create_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.create_instance('foo', 1, 2048, ['netuuid1'], ['8@cirros'],
                               'sshkey', None, namespace=None, force_placement=None,
                               video={'model': 'cirrus', 'memory': 16384})

        self.mock_request.assert_called_with(
            'POST', '/instances',
            deadline=mock.ANY,
            data={
                'name': 'foo',
                'cpus': 1,
                'memory': 2048,
                'network': ['netuuid1'],
                'ssh_key': 'sshkey',
                'user_data': None,
                'namespace': None,
                'video': {'model': 'cirrus', 'memory': 16384},
                'configdrive': None,
                'metadata': None,
                'side_channels': None,
                'uefi': False,
                'secure_boot': False,
                'nvram_template': None,
                'disk': ['8@cirros']
            })

    def test_create_instance_user_data(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.create_instance('foo', 1, 2048, ['netuuid1'], ['8@cirros'],
                               'sshkey', 'userdatabeforebase64', namespace=None,
                               force_placement=None,
                               video={'model': 'cirrus', 'memory': 16384})

        self.mock_request.assert_called_with(
            'POST', '/instances',
            deadline=mock.ANY,
            data={
                'name': 'foo',
                'cpus': 1,
                'memory': 2048,
                'network': ['netuuid1'],
                'ssh_key': 'sshkey',
                'user_data': 'userdatabeforebase64',
                'namespace': None,
                'video': {'model': 'cirrus', 'memory': 16384},
                'configdrive': None,
                'metadata': None,
                'side_channels': None,
                'uefi': False,
                'secure_boot': False,
                'nvram_template': None,
                'disk': ['8@cirros']
            })

    def test_snapshot_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.snapshot_instance('notreallyauuid', all=True)

        self.mock_request.assert_called_with(
            'POST', '/instances/notreallyauuid/snapshot',
            data={'all': True, 'device': None, 'thin': False})

    def test_soft_reboot_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.reboot_instance('notreallyauuid')

        self.mock_request.assert_called_with(
            'POST', '/instances/notreallyauuid/rebootsoft')

    def test_hard_reboot_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.reboot_instance('notreallyauuid', hard=True)

        self.mock_request.assert_called_with(
            'POST', '/instances/notreallyauuid/reboothard')

    def test_power_off_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.power_off_instance('notreallyauuid')

        self.mock_request.assert_called_with(
            'POST', '/instances/notreallyauuid/poweroff')

    def test_power_on_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.power_on_instance('notreallyauuid')

        self.mock_request.assert_called_with(
            'POST', '/instances/notreallyauuid/poweron')

    def test_pause_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.pause_instance('notreallyauuid')

        self.mock_request.assert_called_with(
            'POST', '/instances/notreallyauuid/pause')

    def test_unpause_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.unpause_instance('notreallyauuid')

        self.mock_request.assert_called_with(
            'POST', '/instances/notreallyauuid/unpause')

    def test_delete_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.delete_instance('notreallyauuid', async_request=True)

        self.mock_request.assert_called_with(
            'DELETE', '/instances/notreallyauuid', data=None)

    def test_delete_all_instances(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000',
                                  async_strategy=apiclient.ASYNC_CONTINUE)
        client.delete_all_instances(None)

        self.mock_request.assert_called_with(
            'DELETE', '/instances',
            data={'confirm': True, 'namespace': None})

    def test_delete_all_instances_namespace(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000',
                                  async_strategy=apiclient.ASYNC_CONTINUE)
        client.delete_all_instances('bobspace')

        self.mock_request.assert_called_with(
            'DELETE', '/instances',
            data={'confirm': True, 'namespace': 'bobspace'})

    def test_cache_artifact(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.cache_artifact('imageurl')

        self.mock_request.assert_called_with(
            'POST', '/artifacts',
            data={'url': 'imageurl', 'shared': False, 'namespace': None})

    def test_get_artifacts(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_artifacts('sf-2')

        self.mock_request.assert_called_with(
            'GET', '/artifacts',
            data={'node': 'sf-2'})

    def test_create_namespace(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.create_namespace('testspace')

        self.mock_request.assert_called_with(
            'POST', '/auth/namespaces',
            data={'namespace': 'testspace'})

    def test_get_namespace_keynames(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_namespace_keynames('testspace')

        self.mock_request.assert_called_with(
            'GET', '/auth/namespaces/testspace/keys')

    def test_get_namespace_keys(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'namespace-key-metadata'
        client.get_namespace_keys('testspace')

        self.mock_request.assert_called_with(
            'GET', '/auth/namespaces/testspace/keys?detail=true')

    def test_get_namespace_keys_incapable(self):
        # A server without the capability answers this same route with a
        # list of names, because detail is an unknown query parameter
        # there. Refusing is what stops a caller indexing into strings.
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''
        self.assertRaises(apiclient.IncapableException,
                          client.get_namespace_keys, 'testspace')
        self.mock_request.assert_not_called()

    def _public_client(self, root_html):
        # The login verbs reach public routes through
        # _actual_request_url, so this patches that rather than the
        # fixture's _request_url, and shows the latter is not used.
        patcher = mock.patch(
            'shakenfist_client.apiclient.Client._actual_request_url')
        mock_actual = patcher.start()
        self.addCleanup(patcher.stop)
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = root_html
        return client, mock_actual

    def test_get_login_issuers(self):
        client, mock_actual = self._public_client('human-login-metadata')
        mock_actual.return_value.json.return_value = [{'name': 'fake'}]

        self.assertEqual([{'name': 'fake'}], client.get_login_issuers())
        mock_actual.assert_called_once_with('GET', '/auth/login')
        self.mock_request.assert_not_called()

    def test_get_login_issuers_incapable(self):
        # Having the mint is not having the metadata.
        client, mock_actual = self._public_client('human-login')
        self.assertRaises(apiclient.IncapableException,
                          client.get_login_issuers)
        mock_actual.assert_not_called()
        self.mock_request.assert_not_called()

    def test_login(self):
        client, mock_actual = self._public_client('human-login')
        mock_actual.return_value.json.return_value = [{'namespace': 'fake'}]

        self.assertEqual([{'namespace': 'fake'}], client.login('faketoken'))
        mock_actual.assert_called_once_with(
            'POST', '/auth/login', data={'token': 'faketoken'},
            body_is_sensitive=True)
        self.mock_request.assert_not_called()

    def test_login_incapable(self):
        # Having the metadata is not having the mint.
        client, mock_actual = self._public_client('human-login-metadata')
        self.assertRaises(apiclient.IncapableException,
                          client.login, 'faketoken')
        mock_actual.assert_not_called()
        self.mock_request.assert_not_called()

    def test_check_capability_matches_whole_tokens(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = '<li>auth: human-login-metadata, other</li>'
        self.assertTrue(client.check_capability('human-login-metadata'))
        self.assertFalse(client.check_capability('human-login'))
        client.root_html = '<li>auth: other, human-login</li>'
        self.assertTrue(client.check_capability('human-login'))

    # A root page shaped exactly as the server renders one. The server
    # builds each family as '<li>%s: %s</li>' % (family, ', '.join(tokens))
    # and wraps the lot in the markup below -- see render_capabilities() and
    # Root.get() in shakenfist/external_api/app.py. The tokens are a real
    # excerpt of API_CAPABILITIES, chosen for the three shapes which decide
    # whether a whole-token match can fail closed: a token which is a
    # prefix of another in the same family, a family label which is itself
    # hyphen-joined, and a family with exactly one token so that the token
    # abuts the markup rather than a comma.
    REAL_ROOT_HTML = (
        '<html><head><title>Shaken Fist REST API service</title></head>'
        '<body><h1>Shaken Fist REST API service</h1>'
        '<p>You might be interested in the <a href="/apidocs">apidocs</a>.'
        '</p><p>Machine searchable API capabilities:</p><ul>'
        '<li>admin: cluster-cacert, cluster-resources, vdi-token-pubkey, '
        'cluster-capacity, cluster-resources-excluded, cluster-claims</li>'
        '<li>agent-operations: agentoperations-crud, '
        'instance-agentoperations, instance-agentoperations-all, '
        'agentoperations-put-with-mode</li>'
        '<li>auth: trusted-issuers, generated-key-secrets, '
        'scope-enforcement, mapping-rules, federated-exchange, '
        'issuer-discovery, human-login-metadata, human-login, '
        'namespace-claims, namespace-events, namespace-claim-events, '
        'namespace-key-metadata</li>'
        '<li>networkinterfaces: interface-metadata</li>'
        '<li>nodes: node-get, node-metadata, node-process-metrics</li>'
        '</ul></p></body></html>')

    def test_check_capability_against_a_real_root_page(self):
        # The whole-token match is a lookbehind and a lookahead over the
        # page as text, so it fails closed if a token is rendered next to a
        # word character or a hyphen: the verb then raises
        # IncapableException against a cluster which is perfectly capable.
        # The suite otherwise only feeds check_capability hand-written
        # fragments and bare space-separated strings, neither of which can
        # show that, and it gates around fifty call sites.
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = self.REAL_ROOT_HTML

        for token in ['cluster-cacert', 'cluster-claims',
                      'agentoperations-crud', 'instance-agentoperations',
                      'instance-agentoperations-all', 'human-login',
                      'human-login-metadata', 'namespace-events',
                      'namespace-claim-events', 'interface-metadata',
                      'node-get', 'node-process-metrics']:
            self.assertTrue(client.check_capability(token),
                            '%s was advertised and was not found' % token)

    def test_check_capability_against_a_real_root_page_refuses_fragments(self):
        # The other direction on the same page: nothing which is merely a
        # piece of an advertised token, or a piece of the markup around it,
        # may answer for a capability the cluster never claimed.
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = self.REAL_ROOT_HTML

        for token in ['agentoperations', 'instance', 'login', 'metadata',
                      'cluster', 'node', 'events', 'operations']:
            self.assertFalse(client.check_capability(token),
                             '%s is not a capability and was found' % token)

    def test_a_family_label_answers_as_though_it_were_a_capability(self):
        # Found by the fixture above, and recorded rather than fixed. The
        # page renders each family as '<li>auth: ...', so a family name sits
        # in the searched text on the same whole-token boundaries as the
        # tokens do, and 'auth' or 'nodes' answer a capability check which
        # no server ever advertised.
        #
        # Harmless today: no capability token shares a name with a family,
        # and this has been true of every version of this function -- the
        # substring match it replaced had the same hole and a larger one.
        # Closing it means parsing the list out of the page instead of
        # searching the page, which is a change to the roughly fifty call
        # sites' shared contract rather than to this test's subject. The
        # assertion is here so that the day a token is named after a family
        # the suite says so.
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = self.REAL_ROOT_HTML

        self.assertTrue(client.check_capability('auth'))
        self.assertTrue(client.check_capability('nodes'))

    def _mint_response(self):
        response = mock.MagicMock()
        response.status_code = 200
        response.history = []
        response.headers = {}
        response.text = json.dumps(
            [{'namespace': 'ci', 'key_name': 'corporate-aaaa',
              'key': 'notarealkey', 'expiry': 1.0, 'scopes': []}])
        return response

    def _mint_debug_log(self, **kwargs):
        # Drive the real request helper with a faked out session, and
        # return everything it debug logged.
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        with mock.patch.object(client, 'session') as mock_session:
            mock_session.request.return_value = self._mint_response()
            with mock.patch('shakenfist_client.apiclient.LOG') as mock_log:
                client._actual_request_url(
                    'POST', '/auth/login',
                    data=json.dumps({'token': 'notarealtoken'}), **kwargs)
        return ' '.join(str(call) for call in mock_log.debug.call_args_list)

    def test_the_mint_does_not_log_either_body(self):
        # The request body is an identity token and the response body is
        # every secret the call will ever return. The logging filter
        # sf-client installs redacts a JWT, which a namespace key is not,
        # so suppressing the logging is the only thing protecting one.
        logged = self._mint_debug_log(body_is_sensitive=True)
        self.assertNotIn('notarealtoken', logged)
        self.assertNotIn('notarealkey', logged)
        self.assertIn('not logged', logged)

    def test_an_ordinary_route_still_logs_its_bodies(self):
        # The negative control for the test above: without the flag both
        # bodies are logged, so that test can actually fail.
        logged = self._mint_debug_log()
        self.assertIn('notarealtoken', logged)
        self.assertIn('notarealkey', logged)

    def test_the_mint_marks_its_bodies_sensitive(self):
        client, mock_actual = self._public_client('human-login')
        mock_actual.return_value.json.return_value = []
        client.login('notarealtoken')

        self.assertTrue(
            mock_actual.call_args.kwargs.get('body_is_sensitive'))

    def test_login_incapable_messages_differ(self):
        client, _ = self._public_client('')
        with self.assertRaises(apiclient.IncapableException) as issuers:
            client.get_login_issuers()
        with self.assertRaises(apiclient.IncapableException) as mint:
            client.login('faketoken')
        self.assertNotEqual(str(issuers.exception), str(mint.exception))
        self.assertNotIn('faketoken', str(mint.exception))

    def test_get_namespace_key(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'namespace-key-metadata'
        client.get_namespace_key('testspace', 'testkeyname')

        self.mock_request.assert_called_with(
            'GET', '/auth/namespaces/testspace/keys/testkeyname')

    def test_get_namespace_key_incapable(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''
        self.assertRaises(apiclient.IncapableException,
                          client.get_namespace_key, 'testspace', 'testkeyname')
        self.mock_request.assert_not_called()

    def test_get_namespace_keynames_is_not_gated(self):
        # The bare listing predates the capability and must keep working
        # against a server which does not advertise it.
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''
        client.get_namespace_keynames('testspace')

        self.mock_request.assert_called_with(
            'GET', '/auth/namespaces/testspace/keys')

    def test_add_namespace_key(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.add_namespace_key('testspace', 'testkeyname', 'secretkey')

        self.mock_request.assert_called_with(
            'POST', '/auth/namespaces/testspace/keys',
            data={'key_name': 'testkeyname', 'key': 'secretkey'})

    def test_update_namespace_key(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.update_namespace_key('testspace', 'testkeyname', 'secretkey')

        self.mock_request.assert_called_with(
            'PUT', '/auth/namespaces/testspace/keys/testkeyname',
            data={'key': 'secretkey'})

    def test_delete_namespace_key(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.delete_namespace_key('testspace', 'keyname')

        self.mock_request.assert_called_with(
            'DELETE', '/auth/namespaces/testspace/keys/keyname')

    def test_get_namespace_claims(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_namespace_claims('testspace')

        self.mock_request.assert_called_with(
            'GET', '/auth/namespaces/testspace/claims')

    def test_get_namespace_events(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'namespace-events'
        client.get_namespace_events('testspace')

        self.mock_request.assert_called_with(
            'GET', '/auth/namespaces/testspace/events', {})

    def test_get_namespace_events_filtered(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'namespace-events events-by-type'
        client.get_namespace_events('testspace', event_type='audit', limit=5)

        self.mock_request.assert_called_with(
            'GET', '/auth/namespaces/testspace/events',
            {'event_type': 'audit', 'limit': 5})

    def test_get_namespace_events_incapable(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''
        self.assertRaises(apiclient.IncapableException,
                          client.get_namespace_events, 'testspace')

    def test_get_namespace_claim_events(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'namespace-claim-events'
        client.get_namespace_claim_events('testspace', 'a-claim')

        self.mock_request.assert_called_with(
            'GET', '/auth/namespaces/testspace/claims/a-claim/events', {})

    def test_get_namespace_claim_events_incapable(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        # The namespace token alone does not advertise the claim endpoint.
        client.root_html = 'namespace-events'
        self.assertRaises(apiclient.IncapableException,
                          client.get_namespace_claim_events, 'testspace', 'a-claim')

    def test_get_cluster_claims(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'cluster-claims'
        client.get_cluster_claims()

        self.mock_request.assert_called_with('GET', '/admin/claims', {})

    def test_get_cluster_claims_coverage_state(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'cluster-claims'
        client.get_cluster_claims(coverage_state='expired')

        self.mock_request.assert_called_with(
            'GET', '/admin/claims', {'coverage_state': 'expired'})

    def test_get_cluster_claims_forwards_an_empty_filter(self):
        # The server is the only validator, so a value the caller supplied
        # reaches it even when it is falsy, and gets the server's 400.
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'cluster-claims'
        client.get_cluster_claims(coverage_state='')

        self.mock_request.assert_called_with(
            'GET', '/admin/claims', {'coverage_state': ''})

    def test_get_cluster_claims_capability_gated(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''
        self.assertRaises(apiclient.IncapableException,
                          client.get_cluster_claims)

    def test_get_namespace_claim(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_namespace_claim('testspace', 'notreallyauuid')

        self.mock_request.assert_called_with(
            'GET', '/auth/namespaces/testspace/claims/notreallyauuid')

    def test_create_namespace_claim(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.create_namespace_claim('testspace', 4, 4096, 40, 3600)

        self.mock_request.assert_called_with(
            'POST', '/auth/namespaces/testspace/claims',
            data={'limit_cpus': 4, 'limit_memory_mb': 4096,
                  'limit_disk_gb': 40, 'expires_in_seconds': 3600})

    def test_update_namespace_claim_sends_only_what_changed(self):
        # The server reads the body as a field mask. Sending values the
        # caller did not ask to change turns a re-date into a resize, and
        # races whatever else is moving the claim.
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.update_namespace_claim('testspace', 'notreallyauuid',
                                      expires_in_seconds=1800)

        self.mock_request.assert_called_with(
            'PUT', '/auth/namespaces/testspace/claims/notreallyauuid',
            data={'expires_in_seconds': 1800})

    def test_update_namespace_claim_sends_every_field_it_is_given(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.update_namespace_claim('testspace', 'notreallyauuid',
                                      limit_cpus=8, limit_memory_mb=8192,
                                      limit_disk_gb=80, expires_in_seconds=60)

        self.mock_request.assert_called_with(
            'PUT', '/auth/namespaces/testspace/claims/notreallyauuid',
            data={'limit_cpus': 8, 'limit_memory_mb': 8192,
                  'limit_disk_gb': 80, 'expires_in_seconds': 60})

    def test_update_namespace_claim_leaves_an_empty_body_to_the_server(self):
        # Guessing at what the caller meant would be worse than the 400 the
        # server already answers for an update which names no fields.
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.update_namespace_claim('testspace', 'notreallyauuid')

        self.mock_request.assert_called_with(
            'PUT', '/auth/namespaces/testspace/claims/notreallyauuid',
            data={})

    def test_delete_namespace_claim(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.delete_namespace_claim('testspace', 'notreallyauuid')

        self.mock_request.assert_called_with(
            'DELETE', '/auth/namespaces/testspace/claims/notreallyauuid')

    def test_check_placement(self):
        answer = {
            'fits': True,
            'shapes': [
                {'node': 'node-1', 'candidates': ['node-1', 'node-3']},
                {'node': 'node-3', 'candidates': ['node-3']},
            ],
        }
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': answer})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'placement-query'
        result = client.check_placement(
            [{'cpus': 2, 'memory': 2048, 'disk': 20},
             {'cpus': 4, 'memory': 4096, 'disk': 40}])

        self.assertEqual(answer, result)
        self.mock_request.assert_called_with(
            'POST', '/placement',
            data={'shapes': [{'cpus': 2, 'memory': 2048, 'disk': 20},
                             {'cpus': 4, 'memory': 4096, 'disk': 40}]})

    def test_check_placement_sends_shapes_unaltered(self):
        # The server is the only validator of a shape, and the only thing
        # which sums a disk_spec or expands a count. Reshaping a shape here
        # is how a client and a cluster come to disagree about how large a
        # request is, so every key the caller supplied goes out as supplied.
        # The sizes are strings on purpose. create_instance() coerces
        # its caller's disk dicts with int(), and a shape whose values
        # are already canonical cannot tell that apart from passing them
        # through: int(50) is 50. A string size makes a coercion visible,
        # so the assertions below can see one.
        shapes = [
            {'cpus': 8, 'memory': 16384, 'count': 3,
             'disk_spec': [{'size': '50', 'base': 'debian:11'},
                           {'size': '10'}],
             'affinity': {'require_without_tag': ['runner']}},
        ]
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'placement-query'
        # Snapshotted before the call, because mock records the call by
        # reference: asserting against the same list object the method was
        # handed would put any in-place edit on both sides of the
        # comparison and pass. create_instance edits its caller's disk
        # dicts that way, so the failure mode has precedent here.
        expected = copy.deepcopy(shapes)
        client.check_placement(shapes)

        self.mock_request.assert_called_with(
            'POST', '/placement', data={'shapes': expected})

        # This is the assertion that pins "unaltered".
        self.assertEqual(expected, shapes)

    def test_check_placement_sends_an_empty_set_to_the_server(self):
        # An empty set is refused, and refused by the server: it answers
        # 400 'no shapes specified'. The client does not shortcut it,
        # because a caller-side guard would be a second opinion about what
        # a valid query is, and the two would drift.
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'placement-query'
        client.check_placement([])

        self.mock_request.assert_called_with(
            'POST', '/placement', data={'shapes': []})

    def test_check_placement_a_set_which_does_not_fit_is_an_answer(self):
        # The whole point of the endpoint's 200: a refused set is a
        # successful call which returns the refusal, so a caller can tell a
        # full cluster from a failed query. Raising here would throw away
        # the distinction, and this method's only consumer scores the stage
        # name against what a real create later met.
        answer = {'fits': False, 'stage': 'sufficient_idle_memory',
                  'shape': 1}
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': answer})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'placement-query'
        result = client.check_placement([{'cpus': 2, 'memory': 2048}])

        self.assertEqual(answer, result)
        self.assertFalse(result['fits'])
        self.assertEqual('sufficient_idle_memory', result['stage'])
        self.assertEqual(1, result['shape'])

    def test_check_placement_accepts_a_set_level_answer(self):
        # Per-node detail is administrative, so a caller without it is
        # answered with fits alone. The method must not require the shapes
        # key to be there.
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': {'fits': True}})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'placement-query'
        result = client.check_placement([{'cpus': 2, 'memory': 2048}])

        self.assertEqual({'fits': True}, result)

    def test_check_placement_capability_gated(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''
        self.assertRaises(
            apiclient.IncapableException,
            client.check_placement, [{'cpus': 2, 'memory': 2048}])

        # A server which predates the endpoint answers the request with a
        # 404, which says nothing a caller can act on. So no request is
        # sent at all.
        self.assertEqual(0, self.mock_request.call_count)

    def test_get_namespace_metadata(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_namespace_metadata('testspace')

        self.mock_request.assert_called_with(
            'GET', '/auth/namespaces/testspace/metadata')

    def test_set_namespace_metadata_item(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.set_namespace_metadata_item('testspace', 'billy', 'bob')

        self.mock_request.assert_called_with(
            'PUT', '/auth/namespaces/testspace/metadata/billy',
            data={'value': 'bob'})

    def test_delete_namespace_metadata_item(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.delete_namespace_metadata_item('testspace', 'billy')

        self.mock_request.assert_called_with(
            'DELETE', '/auth/namespaces/testspace/metadata/billy')

    def test_delete_instance_metadata_item(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.delete_instance_metadata_item('instance1', 'petname')

        self.mock_request.assert_called_with(
            'DELETE', '/instances/instance1/metadata/petname')

    def test_delete_network_metadata_item(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.delete_network_metadata_item('net1', 'herd')

        self.mock_request.assert_called_with(
            'DELETE', '/networks/net1/metadata/herd')

    def test_get_networks(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_networks()

        self.mock_request.assert_called_with(
            'GET', '/networks', data={'all': False})

    def test_get_network(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_network('notreallyauuid')

        self.mock_request.assert_called_with(
            'GET', '/networks/notreallyauuid', data=None)

    def test_delete_network(self):
        # Phase 7 contract: the server returns a 202 + op handle, and
        # the client transparently polls the op to terminal.
        handle = {'op_type': 'net_op', 'op_uuid': 'op-uuid'}
        final_view = {
            'operation_type': 'net_op',
            'uuid': 'op-uuid',
            'state': 'complete',
            'tasks': ['network_delete'],
        }
        self.mock_request.side_effect = [
            mock.Mock(**{'json.return_value': handle}),
            mock.Mock(**{'json.return_value': final_view}),
        ]

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        # Manually set the capability marker that get_cluster_operation
        # checks for; we mock the request path so it doesn't matter what
        # the server actually advertises.
        client.root_html = 'get-cluster-operations network-delete-async'
        result = client.delete_network('notreallyauuid')

        self.assertEqual(final_view, result)
        self.assertEqual(2, self.mock_request.call_count)
        self.mock_request.assert_any_call(
            'DELETE', '/networks/notreallyauuid', data=None)
        self.mock_request.assert_any_call(
            'GET', '/clusteroperations/net_op/op-uuid')

    def test_delete_network_no_wait(self):
        # wait=False returns the op handle untouched.
        handle = {'op_type': 'net_op', 'op_uuid': 'op-uuid'}
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': handle})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'network-delete-async'
        result = client.delete_network('notreallyauuid', wait=False)

        self.assertEqual(handle, result)
        self.mock_request.assert_called_with(
            'DELETE', '/networks/notreallyauuid', data=None)

    def test_delete_network_failure_raises(self):
        # An op that reaches the error state surfaces as
        # ClusterOperationFailed carrying the final view.
        handle = {'op_type': 'net_op', 'op_uuid': 'op-uuid'}
        error_view = {
            'operation_type': 'net_op',
            'uuid': 'op-uuid',
            'state': 'error',
            'tasks': ['network_delete'],
        }
        self.mock_request.side_effect = [
            mock.Mock(**{'json.return_value': handle}),
            mock.Mock(**{'json.return_value': error_view}),
        ]

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'get-cluster-operations network-delete-async'
        exc = self.assertRaises(
            apiclient.ClusterOperationFailed,
            client.delete_network, 'notreallyauuid')
        self.assertEqual('net_op', exc.op_type)
        self.assertEqual('op-uuid', exc.op_uuid)
        self.assertEqual(error_view, exc.op_view)

    def test_delete_all_networks(self):
        # Phase 7 contract: 202 returns a list of {network_uuid, op_type,
        # op_uuid}; the client polls each op to terminal and attaches the
        # final view under ``op_view``.
        handles = [
            {'network_uuid': 'net-a', 'op_type': 'net_op',
             'op_uuid': 'op-a'},
        ]
        final_view = {
            'operation_type': 'net_op', 'uuid': 'op-a',
            'state': 'complete', 'tasks': ['network_delete']}
        self.mock_request.side_effect = [
            mock.Mock(**{'json.return_value': handles}),
            mock.Mock(**{'json.return_value': final_view}),
        ]

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'get-cluster-operations network-delete-async'
        result = client.delete_all_networks(None)

        self.assertEqual(
            [{'network_uuid': 'net-a', 'op_type': 'net_op',
              'op_uuid': 'op-a', 'op_view': final_view}],
            result)
        self.mock_request.assert_any_call(
            'DELETE', '/networks',
            data={'confirm': True, 'namespace': None, 'clean_wait': False})
        self.mock_request.assert_any_call(
            'GET', '/clusteroperations/net_op/op-a')

    def test_delete_all_networks_no_wait(self):
        handles = [
            {'network_uuid': 'net-a', 'op_type': 'net_op',
             'op_uuid': 'op-a'},
            {'network_uuid': 'net-b', 'op_type': 'net_op',
             'op_uuid': 'op-b'},
        ]
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': handles})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'network-delete-async'
        result = client.delete_all_networks(None, wait=False)

        self.assertEqual(handles, result)
        self.mock_request.assert_called_with(
            'DELETE', '/networks',
            data={'confirm': True, 'namespace': None, 'clean_wait': False})

    def test_delete_all_networks_namespace(self):
        handles = [
            {'network_uuid': 'net-a', 'op_type': 'net_op',
             'op_uuid': 'op-a'},
        ]
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': handles})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'network-delete-async'
        client.delete_all_networks('bobspace', wait=False)

        self.mock_request.assert_called_with(
            'DELETE', '/networks',
            data={'confirm': True,
                  'namespace': 'bobspace',
                  'clean_wait': False})

    def test_delete_network_pre_phase7_server(self):
        # Pre-phase-7 servers do not advertise ``network-delete-async``
        # and return a 200 with the network's external view. The client
        # must passthrough that body without trying to poll.
        legacy_view = {
            'uuid': 'notreallyauuid', 'state': 'deleted', 'name': 'n'}
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': legacy_view})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''  # No capabilities advertised.
        result = client.delete_network('notreallyauuid')

        self.assertEqual(legacy_view, result)
        # Only the DELETE round-trip; no GET for the op handle.
        self.assertEqual(1, self.mock_request.call_count)

    def test_delete_all_networks_pre_phase7_server(self):
        legacy_list = [
            {'uuid': 'net-a', 'state': 'deleted', 'name': 'a'},
        ]
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': legacy_list})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''
        result = client.delete_all_networks(None)

        self.assertEqual(legacy_list, result)
        self.assertEqual(1, self.mock_request.call_count)

    def test_reserve_network_address(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'reserve-addresses'
        client.reserve_network_address('netuuid', '10.0.2.3',
                                       comment='kolla VIP')

        self.mock_request.assert_called_with(
            'POST', '/networks/netuuid/addresses/10.0.2.3',
            data={'comment': 'kolla VIP'})

    def test_reserve_network_address_without_a_comment(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'reserve-addresses'
        client.reserve_network_address('netuuid', '10.0.2.3')

        self.mock_request.assert_called_with(
            'POST', '/networks/netuuid/addresses/10.0.2.3', data={})

    def test_reserve_network_address_capability_gated(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''
        self.assertRaises(
            apiclient.IncapableException,
            client.reserve_network_address, 'netuuid', '10.0.2.3')

    def test_release_network_address(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'reserve-addresses'
        client.release_network_address('netuuid', '10.0.2.3')

        self.mock_request.assert_called_with(
            'DELETE', '/networks/netuuid/addresses/10.0.2.3')

    def test_release_network_address_capability_gated(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''
        self.assertRaises(
            apiclient.IncapableException,
            client.release_network_address, 'netuuid', '10.0.2.3')

    def test_get_cluster_operation_chain_capability_gated(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''
        self.assertRaises(
            apiclient.IncapableException,
            client.get_cluster_operation_chain, 'op-1')

    def test_list_cluster_operations_for_target_capability_gated(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = ''
        self.assertRaises(
            apiclient.IncapableException,
            client.list_cluster_operations_for_target, 'network', 'net-uuid')

    def test_get_cluster_operation_chain(self):
        chain = [
            {'operation_type': 'net_op', 'uuid': 'op-1',
             'state': 'complete', 'tasks': ['network_delete']},
            {'operation_type': 'net_op', 'uuid': 'op-2',
             'state': 'complete', 'tasks': ['network_deploy']},
        ]
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': chain})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'cluster-operation-chain'
        result = client.get_cluster_operation_chain('op-1')

        self.assertEqual(chain, result)
        self.mock_request.assert_called_with(
            'GET', '/clusteroperations/op-1/chain')

    def test_list_cluster_operations_for_target(self):
        ops = [
            {'operation_type': 'net_op', 'uuid': 'op-1',
             'state': 'complete', 'tasks': ['network_delete']},
        ]
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': ops})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = 'cluster-operations-by-target'
        result = client.list_cluster_operations_for_target(
            'network', 'net-uuid')

        self.assertEqual(ops, result)
        self.mock_request.assert_called_with(
            'GET', '/clusteroperations',
            data={'target_object_type': 'network',
                  'target_uuid': 'net-uuid'})

    def _client_with(self, capability):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.root_html = capability
        return client

    def _check_clusteroperations_wrapper(self, object_type):
        ops = [
            {'operation_type': 'inst_op', 'uuid': 'op-1',
             'state': 'executing', 'observational': False},
            {'operation_type': 'inst_op', 'uuid': 'op-0',
             'state': 'complete', 'observational': True},
        ]
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': ops})

        client = self._client_with('%s-clusteroperations' % object_type)
        method = getattr(client, 'get_%s_clusteroperations' % object_type)
        url = '/%ss/some-ref/clusteroperations' % object_type

        self.assertEqual(ops, method('some-ref'))
        self.mock_request.assert_called_with(
            'GET', url, data={'all': False})

        self.assertEqual(ops, method('some-ref', all=True))
        self.mock_request.assert_called_with(
            'GET', url, data={'all': True})

        self.assertEqual(ops, method('some-ref', namespace='gerkin'))
        self.mock_request.assert_called_with(
            'GET', url, data={'all': False, 'namespace': 'gerkin'})

        self.mock_request.reset_mock()
        client.root_html = 'nothing-useful'
        self.assertRaises(apiclient.IncapableException, method, 'some-ref')
        self.mock_request.assert_not_called()

    def test_get_instance_clusteroperations(self):
        self._check_clusteroperations_wrapper('instance')

    def test_get_network_clusteroperations(self):
        self._check_clusteroperations_wrapper('network')

    def test_get_artifact_clusteroperations(self):
        self._check_clusteroperations_wrapper('artifact')

    def test_await_no_pending_operations_polls_until_clear(self):
        views = [{'uuid': 'i', 'has_pending_operations': True},
                 {'uuid': 'i', 'has_pending_operations': True},
                 {'uuid': 'i', 'has_pending_operations': False}]
        self.mock_request.side_effect = [
            mock.Mock(**{'json.return_value': v}) for v in views]

        client = self._client_with('object-pending-operations')
        with mock.patch('shakenfist_client.apiclient.time.sleep') as sleep:
            result = client.await_no_pending_operations(
                'instance', 'i', timeout=60, interval=3)

        self.assertEqual(views[2], result)
        self.assertEqual(3, self.mock_request.call_count)
        self.assertEqual([mock.call(3), mock.call(3)], sleep.call_args_list)

    def test_await_no_pending_operations_each_type(self):
        for object_type in ('instance', 'network', 'artifact'):
            self.mock_request.reset_mock()
            self.mock_request.side_effect = None
            self.mock_request.return_value = mock.Mock(
                **{'json.return_value': {'has_pending_operations': False}})

            client = self._client_with(
                'object-pending-operations get-instance-namespace '
                'get-network-namespace')
            client.await_no_pending_operations(
                object_type, 'ref', namespace='gerkin', timeout=60)
            self.mock_request.assert_called_once_with(
                'GET', '/%ss/ref' % object_type,
                data={'namespace': 'gerkin'})

    def test_await_no_pending_operations_incapable(self):
        # The mocked object is settled, so if the capability check were
        # missing the call would return a view at once instead of raising,
        # and both assertions below would fail.
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': {'has_pending_operations': False}})
        client = self._client_with('nothing-useful')
        self.assertRaises(
            apiclient.IncapableException,
            client.await_no_pending_operations, 'instance', 'i', timeout=60)
        self.mock_request.assert_not_called()

    def test_await_no_pending_operations_bad_type(self):
        client = self._client_with('object-pending-operations')
        self.assertRaises(
            apiclient.InvalidException,
            client.await_no_pending_operations, 'blob', 'b', timeout=60)
        self.mock_request.assert_not_called()

    def test_await_no_pending_operations_bad_interval(self):
        client = self._client_with('object-pending-operations')
        for interval in (0, -1):
            self.assertRaises(
                apiclient.InvalidException,
                client.await_no_pending_operations, 'instance', 'i',
                timeout=60, interval=interval)
        self.mock_request.assert_not_called()

    def test_await_no_pending_operations_view_without_field(self):
        # A view which does not report the field must not count as settled.
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': {'uuid': 'a'}})
        client = self._client_with('object-pending-operations')
        self.assertRaises(
            apiclient.IncapableException,
            client.await_no_pending_operations, 'artifact', 'a', timeout=60)

    def test_await_no_pending_operations_no_wait_view_without_field(self):
        # A negative timeout returns without waiting, but still refuses a
        # view which does not report the field.
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': {'uuid': 'i'}})
        client = self._client_with('object-pending-operations')
        client.async_strategy = apiclient.ASYNC_CONTINUE
        self.assertRaises(
            apiclient.IncapableException,
            client.await_no_pending_operations, 'instance', 'i')
        self.assertEqual(1, self.mock_request.call_count)

    def test_await_no_pending_operations_object_deleted(self):
        # An object deleted while waiting raises rather than counting as
        # settled.
        self.mock_request.side_effect = [
            mock.Mock(**{'json.return_value': {'has_pending_operations': True}}),
            apiclient.ResourceNotFoundException(
                'API request failed', 'GET',
                'http://localhost:13000/instances/i', 404,
                '{"error": "instance not found"}')]
        client = self._client_with('object-pending-operations')
        with mock.patch('shakenfist_client.apiclient.time.sleep'):
            self.assertRaises(
                apiclient.ResourceNotFoundException,
                client.await_no_pending_operations, 'instance', 'i',
                timeout=60)
        self.assertEqual(2, self.mock_request.call_count)

    def test_await_no_pending_operations_zero_timeout(self):
        # A zero timeout checks once and raises if the object is pending.
        self.mock_request.side_effect = [
            mock.Mock(**{'json.return_value': {'has_pending_operations': True}})
            for _ in range(10)]
        client = self._client_with('object-pending-operations')
        with mock.patch('shakenfist_client.apiclient.time.sleep'):
            with mock.patch('shakenfist_client.apiclient.time.time',
                            side_effect=[0, 1, 2, 3]):
                self.assertRaises(
                    apiclient.TimeoutException,
                    client.await_no_pending_operations, 'instance', 'i',
                    timeout=0)
        self.assertEqual(1, self.mock_request.call_count)

    def test_get_artifact_namespace(self):
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': {'uuid': 'a', 'blobs': {}}})
        client = self._client_with('')
        client.get_artifact('a')
        self.mock_request.assert_called_with('GET', '/artifacts/a', data=None)
        client.get_artifact('a', namespace='gerkin')
        self.mock_request.assert_called_with(
            'GET', '/artifacts/a', data={'namespace': 'gerkin'})

    def test_await_no_pending_operations_timeout(self):
        # A finite supply of responses, so a loop which never times out
        # fails instead of hanging.
        self.mock_request.side_effect = [
            mock.Mock(**{'json.return_value': {'has_pending_operations': True}})
            for _ in range(10)]

        client = self._client_with('object-pending-operations')
        with mock.patch('shakenfist_client.apiclient.time.sleep'):
            with mock.patch('shakenfist_client.apiclient.time.time',
                            side_effect=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]):
                self.assertRaises(
                    apiclient.TimeoutException,
                    client.await_no_pending_operations,
                    'network', 'n', timeout=2)
        # Polled at times 0 and 1 within the deadline, then once more at 2
        # before the check at 3 finds it passed.
        self.assertEqual(3, self.mock_request.call_count)

    def test_await_no_pending_operations_default_timeout(self):
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': {'has_pending_operations': True}})

        client = self._client_with('object-pending-operations')
        client.async_strategy = apiclient.ASYNC_CONTINUE
        result = client.await_no_pending_operations('instance', 'i')
        self.assertEqual({'has_pending_operations': True}, result)
        self.assertEqual(1, self.mock_request.call_count)

        client.async_strategy = apiclient.ASYNC_PAUSE
        self.mock_request.reset_mock()
        self.mock_request.side_effect = [
            mock.Mock(**{'json.return_value': {'has_pending_operations': True}})
            for _ in range(10)]
        with mock.patch('shakenfist_client.apiclient.time.sleep'):
            with mock.patch('shakenfist_client.apiclient.time.time',
                            side_effect=[0, 59, 61, 62]):
                self.assertRaises(
                    apiclient.TimeoutException,
                    client.await_no_pending_operations, 'instance', 'i')
        # The 60 second default deadline lets the poll at 59 through and
        # stops at 61.
        self.assertEqual(2, self.mock_request.call_count)

    def test_allocate_network(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.allocate_network('192.168.1.0/24', True, True, 'gerkin', None)

        self.mock_request.assert_called_with(
            'POST', '/networks',
            data={
                'netblock': '192.168.1.0/24',
                'provide_dhcp': True,
                'provide_nat': True,
                'name': 'gerkin',
                'namespace': None
            })

    def test_get_existing_locks(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_existing_locks()

        self.mock_request.assert_called_with(
            'GET', '/admin/locks')

    def test_get_console_data_returns_bytes_when_decode_none(self):
        # 1000 raw bytes including a 3-byte UTF-8 ellipsis. Decoding as text
        # would yield 998 characters; with decode=None we want the raw bytes.
        raw = b'a' * 997 + '…'.encode('utf-8')
        self.assertEqual(1000, len(raw))
        self.mock_request.return_value = mock.Mock(content=raw)

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        out = client.get_console_data('uuid', length=1000, decode=None)

        self.assertEqual(raw, out)
        self.assertEqual(1000, len(out))
        self.mock_request.assert_called_with(
            'GET', '/instances/uuid/consoledata',
            data={'length': 1000}, response_body_is_binary=True)

    def test_get_vdi_console_proxy(self):
        proxy = {'url': 'https://kerbside.example.com/sf-console.vv?t=xyz',
                 'expires_at': '2026-07-20T00:00:00Z'}
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': proxy})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        result = client.get_vdi_console_proxy('notreallyauuid')

        self.assertEqual(proxy, result)
        self.mock_request.assert_called_with(
            'GET', '/instances/notreallyauuid/vdiconsoleproxy')

    def test_get_vdi_token_public_keys(self):
        keys = {'keys': [{'kid': 'key-1', 'pem': 'fake-pem'}]}
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': keys})

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        result = client.get_vdi_token_public_keys()

        self.assertEqual(keys, result)
        self.mock_request.assert_called_with(
            'GET', '/admin/vditokenpubkey')

    @mock.patch('shakenfist_client.apiclient.requests.get')
    def test_get_vdi_console_proxy_file(self, mock_get):
        proxy_url = 'https://kerbside.example.com/sf-console.vv?t=xyz'
        proxy = {'url': proxy_url, 'expires_at': '2026-07-20T00:00:00Z'}
        self.mock_request.return_value = mock.Mock(
            **{'json.return_value': proxy})
        mock_get.return_value = mock.Mock(text='[virt-viewer]\nfake=1\n')

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        result = client.get_vdi_console_proxy_file('notreallyauuid')

        self.assertEqual('[virt-viewer]\nfake=1\n', result)

        # The .vv fetch must be a plain requests.get() carrying no SF
        # bearer token -- not routed through _request_url.
        mock_get.assert_called_once_with(
            proxy_url, timeout=client.sync_request_timeout)
        mock_get.return_value.raise_for_status.assert_called_once_with()
        self.assertNotIn(
            mock.call('GET', '/sf-console.vv', data=mock.ANY),
            self.mock_request.mock_calls)
        # Only the vdiconsoleproxy round-trip went through _request_url.
        self.mock_request.assert_called_once_with(
            'GET', '/instances/notreallyauuid/vdiconsoleproxy')
        for call_args in mock_get.call_args_list:
            args, kwargs = call_args
            self.assertNotIn('headers', kwargs)

    def test_get_console_data_decodes_with_replacement(self):
        # Invalid byte 0x80 in the middle should not raise; the decoder is
        # asked to replace it.
        raw = b'hello\x80world'
        self.mock_request.return_value = mock.Mock(content=raw)

        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        out = client.get_console_data('uuid', length=100)

        self.assertIsInstance(out, str)
        self.assertIn('hello', out)
        self.assertIn('world', out)


class GetNodesMock():
    def json(self):
        return json.loads("""[
{
    "name": "sf-1.c.mikal-269605.internal",
    "ip": "10.128.15.213",
    "lastseen": "Mon, 13 Apr 2020 03:00:22 -0000"
},
{
    "name": "sf-2.c.mikal-269605.internal",
    "ip": "10.128.15.210",
    "lastseen": "Mon, 13 Apr 2020 03:04:17 -0000"
}
]
""")


class ApiClientGetNodesTestCase(testtools.TestCase):
    def setUp(self):
        super().setUp()

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

    @mock.patch('shakenfist_client.apiclient.Client._request_url',
                return_value=GetNodesMock())
    def test_get_nodes(self, mock_request):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        list(client.get_nodes())

        mock_request.assert_called_with(
            'GET', '/nodes')


class ApiClientConfigurationLookupTestCase(testtools.TestCase):
    """Where a client's credentials come from, and in what order.

    Five sources, consulted in this order: the constructor's arguments
    (which are the command line flags), the environment variables, the
    credentials a human login cached, ~/.shakenfist and
    /etc/sf/shakenfist.json. Each is consulted only while no API URL has
    been established, so the order is very nearly the whole of the
    behaviour; the exception is a caller which named a cluster and no
    credential, which no source answered at all before the cache existed.
    """

    CLUSTER = 'http://sf.example.com/api'
    OTHER_CLUSTER = 'http://elsewhere.example.com/api'

    # Deliberately unconvincing. A credential scanner runs over this
    # repository's history with a positive control, so a fixture shaped like
    # a real namespace key is a finding whether or not it ever unlocked
    # anything.
    CACHED_KEY = 'not-a-cached-key-1'
    OTHER_CACHED_KEY = 'not-a-cached-key-2'

    def setUp(self):
        super().setUp()

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

        # Ensure the config files on the test machine are not consulted
        self.exists = mock.patch(
            'shakenfist_client.apiclient.os.path.exists', return_value=False)
        self.mock_exists = self.exists.start()
        self.addCleanup(self.exists.stop)

        # Nor the credential cache on it. The chain asks the cache module
        # where the file is rather than being told, so that one function is
        # the only seam a test needs: pointed at an empty temporary
        # directory, a login by whoever is running the tests cannot change
        # what they assert.
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir, True)
        self.cache_path = os.path.join(self.tempdir, 'shakenfist', 'credentials.json')
        self.cache_file = mock.patch(
            'shakenfist_client.credentials.credentials_path',
            side_effect=lambda path=None: path or self.cache_path)
        self.cache_file.start()
        self.addCleanup(self.cache_file.stop)

    def credential(self, apiurl=None, namespace='cachedspace',
                   key_name='cached-aaaa', key=None, expiry=None):
        """One cache entry, shaped as the cache stores it."""
        return {
            'apiurl': apiurl or self.CLUSTER,
            'namespace': namespace,
            'key_name': key_name,
            'key': key or self.CACHED_KEY,
            'expiry': expiry,
            'scopes': ['blob.read'],
        }

    def selection(self, credential):
        return {
            'apiurl': credential['apiurl'],
            'namespace': credential['namespace'],
            'key_name': credential['key_name'],
        }

    def cache(self, entries, current=-1, mode=None):
        """Write the credential cache the chain will consult.

        current defaults to selecting the first entry, which is what a login
        minting one credential leaves behind; None writes a cache with no
        selection at all.
        """
        if current == -1:
            current = self.selection(entries[0])
        credentials.write_credentials(entries, current=current, path=self.cache_path)
        if mode is not None:
            os.chmod(self.cache_path, mode)

    def configuration_files(self, user_conf=None, system_conf=None):
        """Make the two configuration files hold these documents.

        Both paths are absolute and neither is one a test may write, so they
        are answered by name: the chain's os.path.exists() is already
        patched to deny every path, and open() is redirected for these two
        and passed through for everything else. Nothing here reads or writes
        the real ~/.shakenfist or /etc/sf/shakenfist.json.
        """
        documents = {}
        if user_conf is not None:
            documents[os.path.expanduser('~/.shakenfist')] = user_conf
        if system_conf is not None:
            documents['/etc/sf/shakenfist.json'] = system_conf

        self.mock_exists.side_effect = lambda path: path in documents

        real_open = open

        def fake_open(path, *args, **kwargs):
            if path in documents:
                return io.StringIO(json.dumps(documents[path]))
            return real_open(path, *args, **kwargs)

        opener = mock.patch('builtins.open', side_effect=fake_open)
        opener.start()
        self.addCleanup(opener.stop)

    @mock.patch.dict('os.environ', {
        'SHAKENFIST_API_URL': 'http://sf.example.com/api',
        'SHAKENFIST_NAMESPACE': 'testspace',
        'SHAKENFIST_KEY': 'testkey',
    })
    def test_environment_variables(self):
        client = apiclient.Client()

        self.assertEqual('http://sf.example.com/api', client.base_url)
        self.assertEqual('testspace', client.namespace)
        self.assertEqual('testkey', client.key)

    @mock.patch.dict('os.environ', {
        'SHAKENFIST_API_URL': 'http://sf.example.com/api',
        'SHAKENFIST_NAMESPACE': 'testspace',
        'SHAKENFIST_KEY': 'testkey',
    })
    def test_arguments_beat_environment_variables(self):
        client = apiclient.Client(
            base_url='http://elsewhere.example.com/api',
            namespace='otherspace', key='otherkey')

        self.assertEqual('http://elsewhere.example.com/api', client.base_url)
        self.assertEqual('otherspace', client.namespace)
        self.assertEqual('otherkey', client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_unconfigured(self):
        self.assertRaises(apiclient.UnconfiguredException, apiclient.Client)

    # -- the credential cache's place in the order -------------------------

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_a_flag_beats_the_cache(self):
        # A flag is how a person says "this credential, this time", and it is
        # as explicit as a login and more recent than one.
        self.cache([self.credential()])

        client = apiclient.Client(base_url=self.CLUSTER, namespace='flagspace',
                                  key='flagkey')

        self.assertEqual(self.CLUSTER, client.base_url)
        self.assertEqual('flagspace', client.namespace)
        self.assertEqual('flagkey', client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_a_flag_beats_the_cache_without_an_api_url(self):
        # The flags named a credential but not a cluster, so the cache
        # supplies the cluster it was minted against and nothing else: the
        # namespace and key the caller named are not replaced.
        self.cache([self.credential()])

        client = apiclient.Client(namespace='flagspace', key='flagkey')

        self.assertEqual(self.CLUSTER, client.base_url)
        self.assertEqual('flagspace', client.namespace)
        self.assertEqual('flagkey', client.key)

    @mock.patch.dict('os.environ', {
        'SHAKENFIST_API_URL': 'http://sf.example.com/api',
        'SHAKENFIST_NAMESPACE': 'testspace',
        'SHAKENFIST_KEY': 'testkey',
    })
    def test_an_environment_variable_beats_the_cache(self):
        # The cache holds a selected credential for the very cluster the
        # environment names, and still loses: this is how a script or a CI
        # job pins a credential, and it must not start using whoever logged
        # in on the machine it runs on.
        self.cache([self.credential()])

        client = apiclient.Client()

        self.assertEqual('http://sf.example.com/api', client.base_url)
        self.assertEqual('testspace', client.namespace)
        self.assertEqual('testkey', client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_the_cache_beats_the_user_configuration_file(self):
        # A login is more recent and more explicit than a configuration file
        # written at some point in the past. If the file won, the login would
        # appear to have done nothing.
        self.cache([self.credential()])
        self.configuration_files(user_conf={
            'apiurl': self.OTHER_CLUSTER, 'namespace': 'filespace',
            'key': 'filekey'})

        client = apiclient.Client()

        self.assertEqual(self.CLUSTER, client.base_url)
        self.assertEqual('cachedspace', client.namespace)
        self.assertEqual(self.CACHED_KEY, client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_the_cache_beats_the_system_configuration_file(self):
        self.cache([self.credential()])
        self.configuration_files(system_conf={
            'apiurl': self.OTHER_CLUSTER, 'namespace': 'systemspace',
            'key': 'systemkey'})

        client = apiclient.Client()

        self.assertEqual(self.CLUSTER, client.base_url)
        self.assertEqual('cachedspace', client.namespace)
        self.assertEqual(self.CACHED_KEY, client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_the_user_configuration_file_is_used_when_there_is_no_cache(self):
        # Nobody has logged in, so the behaviour is exactly what it was
        # before the cache existed.
        self.configuration_files(user_conf={
            'apiurl': self.OTHER_CLUSTER, 'namespace': 'filespace',
            'key': 'filekey'})

        client = apiclient.Client()

        self.assertEqual(self.OTHER_CLUSTER, client.base_url)
        self.assertEqual('filespace', client.namespace)
        self.assertEqual('filekey', client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_the_user_configuration_file_still_beats_the_system_one(self):
        self.configuration_files(
            user_conf={'apiurl': self.OTHER_CLUSTER, 'namespace': 'filespace',
                       'key': 'filekey'},
            system_conf={'apiurl': self.CLUSTER, 'namespace': 'systemspace',
                         'key': 'systemkey'})

        client = apiclient.Client()

        self.assertEqual(self.OTHER_CLUSTER, client.base_url)
        self.assertEqual('filespace', client.namespace)
        self.assertEqual('filekey', client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_an_expired_cached_credential_is_not_used(self):
        # The cluster stops honouring the key the moment its expiry passes,
        # so a cache which went on presenting one would turn a lapsed login
        # into an unexplained authorisation failure.
        self.cache([self.credential(expiry=1.0)])
        self.configuration_files(user_conf={
            'apiurl': self.OTHER_CLUSTER, 'namespace': 'filespace',
            'key': 'filekey'})

        client = apiclient.Client()

        self.assertEqual(self.OTHER_CLUSTER, client.base_url)
        self.assertEqual('filespace', client.namespace)
        self.assertEqual('filekey', client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_an_unselected_cache_supplies_nothing(self):
        # Several credentials and no selection: the person has not said which
        # namespace they mean, and nothing here guesses.
        self.cache([self.credential(),
                    self.credential(namespace='other', key_name='cached-bbbb',
                                    key=self.OTHER_CACHED_KEY)],
                   current=None)
        self.configuration_files(user_conf={
            'apiurl': self.OTHER_CLUSTER, 'namespace': 'filespace',
            'key': 'filekey'})

        client = apiclient.Client()

        self.assertEqual(self.OTHER_CLUSTER, client.base_url)
        self.assertEqual('filespace', client.namespace)
        self.assertEqual('filekey', client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_a_cache_which_cannot_be_read_is_refused_not_passed_over(self):
        # A world readable cache is indistinguishable from "nobody has logged
        # in" to anything which only checks whether a credential came back,
        # so falling through to the configuration file would quietly use a
        # different identity and say nothing about a long lived secret other
        # users can now read.
        self.cache([self.credential()], mode=0o644)
        self.configuration_files(user_conf={
            'apiurl': self.OTHER_CLUSTER, 'namespace': 'filespace',
            'key': 'filekey'})

        e = self.assertRaises(credentials.CredentialCacheException,
                              apiclient.Client)

        self.assertIn(self.cache_path, str(e))

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_a_corrupt_cache_is_refused_not_passed_over(self):
        self.cache([self.credential()])
        with open(self.cache_path, 'w') as f:
            f.write('{"version": 1, "credentials": [')
        self.configuration_files(user_conf={
            'apiurl': self.OTHER_CLUSTER, 'namespace': 'filespace',
            'key': 'filekey'})

        e = self.assertRaises(credentials.CredentialCacheException,
                              apiclient.Client)

        self.assertIn(self.cache_path, str(e))

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_a_cache_which_cannot_be_read_does_not_break_a_pinned_caller(self):
        # The file is read only where the cache would have been used, so a
        # caller which brought its own credentials never reads it and cannot
        # be stopped by one it did not ask to use.
        self.cache([self.credential()], mode=0o644)

        client = apiclient.Client(base_url=self.OTHER_CLUSTER,
                                  namespace='flagspace', key='flagkey')

        self.assertEqual(self.OTHER_CLUSTER, client.base_url)
        self.assertEqual('flagspace', client.namespace)
        self.assertEqual('flagkey', client.key)

    # -- a caller which named a cluster but no credential ------------------

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_the_cache_completes_a_flag_supplied_url(self):
        # An API URL and nothing else reaches none of the sources below,
        # because they are all gated on there being no API URL yet, so such a
        # caller has no credential at all today. The cache can answer that
        # for the cluster it names.
        self.cache([self.credential()])

        client = apiclient.Client(base_url=self.CLUSTER)

        self.assertEqual(self.CLUSTER, client.base_url)
        self.assertEqual('cachedspace', client.namespace)
        self.assertEqual(self.CACHED_KEY, client.key)

    @mock.patch.dict('os.environ',
                     {'SHAKENFIST_API_URL': 'http://sf.example.com/api'},
                     clear=True)
    def test_the_cache_completes_an_environment_supplied_url(self):
        self.cache([self.credential()])

        client = apiclient.Client()

        self.assertEqual(self.CLUSTER, client.base_url)
        self.assertEqual('cachedspace', client.namespace)
        self.assertEqual(self.CACHED_KEY, client.key)

    @mock.patch.dict('os.environ', {
        'SHAKENFIST_NAMESPACE': 'testspace',
        'SHAKENFIST_KEY': 'testkey',
    }, clear=True)
    def test_an_environment_credential_beats_the_cache_for_a_named_cluster(self):
        # The caller named a cluster, so the environment's own API URL never
        # came up -- but its credential still outranks a login, for the same
        # reason it does when it supplies the URL too. A job which exports
        # SHAKENFIST_KEY and passes an API URL must not quietly run as
        # whoever last logged in on the machine.
        self.cache([self.credential()])

        client = apiclient.Client(base_url=self.CLUSTER)

        self.assertEqual('testspace', client.namespace)
        self.assertEqual('testkey', client.key)

    @mock.patch.dict('os.environ', {'SHAKENFIST_KEY': 'testkey'}, clear=True)
    def test_an_environment_key_alone_is_not_completed_from_the_cache(self):
        # Half a credential from the environment is still a pinned
        # credential, and completing the other half from a login would
        # present a pair that neither ever had.
        self.cache([self.credential()])

        client = apiclient.Client(base_url=self.CLUSTER)

        self.assertEqual('testkey', client.key)
        self.assertIsNone(client.namespace)

    # -- an environment which pins a credential but names no cluster -------

    @mock.patch.dict('os.environ', {
        'SHAKENFIST_NAMESPACE': 'testspace',
        'SHAKENFIST_KEY': 'testkey',
    }, clear=True)
    def test_an_environment_credential_with_no_url_beats_the_cache(self):
        # The environment pins a credential and leaves the cluster to be
        # found. The cache is the only thing here which knows a cluster, so
        # it supplies that -- and nothing else. Before the cache existed
        # this shape reached ~/.shakenfist and took all three from it, which
        # is the same inversion one level down: an automated caller which
        # exports SHAKENFIST_KEY must not run as whoever logged in on the
        # machine, whether the identity it would acquire came from a login
        # or from a file.
        #
        # This is the ordering D7 names which the CLI gets for free, because
        # click injects both variables as arguments however the cluster was
        # named. A library caller such as the ansible collection does not,
        # which is why it is asserted here.
        self.cache([self.credential()])

        client = apiclient.Client()

        self.assertEqual(self.CLUSTER, client.base_url)
        self.assertEqual('testspace', client.namespace)
        self.assertEqual('testkey', client.key)

    @mock.patch.dict('os.environ', {'SHAKENFIST_KEY': 'testkey'}, clear=True)
    def test_an_environment_key_alone_with_no_url_is_not_completed(self):
        # The same rule as the named-cluster case: half a credential from
        # the environment is a pinned credential, so the cache supplies the
        # cluster and neither half of its own. The result is a client with
        # no namespace, which fails to authenticate -- the honest outcome,
        # and better than silently presenting one user's key against another
        # user's namespace.
        self.cache([self.credential()])

        client = apiclient.Client()

        self.assertEqual(self.CLUSTER, client.base_url)
        self.assertEqual('testkey', client.key)
        self.assertIsNone(client.namespace)

    @mock.patch.dict('os.environ', {
        'SHAKENFIST_NAMESPACE': 'testspace',
        'SHAKENFIST_KEY': 'testkey',
    }, clear=True)
    def test_an_environment_credential_with_no_url_beats_the_file(self):
        # The same inversion with no cache in play at all. The documented
        # order puts the environment above both configuration files, and
        # this is the one input shape where it did not.
        self.configuration_files(user_conf={
            'apiurl': self.OTHER_CLUSTER, 'namespace': 'filespace',
            'key': 'filekey'})

        client = apiclient.Client()

        self.assertEqual(self.OTHER_CLUSTER, client.base_url)
        self.assertEqual('testspace', client.namespace)
        self.assertEqual('testkey', client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_a_half_supplied_credential_with_no_url_is_not_completed(self):
        # The argument half of the same rule, and the reason the cache's
        # credential is taken only when neither half was pinned rather than
        # half at a time: a caller which named a namespace and no key used
        # to be handed the login's key to go with its own namespace.
        self.cache([self.credential()])

        client = apiclient.Client(namespace='flagspace')

        self.assertEqual(self.CLUSTER, client.base_url)
        self.assertEqual('flagspace', client.namespace)
        self.assertIsNone(client.key)

    @mock.patch.dict('os.environ', {'SHAKENFIST_NAMESPACE': 'testspace'},
                     clear=True)
    def test_half_an_environment_credential_pairs_with_a_files_other_half(self):
        # The documented behaviour of the one shape where the two
        # configuration files' per-half fill is visible, pinned because it
        # is a behaviour change for library embedders rather than something
        # anyone would want. Before, with no API URL in the environment,
        # neither variable was read at all and the file supplied a matched
        # pair; now the environment's namespace is kept and only the key
        # comes from the file, which is a credential no single source ever
        # held.
        #
        # The files keep that behaviour deliberately -- it is years older
        # than the credential cache and not this work's to change -- so the
        # advice in docs/human-login.md is to export both halves or
        # neither. sf-client is unaffected: click reads whichever variables
        # are set and passes them down as arguments.
        self.configuration_files(user_conf={
            'apiurl': self.OTHER_CLUSTER, 'namespace': 'filespace',
            'key': 'filekey'})

        client = apiclient.Client()

        self.assertEqual(self.OTHER_CLUSTER, client.base_url)
        self.assertEqual('testspace', client.namespace)
        self.assertEqual('filekey', client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_another_cluster_is_not_answered_from_the_cache(self):
        # A namespace key is scoped to the cluster which minted it, so a
        # login against one cluster says nothing about another.
        self.cache([self.credential()])

        client = apiclient.Client(base_url=self.OTHER_CLUSTER)

        self.assertEqual(self.OTHER_CLUSTER, client.base_url)
        self.assertIsNone(client.namespace)
        self.assertIsNone(client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_a_half_supplied_credential_is_not_completed_from_the_cache(self):
        # A caller which named a namespace is pinning a credential.
        # Completing the other half of it from a login would present a pair
        # that neither the caller nor the login ever had.
        self.cache([self.credential()])

        client = apiclient.Client(base_url=self.CLUSTER, namespace='flagspace')

        self.assertEqual('flagspace', client.namespace)
        self.assertIsNone(client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_a_single_credential_answers_a_named_cluster_unselected(self):
        # The selection is against another cluster, so it cannot answer for
        # this one -- but one credential for the cluster asked about is not
        # ambiguous, and leaving the caller with no credential at all when
        # there is exactly one would be a surprise and not a safeguard.
        elsewhere = self.credential(apiurl=self.OTHER_CLUSTER,
                                    namespace='other', key_name='cached-bbbb',
                                    key=self.OTHER_CACHED_KEY)
        self.cache([self.credential(), elsewhere],
                   current=self.selection(elsewhere))

        client = apiclient.Client(base_url=self.CLUSTER)

        self.assertEqual('cachedspace', client.namespace)
        self.assertEqual(self.CACHED_KEY, client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_several_credentials_for_a_named_cluster_are_not_guessed_between(self):
        self.cache([self.credential(),
                    self.credential(namespace='other', key_name='cached-bbbb',
                                    key=self.OTHER_CACHED_KEY)],
                   current=None)

        client = apiclient.Client(base_url=self.CLUSTER)

        self.assertIsNone(client.namespace)
        self.assertIsNone(client.key)

    @mock.patch.dict('os.environ', {}, clear=True)
    def test_a_url_only_caller_is_refused_an_unreadable_cache(self):
        # The cache is the only source such a caller has, so an unsafe mode
        # is told to it rather than swallowed: the alternative is a person
        # whose cached keys are readable by every account on the machine
        # never hearing about it.
        self.cache([self.credential()], mode=0o644)

        e = self.assertRaises(credentials.CredentialCacheException,
                              apiclient.Client, base_url=self.CLUSTER)

        self.assertIn(self.cache_path, str(e))


class InstanceCreateBudgetTestCase(testtools.TestCase):
    """The create and the await must not be two budgets on one condition.

    shakenfist/kerbside#355: sf_instance built a client in ASYNC_BLOCK
    mode, so create_instance waited an hour for the instance to leave
    'creating' and then returned it still transitional, and only then did
    await_instance_create start its own 600 second clock. The task took
    the sum, 4200 seconds, and reported the 600.
    """

    def setUp(self):
        super().setUp()

        self.request_url = mock.patch(
            'shakenfist_client.apiclient.Client._request_url')
        self.mock_request = self.request_url.start()
        self.addCleanup(self.request_url.stop)

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

        self.sleep = mock.patch('time.sleep')
        self.mock_sleep = self.sleep.start()
        self.addCleanup(self.sleep.stop)

    def _client(self):
        return apiclient.Client(suppress_configuration_lookup=True,
                                base_url='http://localhost:13000')

    def _creating(self):
        self.mock_request.return_value.json.return_value = {
            'uuid': 'notreallyauuid', 'state': 'creating'}

    def test_zero_timeout_does_not_wait_for_creation(self):
        # What a caller about to await should get: the POST is made, the
        # transitional instance comes straight back, and nothing polls.
        self._creating()
        client = self._client()
        with mock.patch.object(client, 'get_instance') as get_instance:
            i = client.create_instance(
                'foo', 1, 2048, ['netuuid1'], ['8@cirros'], 'sshkey', None,
                timeout=0)

        self.assertEqual('creating', i['state'])
        get_instance.assert_not_called()

    def test_the_post_is_bounded_by_the_same_deadline(self):
        # The dependency retry inside _request_url is the other hour-long
        # blocking region, so the caller's budget has to reach it too.
        self._creating()
        client = self._client()
        client.create_instance(
            'foo', 1, 2048, ['netuuid1'], ['8@cirros'], 'sshkey', None,
            timeout=0)

        _args, kwargs = self.mock_request.call_args
        self.assertLessEqual(kwargs['deadline'], time.time())

    def test_a_default_create_still_waits(self):
        # Callers that do not await themselves rely on this, so the
        # historical behaviour has to survive.
        self._creating()
        client = self._client()
        with mock.patch.object(client, 'get_instance') as get_instance:
            get_instance.return_value = {
                'uuid': 'notreallyauuid', 'state': 'created'}
            i = client.create_instance(
                'foo', 1, 2048, ['netuuid1'], ['8@cirros'], 'sshkey', None)

        self.assertEqual('created', i['state'])
        get_instance.assert_called_once_with('notreallyauuid')

    def test_await_checks_once_before_giving_up(self):
        # With timeout=0 the old loop body never ran, so the error path
        # referenced an unbound name. An instance that is already created
        # is also not a timeout.
        client = self._client()
        with mock.patch.object(client, 'get_instance') as get_instance:
            get_instance.return_value = {
                'uuid': 'notreallyauuid', 'state': 'created'}
            client.await_instance_create('notreallyauuid', timeout=0)

        get_instance.assert_called_once_with('notreallyauuid')

    def test_await_timeout_names_the_last_state(self):
        # "not created within 600 second timeout" alone does not say
        # whether the instance was still creating or never scheduled.
        client = self._client()
        with mock.patch.object(client, 'get_instance') as get_instance:
            get_instance.return_value = {
                'uuid': 'notreallyauuid', 'state': 'creating'}
            e = self.assertRaises(
                apiclient.TimeoutException,
                client.await_instance_create, 'notreallyauuid', timeout=0)

        self.assertIn('creating', str(e))

    def test_await_reports_an_error_state_rather_than_a_timeout(self):
        client = self._client()
        with mock.patch.object(client, 'get_instance') as get_instance:
            get_instance.return_value = {
                'uuid': 'notreallyauuid', 'state': 'creating-error'}
            self.assertRaises(
                apiclient.InstanceWillNeverBeReady,
                client.await_instance_create, 'notreallyauuid', timeout=0)


class RequestDeadlineTestCase(testtools.TestCase):
    """A caller deadline bounds the dependency retry loop."""

    def setUp(self):
        super().setUp()

        self.actual = mock.patch(
            'shakenfist_client.apiclient.Client._actual_request_url')
        self.mock_actual = self.actual.start()
        self.addCleanup(self.actual.stop)

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

        self.sleep = mock.patch('time.sleep')
        self.mock_sleep = self.sleep.start()
        self.addCleanup(self.sleep.stop)

    def _client(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000',
                                  async_strategy=apiclient.ASYNC_BLOCK)
        client.cached_auth = 'Bearer notreallyatoken'
        return client

    def test_an_expired_deadline_does_not_retry(self):
        # Without a caller deadline this loop runs for an hour on
        # ASYNC_BLOCK, inside one call, invisible to the caller's budget.
        self.mock_actual.side_effect = apiclient.DependenciesNotReadyException(
            'not ready', 'POST', '/instances', 406, 'nope')

        client = self._client()
        self.assertRaises(
            apiclient.DependenciesNotReadyException,
            client._request_url, 'POST', '/instances',
            deadline=time.time() - 1)

        self.assertEqual(1, self.mock_actual.call_count)
        self.mock_sleep.assert_not_called()

    def test_a_live_deadline_still_retries(self):
        self.mock_actual.side_effect = [
            apiclient.DependenciesNotReadyException(
                'not ready', 'POST', '/instances', 406, 'nope'),
            'success']

        client = self._client()
        r = client._request_url('POST', '/instances',
                                deadline=time.time() + 60)

        self.assertEqual('success', r)
        self.assertEqual(2, self.mock_actual.call_count)


TRANSIENT_BODY = json.dumps(
    {'error': 'no node had capacity for this instance', 'status': 507,
     'stage': 'capacity_guard', 'transient': True})
DURABLE_BODY = json.dumps(
    {'error': 'no node had capacity for this instance', 'status': 507})


def _refusal(body=TRANSIENT_BODY, headers=None):
    return apiclient.InsufficientResourcesException(
        'API request failed', 'POST', '/instances', 507, body,
        headers=headers or {})


class TransientCapacityRetryTestCase(testtools.TestCase):
    """The opt-in retry of a 507 the server marked as transient."""

    def setUp(self):
        super().setUp()

        self.actual = mock.patch(
            'shakenfist_client.apiclient.Client._actual_request_url')
        self.mock_actual = self.actual.start()
        self.addCleanup(self.actual.stop)

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

        self.sleep = mock.patch('time.sleep')
        self.mock_sleep = self.sleep.start()
        self.addCleanup(self.sleep.stop)

    def _client(self, retry_transient_capacity=False):
        client = apiclient.Client(
            suppress_configuration_lookup=True,
            base_url='http://localhost:13000',
            async_strategy=apiclient.ASYNC_BLOCK,
            retry_transient_capacity=retry_transient_capacity)
        client.cached_auth = 'Bearer notreallyatoken'
        return client

    def _forbid_sleep(self):
        # A test which asserts that no retry happens should fail rather
        # than hang if one does: with time.sleep mocked out, a retry loop
        # never advances the clock towards its deadline.
        self.mock_sleep.side_effect = AssertionError(
            'this refusal should not have been waited out')

    def test_the_flag_defaults_to_off(self):
        self.assertFalse(self._client().retry_transient_capacity)

    def test_off_by_default_a_marked_refusal_raises_immediately(self):
        # A caller which did not ask to wait is told about the refusal
        # straight away, however transient the server says it is.
        self._forbid_sleep()
        self.mock_actual.side_effect = _refusal(
            headers={'Retry-After': '15'})

        client = self._client()
        self.assertRaises(
            apiclient.InsufficientResourcesException,
            client._request_url, 'POST', '/instances',
            deadline=time.time() + 600)

        self.assertEqual(1, self.mock_actual.call_count)
        self.mock_sleep.assert_not_called()

    def test_on_an_unmarked_refusal_raises_immediately(self):
        # An older server's 507 has no marker, and an unmarked 507 is not
        # a promise that waiting will help.
        self._forbid_sleep()
        self.mock_actual.side_effect = _refusal(body=DURABLE_BODY)

        client = self._client(retry_transient_capacity=True)
        self.assertRaises(
            apiclient.InsufficientResourcesException,
            client._request_url, 'POST', '/instances',
            deadline=time.time() + 600)

        self.assertEqual(1, self.mock_actual.call_count)
        self.mock_sleep.assert_not_called()

    def test_on_an_unparseable_body_raises_immediately(self):
        # A proxy's HTML 507 carries no marker either.
        self._forbid_sleep()
        self.mock_actual.side_effect = _refusal(
            body='<html><body>Insufficient Storage</body></html>')

        client = self._client(retry_transient_capacity=True)
        self.assertRaises(
            apiclient.InsufficientResourcesException,
            client._request_url, 'POST', '/instances',
            deadline=time.time() + 600)

        self.assertEqual(1, self.mock_actual.call_count)
        self.mock_sleep.assert_not_called()

    def test_on_a_marked_refusal_retries_and_honours_the_header(self):
        # Seven is deliberately not TRANSIENT_RETRY_DEFAULT, so this
        # assertion can tell the header from the fallback.
        self.mock_actual.side_effect = [
            _refusal(headers={'Retry-After': '7'}), 'success']

        client = self._client(retry_transient_capacity=True)
        r = client._request_url('POST', '/instances',
                                deadline=time.time() + 600)

        self.assertEqual('success', r)
        self.assertEqual(2, self.mock_actual.call_count)
        self.mock_sleep.assert_called_once_with(7)
        self.assertNotEqual(apiclient.TRANSIENT_RETRY_DEFAULT, 7)

    def test_a_missing_header_falls_back_to_the_default(self):
        self.mock_actual.side_effect = [_refusal(), 'success']

        client = self._client(retry_transient_capacity=True)
        self.assertEqual(
            'success',
            client._request_url('POST', '/instances',
                                deadline=time.time() + 600))
        self.mock_sleep.assert_called_once_with(
            apiclient.TRANSIENT_RETRY_DEFAULT)

    # The two loops below name the header value in their assertions rather
    # than wrapping each case in subTest. subTest is inert here:
    # unittest's implementation short circuits when self._outcome is None,
    # and testtools' RunTest replaces run() and never sets it, so a
    # failure would report neither which value broke nor run the
    # remaining cases.
    def _assert_one_sleep(self, expected, value):
        self.assertEqual([mock.call(expected)], self.mock_sleep.call_args_list,
                         'Retry-After: %r' % value)

    def test_a_garbage_header_falls_back_to_the_default(self):
        # Including the RFC 9110 HTTP-date form, which we do not parse.
        for value in ['tomorrow please', '', 'Wed, 21 Oct 2026 07:28:00 GMT',
                      '12.5']:
            self.mock_actual.reset_mock()
            self.mock_sleep.reset_mock()
            self.mock_actual.side_effect = [
                _refusal(headers={'Retry-After': value}), 'success']

            client = self._client(retry_transient_capacity=True)
            self.assertEqual(
                'success',
                client._request_url('POST', '/instances',
                                    deadline=time.time() + 600),
                'Retry-After: %r' % value)
            self._assert_one_sleep(apiclient.TRANSIENT_RETRY_DEFAULT, value)

    def test_a_hostile_header_is_clamped(self):
        for value, expected in [('0', apiclient.TRANSIENT_RETRY_MINIMUM),
                                ('-60', apiclient.TRANSIENT_RETRY_MINIMUM),
                                ('86400', apiclient.TRANSIENT_RETRY_MAXIMUM)]:
            self.mock_actual.reset_mock()
            self.mock_sleep.reset_mock()
            self.mock_actual.side_effect = [
                _refusal(headers={'Retry-After': value}), 'success']

            client = self._client(retry_transient_capacity=True)
            self.assertEqual(
                'success',
                client._request_url('POST', '/instances',
                                    deadline=time.time() + 600),
                'Retry-After: %r' % value)
            self._assert_one_sleep(expected, value)

    def test_an_expired_deadline_does_not_retry(self):
        self._forbid_sleep()
        self.mock_actual.side_effect = _refusal(
            headers={'Retry-After': '15'})

        client = self._client(retry_transient_capacity=True)
        self.assertRaises(
            apiclient.InsufficientResourcesException,
            client._request_url, 'POST', '/instances',
            deadline=time.time() - 1)

        self.assertEqual(1, self.mock_actual.call_count)
        self.mock_sleep.assert_not_called()

    def test_the_sleep_is_clamped_to_the_remaining_deadline(self):
        # Unlike the 406 clause, a 507 wait never overshoots the budget.
        self.mock_actual.side_effect = [
            _refusal(headers={'Retry-After': '30'}), 'success']

        client = self._client(retry_transient_capacity=True)
        self.assertEqual(
            'success',
            client._request_url('POST', '/instances',
                                deadline=time.time() + 5))

        slept = self.mock_sleep.call_args[0][0]
        self.assertGreater(slept, 4)
        self.assertLessEqual(slept, 5)

    def _never_stops(self, deadline):
        clock = [1000.0]
        self.mock_actual.side_effect = _refusal()
        self.mock_sleep.side_effect = lambda seconds: clock.__setitem__(
            0, clock[0] + seconds)

        client = self._client(retry_transient_capacity=True)
        with mock.patch('time.time', lambda: clock[0]):
            self.assertRaises(
                apiclient.InsufficientResourcesException,
                client._request_url, 'POST', '/instances', deadline=deadline)

    def test_the_deadline_bounds_a_refusal_which_never_stops(self):
        # A 45 second budget and a 15 second wait each time: three waits,
        # and then the fourth refusal is raised rather than waited out.
        # Deliberately short of TRANSIENT_RETRY_MAXIMUM_ATTEMPTS, so that
        # this asserts the deadline and its sibling below asserts the cap,
        # rather than both passing on whichever bound is tighter.
        self._never_stops(1045.0)

        self.assertEqual(3, self.mock_sleep.call_count)
        self.assertEqual(4, self.mock_actual.call_count)
        self.assertGreater(apiclient.TRANSIENT_RETRY_MAXIMUM_ATTEMPTS, 4)

    def test_the_attempt_cap_bounds_a_refusal_which_never_stops(self):
        # An hour of ASYNC_BLOCK is 240 fifteen second waits, and each
        # replay costs the cluster a discarded instance record, so the
        # attempt cap and not the deadline is what stops this.
        self._never_stops(1000.0 + 3600)

        self.assertEqual(apiclient.TRANSIENT_RETRY_MAXIMUM_ATTEMPTS,
                         self.mock_actual.call_count)
        self.assertEqual(apiclient.TRANSIENT_RETRY_MAXIMUM_ATTEMPTS - 1,
                         self.mock_sleep.call_count)

    def test_an_async_continue_client_never_retries(self):
        # _calculate_async_deadline(ASYNC_CONTINUE) is -1, so a caller
        # which passes no deadline of its own has one already in the past.
        # That is the documented behaviour, not an accident.
        self._forbid_sleep()
        self.mock_actual.side_effect = _refusal(
            headers={'Retry-After': '15'})

        client = apiclient.Client(
            suppress_configuration_lookup=True,
            base_url='http://localhost:13000',
            async_strategy=apiclient.ASYNC_CONTINUE,
            retry_transient_capacity=True)
        client.cached_auth = 'Bearer notreallyatoken'

        self.assertRaises(
            apiclient.InsufficientResourcesException,
            client._request_url, 'POST', '/instances')

        self.assertEqual(1, self.mock_actual.call_count)
        self.mock_sleep.assert_not_called()

    def _instance(self, state):
        response = mock.MagicMock()
        response.json.return_value = {'uuid': 'notreallyauuid',
                                      'state': state}
        return response

    def test_create_instance_retries_a_transient_refusal(self):
        # The end to end path, rather than _request_url() directly: a
        # refused create is waited out and the instance comes back.
        # test_the_post_is_bounded_by_the_same_deadline pins which budget
        # that spends.
        self.mock_actual.side_effect = [_refusal(), self._instance('created')]

        client = self._client(retry_transient_capacity=True)
        i = client.create_instance(
            'foo', 1, 2048, ['netuuid1'], ['8@cirros'], 'sshkey', None,
            timeout=600)

        self.assertEqual('created', i['state'])
        self.assertEqual(2, self.mock_actual.call_count)
        self.mock_sleep.assert_called_once_with(
            apiclient.TRANSIENT_RETRY_DEFAULT)

    def test_create_instance_spends_one_budget_on_both_halves(self):
        # The waiting out and the wait for creation share create_instance's
        # single deadline, so a refusal which eats the budget leaves the
        # await with nothing and the caller gets a transitional instance
        # back rather than an exception. Documented in
        # docs/transient-capacity-retry.md; asserted here because it is
        # the surprising half of enabling the flag.
        clock = [1000.0]
        self.mock_sleep.side_effect = lambda seconds: clock.__setitem__(
            0, clock[0] + seconds)
        self.mock_actual.side_effect = [_refusal(), self._instance('creating')]

        client = self._client(retry_transient_capacity=True)
        with mock.patch('time.time', lambda: clock[0]):
            with mock.patch.object(client, 'get_instance') as get_instance:
                get_instance.return_value = {'uuid': 'notreallyauuid',
                                             'state': 'creating'}
                i = client.create_instance(
                    'foo', 1, 2048, ['netuuid1'], ['8@cirros'], 'sshkey',
                    None, timeout=apiclient.TRANSIENT_RETRY_DEFAULT)

        self.assertEqual('creating', i['state'])
        self.assertEqual(2, self.mock_actual.call_count)
        # The state alone proves nothing -- an await with a budget of its
        # own would return a transitional instance too, just 600 seconds
        # later. What says the budget was shared is that the whole call
        # ended within it: one 15 second wait, then a single poll.
        self.assertEqual(1, get_instance.call_count)
        self.assertLessEqual(clock[0], 1000.0 +
                             apiclient.TRANSIENT_RETRY_DEFAULT + 1)


# A structurally valid JWT: three base64url segments, the header being
# base64url('{"alg":"none"}'). The redaction keys off the shape of the
# string rather than where it came from, so a real shape matters.
JWT = ('eyJhbGciOiJub25lIn0.'
       'eyJzdWIiOiJpbnN0YW5jZSIsImV4cCI6MTc2NzIyNTYwMH0.'
       'c2lnbmF0dXJlLWdvZXMtaGVyZQ')
PROXY_URL = 'https://kerbside.example.com/vdi/console.vv?token=%s' % JWT


class RedactTokensTestCase(testtools.TestCase):
    def test_jwt_in_url_is_redacted(self):
        redacted = apiclient.redact_tokens('fetching ' + PROXY_URL)
        self.assertNotIn(JWT, redacted)
        # The rest of the URL survives, because knowing which host was
        # called is the reason the text is being rendered at all.
        self.assertIn('kerbside.example.com', redacted)

    def test_bare_jwt_is_redacted(self):
        self.assertEqual('*****', apiclient.redact_tokens(JWT))

    def test_hostname_is_not_redacted(self):
        # Three dotted segments of base64url characters, which a shape
        # test looser than "starts with eyJ" would eat.
        self.assertEqual(
            'mycluster.mycompany.internal',
            apiclient.redact_tokens('mycluster.mycompany.internal'))

    def test_text_without_a_token_is_unchanged(self):
        self.assertEqual(
            'nothing here', apiclient.redact_tokens('nothing here'))


class VDIConsoleProxyFileTestCase(testtools.TestCase):
    """The proxy URL is a credential, so nothing may render it whole.

    requests builds its exception messages out of the URL it was handed,
    and main.py's logging filter never sees an exception the interpreter
    prints as a traceback. So these check the exception, not the logs.
    """

    def setUp(self):
        super().setUp()

        # Client.__init__ calls this, and with base_url pointing at a host
        # which does not resolve it raises. Every other class here patches
        # it; this one used to run on a patch another class had leaked,
        # because its addCleanup stopped a MagicMock rather than the
        # patcher, so these tests only passed when something else ran
        # first in the same worker.
        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

    def _client(self):
        return apiclient.Client(suppress_configuration_lookup=True,
                                base_url='http://sf/api')

    def _raising(self, exception):
        client = self._client()
        with mock.patch.object(client, 'get_vdi_console_proxy',
                               return_value={'url': PROXY_URL}):
            with mock.patch('requests.get', side_effect=exception):
                return self.assertRaises(
                    type(exception), client.get_vdi_console_proxy_file,
                    'inst-ref')

    def test_http_error_message_is_redacted(self):
        # What raise_for_status() raises: the message is built as
        # '<code> Client Error: <reason> for url: <url>'.
        raised = self._raising(requests.exceptions.HTTPError(
            '403 Client Error: Forbidden for url: %s' % PROXY_URL))
        self.assertNotIn(JWT, str(raised))
        self.assertIn('403 Client Error', str(raised))

    def test_connection_error_message_is_redacted(self):
        raised = self._raising(requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='kerbside.example.com', port=443): "
            'Max retries exceeded with url: /vdi/console.vv?token=%s' % JWT))
        self.assertNotIn(JWT, str(raised))

    def test_the_exception_class_is_preserved(self):
        # Callers catch requests' own classes, so redaction must not
        # change what an except clause matches.
        raised = self._raising(requests.exceptions.Timeout(
            'timed out for url: %s' % PROXY_URL))
        self.assertIsInstance(raised, requests.exceptions.Timeout)

    def test_the_unredacted_original_is_not_chained(self):
        # Without "from None" the interpreter prints the original as the
        # context of the replacement, token and all.
        raised = self._raising(requests.exceptions.HTTPError(
            '403 Client Error: Forbidden for url: %s' % PROXY_URL))
        self.assertTrue(raised.__suppress_context__)

    def test_a_successful_fetch_returns_the_body(self):
        client = self._client()
        response = mock.Mock()
        response.text = '[virt-viewer]\n'
        with mock.patch.object(client, 'get_vdi_console_proxy',
                               return_value={'url': PROXY_URL}):
            with mock.patch('requests.get', return_value=response):
                self.assertEqual(
                    '[virt-viewer]\n',
                    client.get_vdi_console_proxy_file('inst-ref'))


class BaseUrlRedirectTestCase(testtools.TestCase):
    """A deployment whose API root answers 301.

    These clusters are supported, and the redirection is resolved once at
    construction. What is under test is not the rewrite itself so much as
    that it happens before any route is built, because the two public login
    routes send no credential and so can never resolve it as a side effect
    of authenticating.

    The consequence if they could not: requests downgrades a redirected
    POST to a GET and discards the body, and GET /auth/login is the public
    issuer list while POST /auth/login is the mint. A mint sent to the
    pre-redirect URL therefore does not fail -- it succeeds as a different
    request, answering with the issuer list, having never sent the identity
    token anywhere. The person is told their credentials could not be
    saved, after authenticating at their provider.

    _collect_capabilities is deliberately not patched out here. It is the
    thing under test.
    """

    REDIRECTED_FROM = 'http://sf.example.com/api'
    REDIRECTED_TO = 'http://sf.example.com:13000/api'

    ROOT_HTML = ('<html><body><p>Machine searchable API capabilities:</p><ul>'
                 '<li>auth: human-login-metadata, human-login</li>'
                 '</ul></body></html>')

    def _response(self, redirected, body, json_body=None):
        hop = mock.MagicMock()
        hop.status_code = 301
        hop.url = self.REDIRECTED_FROM
        hop.headers = {'Location': self.REDIRECTED_TO}

        response = mock.MagicMock()
        response.status_code = 200
        response.history = [hop] if redirected else []
        response.headers = {}
        response.text = body
        response.json.return_value = json_body
        return response

    def _client(self, redirected=True, json_body=None):
        """A client constructed against a root which redirects, or does not.

        One mocked session answers every request, so what each call asked
        for is readable from its mock afterwards.
        """
        session = mock.MagicMock()
        session.request.return_value = self._response(
            redirected, self.ROOT_HTML, json_body=json_body)

        with mock.patch('shakenfist_client.apiclient.requests.Session',
                        return_value=session):
            client = apiclient.Client(suppress_configuration_lookup=True,
                                      base_url=self.REDIRECTED_FROM)
        return client, session

    def test_a_redirecting_root_is_resolved_at_construction(self):
        client, session = self._client()

        self.assertEqual(self.REDIRECTED_TO, client.base_url)

    def test_a_root_which_does_not_redirect_is_left_alone(self):
        # The negative control: the rewrite is driven by the response's
        # history, so a client which was never redirected must keep the URL
        # it was given.
        client, session = self._client(redirected=False)

        self.assertEqual(self.REDIRECTED_FROM, client.base_url)

    def test_the_root_page_is_still_read(self):
        # Resolving the redirection must not cost the capability list, which
        # is read from the same response.
        client, session = self._client()

        self.assertTrue(client.check_capability('human-login'))

    def test_the_mint_posts_to_the_resolved_url(self):
        # The regression this class exists for. Were the POST sent to
        # REDIRECTED_FROM it would arrive as a GET of the issuer list.
        client, session = self._client(json_body=[])

        client.login('notarealtoken')

        self.assertEqual(
            self.REDIRECTED_TO + '/auth/login',
            session.request.call_args.args[1])
        self.assertEqual('POST', session.request.call_args.args[0])

    def test_the_issuer_list_reads_the_resolved_url(self):
        client, session = self._client(json_body=[])

        client.get_login_issuers()

        self.assertEqual(
            self.REDIRECTED_TO + '/auth/login',
            session.request.call_args.args[1])
        self.assertEqual('GET', session.request.call_args.args[0])

    def test_an_authenticated_route_reads_the_resolved_url(self):
        # Every other route resolved this for itself on its first
        # unauthenticated request. It must still end up at the same place
        # now that the probe it used to do has gone.
        client, session = self._client(json_body={'instances': []})
        client.cached_auth = 'Bearer notreallyatoken'

        client.get_instances()

        self.assertEqual(
            self.REDIRECTED_TO + '/instances',
            session.request.call_args.args[1])

    def test_a_302_does_not_move_the_cluster(self):
        # A temporary redirect says nothing about where this cluster lives,
        # so it must not be written down as though it did.
        session = mock.MagicMock()
        response = self._response(True, self.ROOT_HTML)
        response.history[0].status_code = 302
        session.request.return_value = response

        with mock.patch('shakenfist_client.apiclient.requests.Session',
                        return_value=session):
            client = apiclient.Client(suppress_configuration_lookup=True,
                                      base_url=self.REDIRECTED_FROM)

        self.assertEqual(self.REDIRECTED_FROM, client.base_url)

    def _client_redirected_to(self, location):
        """A client whose root answered 301 with this Location."""
        session = mock.MagicMock()
        response = self._response(True, self.ROOT_HTML, json_body=[])
        response.history[0].headers = {'Location': location}
        session.request.return_value = response

        with mock.patch('shakenfist_client.apiclient.requests.Session',
                        return_value=session):
            client = apiclient.Client(suppress_configuration_lookup=True,
                                      base_url=self.REDIRECTED_FROM)
        return client, session

    def test_a_relative_location_is_resolved_against_the_hop(self):
        # RFC 9110 permits a relative Location. Taken as it stands it
        # would leave base_url as '/api' and every request built from it
        # would be a path with no host at all -- and since this is now the
        # single place the redirect is resolved for every route, including
        # the mint, there is no later probe to recover from it.
        client, _ = self._client_redirected_to('/elsewhere/api')

        self.assertEqual('http://sf.example.com/elsewhere/api',
                         client.base_url)

    def test_a_trailing_slash_does_not_double_up(self):
        # Every caller appends a path which begins with a slash.
        client, session = self._client_redirected_to(
            'http://sf.example.com:13000/api/')

        self.assertEqual('http://sf.example.com:13000/api', client.base_url)

        client.get_login_issuers()

        self.assertEqual('http://sf.example.com:13000/api/auth/login',
                         session.request.call_args.args[1])


class StatusCodeMappingTestCase(testtools.TestCase):
    """Statuses which carry a meaning get an exception which carries it too.

    503 matters for the namespace claims API, which answers it for both of
    its retryable refusals -- capacity accounting not built yet, and a claim
    row which kept moving under a concurrent writer. A caller which cannot
    tell that from a durable refusal either retries what it should not, or
    gives up on what it should have retried.
    """

    def setUp(self):
        super().setUp()

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

    def _client(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.cached_auth = 'Bearer notreallyatoken'
        return client

    def _respond(self, client, status_code, headers=None):
        response = mock.MagicMock()
        response.status_code = status_code
        response.text = '{"error": "nope"}'
        # A real dict, not the MagicMock attribute: an assertion about
        # header propagation made against a MagicMock passes whether or
        # not the headers were propagated at all.
        response.headers = headers if headers is not None else {}
        client.session = mock.MagicMock()
        client.session.request.return_value = response

    def test_503_raises_service_unavailable(self):
        client = self._client()
        self._respond(client, 503)

        e = self.assertRaises(
            apiclient.ServiceUnavailableException,
            client._actual_request_url, 'POST', '/auth/namespaces/ns/claims')
        self.assertEqual(503, e.status_code)

    def test_507_still_raises_insufficient_resources(self):
        client = self._client()
        self._respond(client, 507)

        self.assertRaises(
            apiclient.InsufficientResourcesException,
            client._actual_request_url, 'POST', '/auth/namespaces/ns/claims')

    def test_an_unmapped_status_still_raises_the_base_exception(self):
        client = self._client()
        self._respond(client, 418)

        e = self.assertRaises(
            apiclient.APIException,
            client._actual_request_url, 'POST', '/auth/namespaces/ns/claims')
        self.assertEqual(418, e.status_code)

    # The three raise sites which must carry the response headers. Every
    # test in TransientCapacityRetryTestCase fabricates its own exception,
    # so without these the wiring the whole retry rests on is untested:
    # dropping headers=r.headers would leave that class green and make
    # every refusal fall back to TRANSIENT_RETRY_DEFAULT.
    def test_a_mapped_status_carries_the_response_headers(self):
        client = self._client()
        self._respond(client, 507, headers={'Retry-After': '7',
                                            'X-Request-ID': 'abc'})

        e = self.assertRaises(
            apiclient.InsufficientResourcesException,
            client._actual_request_url, 'POST', '/instances')
        self.assertEqual('7', e.headers['Retry-After'])

    def test_an_unmapped_status_carries_the_response_headers(self):
        client = self._client()
        self._respond(client, 418, headers={'Retry-After': '7'})

        e = self.assertRaises(
            apiclient.APIException,
            client._actual_request_url, 'POST', '/instances')
        self.assertEqual('7', e.headers['Retry-After'])

    def test_an_authentication_failure_carries_the_response_headers(self):
        # _authenticate() has its own raise site, and it calls
        # requests.request() directly rather than going through a session.
        client = self._client()
        response = mock.MagicMock()
        response.status_code = 401
        response.text = '{"error": "nope"}'
        response.headers = {'X-Request-ID': 'abc'}

        with mock.patch('shakenfist_client.apiclient.requests.request',
                        return_value=response):
            e = self.assertRaises(apiclient.UnauthenticatedException,
                                  client._authenticate)
        self.assertEqual('abc', e.headers['X-Request-ID'])


class _FakeClock:
    """A monotonically advancing stand-in for ``time.time``.

    A fixed ``side_effect`` list breaks with ``StopIteration`` the moment
    the code under test gains or loses a ``time.time()`` call, which is a
    failure that says nothing about the behaviour being tested. This lets a
    test move the clock explicitly, at the points where the thing it is
    simulating would really have taken time, and read it as often as it
    likes.
    """

    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds

    def advancing(self, seconds, result=None):
        """A side_effect which advances the clock and returns ``result``."""
        def _side_effect(*args, **kwargs):
            self.advance(seconds)
            return result
        return _side_effect


class AgentOperationAwaitTestCase(testtools.TestCase):
    """The three agent operation await loops must fail fast on a terminal
    state rather than spinning out their budget first.

    client-python#363: an operation that lands in 'error', 'expired' or
    'deleted' used to be indistinguishable from one still in flight, so a
    caller waited out the whole timeout only to be told it timed out.
    """

    TERMINAL_FAILURE_STATES = ('error', 'expired', 'abort', 'deleted')

    def setUp(self):
        super().setUp()

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

        self.sleep = mock.patch('time.sleep')
        self.mock_sleep = self.sleep.start()
        self.addCleanup(self.sleep.stop)

    def _client(self, **kwargs):
        return apiclient.Client(suppress_configuration_lookup=True,
                                base_url='http://localhost:13000', **kwargs)

    # -- _await_agentop ----------------------------------------------

    def test_await_agentop_complete_returns_the_operation(self):
        client = self._client()
        op = {'uuid': 'op1', 'state': 'complete'}
        with mock.patch.object(client, 'get_agent_operation') as get_op:
            result = client._await_agentop(dict(op))

        self.assertEqual(op, result)
        get_op.assert_not_called()

    def test_abort_is_a_terminal_state(self):
        self.assertIn('abort', apiclient.TERMINAL_AGENT_OPERATION_STATES)

    def test_await_agentop_terminal_failure_raises_on_first_poll(self):
        for state in self.TERMINAL_FAILURE_STATES:
            client = self._client()
            op = {'uuid': 'op1', 'state': state}
            with mock.patch.object(client, 'get_agent_operation') as get_op:
                e = self.assertRaises(
                    apiclient.AgentOperationFailed,
                    client._await_agentop, dict(op))

            get_op.assert_not_called()
            self.assertEqual('op1', e.op_uuid)
            self.assertEqual(state, e.op_view['state'])
            self.assertIn(state, str(e))

    def test_await_agentop_async_continue_returns_in_flight_operation(self):
        # ASYNC_CONTINUE's deadline is already in the past when the loop
        # starts, so a caller who is not really waiting gets the
        # in-flight operation back -- this is the existing fire-and-forget
        # behaviour, not the "budget ran out" case AgentAwaitTimeout
        # describes, so it must not start raising here.
        client = self._client(async_strategy=apiclient.ASYNC_CONTINUE)
        op = {'uuid': 'op1', 'state': 'queued'}
        with mock.patch.object(client, 'get_agent_operation') as get_op:
            result = client._await_agentop(dict(op))

        self.assertEqual(op, result)
        get_op.assert_not_called()

    def test_await_agentop_async_continue_terminal_failure_still_raises(self):
        # The other half of the fire-and-forget contract, and the reason
        # the terminal check deliberately precedes the deadline check.
        # "Do not wait" is not "do not notice": an operation which is
        # already dead in the POST response cannot usefully be handed back
        # for the caller to poll later, so even ASYNC_CONTINUE raises.
        for state in self.TERMINAL_FAILURE_STATES:
            client = self._client(async_strategy=apiclient.ASYNC_CONTINUE)
            op = {'uuid': 'op1', 'state': state}
            with mock.patch.object(client, 'get_agent_operation') as get_op:
                e = self.assertRaises(
                    apiclient.AgentOperationFailed,
                    client._await_agentop, dict(op))

            get_op.assert_not_called()
            self.assertEqual('op1', e.op_uuid)
            self.assertEqual(state, e.op_view['state'])

    def test_await_agentop_await_seconds_bounds_the_wait(self):
        # await_seconds is how long *this client* waits, which is not the
        # same number as the deadline the operation was created with. A
        # caller that asked to wait ten seconds must not be held for the
        # hour ASYNC_BLOCK would otherwise allow: it is the caller's budget
        # that bounds the poll, not this client's general blocking policy.
        clock = _FakeClock()
        self.mock_sleep.side_effect = lambda seconds: clock.advance(seconds)
        client = self._client(async_strategy=apiclient.ASYNC_BLOCK)
        op = {'uuid': 'op1', 'state': 'queued'}
        with mock.patch('time.time', clock), \
                mock.patch.object(
                    client, 'get_agent_operation',
                    return_value=dict(op)) as get_op:
            result = client._await_agentop(dict(op), await_seconds=10)

        # Still in flight, returned rather than raised -- see the comment in
        # _await_agentop about why the timeout path does not raise here.
        self.assertEqual(op, result)
        # One poll per second of the ten we asked for, give or take the
        # boundary check. Unbounded, this would be ASYNC_BLOCK's 3600.
        self.assertGreater(get_op.call_count, 1)
        self.assertLess(get_op.call_count, 15)

    # -- await_agent_command -------------------------------------------

    def test_await_agent_command_complete_returns_normally(self):
        client = self._client()
        op = {'uuid': 'op1', 'state': 'complete',
              'results': {'0': {'return-code': 0, 'stderr': '', 'stdout': 'hi'}}}
        with mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(client, 'instance_execute', return_value=dict(op)), \
                mock.patch.object(client, 'get_agent_operation') as get_op:
            result = client.await_agent_command('notreallyauuid', 'true')

        self.assertEqual((0, 'hi'), result)
        get_op.assert_not_called()

    def test_await_agent_command_terminal_failure_raises_on_first_poll(self):
        # Only _request_url and get_agent_operation are mocked, so
        # instance_execute() and _await_agentop() both really run. A mocked
        # instance_execute returning a terminal state would prove nothing:
        # the real chain can no longer produce that, because _await_agentop
        # raises before it returns.
        for state in self.TERMINAL_FAILURE_STATES:
            client = self._client()
            client.root_html = 'instance-execute agentoperation-deadlines'
            with mock.patch.object(client, '_request_url') as request_url, \
                    mock.patch.object(client, 'await_agent_ready'), \
                    mock.patch.object(client, 'get_agent_operation') as get_op, \
                    mock.patch.object(
                        client, 'get_instance',
                        return_value={'uuid': 'notreallyauuid', 'agent_state': 'ready'}), \
                    mock.patch.object(client, 'get_console_data', return_value='console'):
                request_url.return_value.json.return_value = {
                    'uuid': 'op1', 'state': state}
                e = self.assertRaises(
                    apiclient.AgentOperationFailed,
                    client.await_agent_command, 'notreallyauuid', 'true')

            get_op.assert_not_called()
            self.assertEqual('op1', e.op_uuid)
            self.assertEqual(state, e.op_view['state'])
            self.assertIn(state, str(e))

    def test_await_agent_command_never_settling_raises_timeout(self):
        # timeout=0 means the operation-wait loop is not entered at all,
        # so this proves the exception without waiting out any budget.
        client = self._client()
        op = {'uuid': 'op1', 'state': 'queued'}
        with mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(client, 'instance_execute', return_value=dict(op)), \
                mock.patch.object(client, 'get_agent_operation') as get_op, \
                mock.patch.object(
                    client, 'get_instance',
                    return_value={'uuid': 'notreallyauuid', 'agent_state': 'ready'}), \
                mock.patch.object(client, 'get_console_data', return_value='console'):
            self.assertRaises(
                apiclient.AgentAwaitTimeout,
                client.await_agent_command, 'notreallyauuid', 'true', timeout=0)

        get_op.assert_not_called()

    def test_await_agent_command_passes_its_budget_as_deadline_and_wait(self):
        # await_agent_command's own budget (its timeout argument) is what it
        # hands instance_execute, both as the server side deadline and as
        # the client side wait -- nothing is derived from the async
        # strategy, and _await_agentop must not poll for longer than the
        # caller asked to wait.
        clock = _FakeClock()
        client = self._client()
        op = {'uuid': 'op1', 'state': 'complete',
              'results': {'0': {'return-code': 0, 'stderr': '', 'stdout': 'hi'}}}
        with mock.patch('time.time', clock), \
                mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(
                    client, 'instance_execute', return_value=dict(op)) as instance_execute, \
                mock.patch.object(client, 'get_agent_operation'):
            client.await_agent_command('notreallyauuid', 'true', timeout=42)

        instance_execute.assert_called_with(
            'notreallyauuid', 'true', deadline_seconds=42, await_seconds=42)

    def test_await_agent_command_deadline_is_the_budget_ready_left_behind(self):
        # await_agent_ready() shares start_time with the loops below, so the
        # deadline sent must be what is *left* of the timeout once the agent
        # became ready. Sending the full 120 here would keep the operation
        # alive on the server for 120 seconds after this call, which gives
        # up in 30, had already stopped caring about it.
        clock = _FakeClock()
        client = self._client()
        op = {'uuid': 'op1', 'state': 'complete',
              'results': {'0': {'return-code': 0, 'stderr': '', 'stdout': 'hi'}}}
        with mock.patch('time.time', clock), \
                mock.patch.object(
                    client, 'await_agent_ready', side_effect=clock.advancing(90)), \
                mock.patch.object(
                    client, 'instance_execute', return_value=dict(op)) as instance_execute, \
                mock.patch.object(client, 'get_agent_operation'):
            client.await_agent_command('notreallyauuid', 'true', timeout=120)

        instance_execute.assert_called_with(
            'notreallyauuid', 'true', deadline_seconds=30, await_seconds=30)

    def test_await_agent_command_exhausted_budget_sends_one_not_zero(self):
        # The server reads a deadline of 0 as "no wall clock deadline at
        # all", so a budget which has already run out must never be spelt
        # that way -- doing so would create an unbounded operation at
        # precisely the moment we have no time left to watch it.
        clock = _FakeClock()
        client = self._client()
        op = {'uuid': 'op1', 'state': 'queued'}
        with mock.patch('time.time', clock), \
                mock.patch.object(
                    client, 'await_agent_ready', side_effect=clock.advancing(500)), \
                mock.patch.object(
                    client, 'instance_execute', return_value=dict(op)) as instance_execute, \
                mock.patch.object(client, 'get_agent_operation'), \
                mock.patch.object(
                    client, 'get_instance',
                    return_value={'uuid': 'notreallyauuid', 'agent_state': 'ready'}), \
                mock.patch.object(client, 'get_console_data', return_value='console'):
            self.assertRaises(
                apiclient.AgentAwaitTimeout,
                client.await_agent_command, 'notreallyauuid', 'true', timeout=120)

        self.assertEqual(
            1, instance_execute.call_args.kwargs['deadline_seconds'])

    def test_await_agent_command_terminal_failure_carries_console_data(self):
        # The console data enrichment has to survive the real call chain,
        # not just a mocked instance_execute: _await_agentop() raises
        # AgentOperationFailed as soon as it polls a terminal state, which
        # is the common case for a command that fails promptly, and the
        # operator wants the console exactly then. Only _request_url and
        # get_agent_operation are mocked here, so instance_execute and
        # _await_agentop both really run.
        client = self._client()
        client.root_html = 'instance-execute agentoperation-deadlines'
        with mock.patch.object(client, '_request_url') as request_url, \
                mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(client, 'get_agent_operation') as get_op, \
                mock.patch.object(
                    client, 'get_instance',
                    return_value={'uuid': 'notreallyauuid',
                                  'agent_state': 'ready'}), \
                mock.patch.object(
                    client, 'get_console_data', return_value='panic: oops'):
            request_url.return_value.json.return_value = {
                'uuid': 'op1', 'state': 'error'}
            e = self.assertRaises(
                apiclient.AgentOperationFailed,
                client.await_agent_command, 'notreallyauuid', 'true')

        get_op.assert_not_called()
        self.assertEqual('op1', e.op_uuid)
        self.assertEqual('error', e.op_view['state'])
        self.assertIn('panic: oops', str(e))
        self.assertIn('Agent state: ready', str(e))

    def test_await_agent_command_empty_results_raises_command_error(self):
        # The guard has to precede the subscripts it protects. It used to
        # sit below `op['results']['0']['return-code']`, which made it
        # unreachable: an operation which completed with an empty results
        # dict raised KeyError instead of saying what had gone wrong.
        clock = _FakeClock()
        client = self._client()
        op = {'uuid': 'op1', 'state': 'complete', 'results': {}}
        with mock.patch('time.time', clock), \
                mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(client, 'instance_execute', return_value=dict(op)), \
                mock.patch.object(
                    client, 'get_agent_operation',
                    side_effect=clock.advancing(300, dict(op))):
            e = self.assertRaises(
                apiclient.AgentCommandError,
                client.await_agent_command, 'notreallyauuid', 'true')

        self.assertIn('operation returned no results', str(e))

    # -- await_agent_fetch --------------------------------------------

    def test_await_agent_fetch_complete_returns_normally(self):
        client = self._client()
        op = {'uuid': 'op1', 'state': 'complete',
              'results': {'0': {'content_blob': 'blob1'}}}
        with mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(client, 'instance_get', return_value=dict(op)), \
                mock.patch.object(client, 'get_agent_operation') as get_op, \
                mock.patch.object(client, 'get_blob', return_value={'state': 'created'}), \
                mock.patch.object(client, 'get_blob_data', return_value=[b'hello']):
            data = client.await_agent_fetch('notreallyauuid', '/tmp/f')

        self.assertEqual('hello', data)
        get_op.assert_not_called()

    def test_await_agent_fetch_terminal_failure_raises_on_first_poll(self):
        # The real chain again -- see the await_agent_command equivalent.
        for state in self.TERMINAL_FAILURE_STATES:
            client = self._client()
            client.root_html = 'instance-get agentoperation-deadlines'
            with mock.patch.object(client, '_request_url') as request_url, \
                    mock.patch.object(client, 'await_agent_ready'), \
                    mock.patch.object(client, 'get_agent_operation') as get_op, \
                    mock.patch.object(
                        client, 'get_instance',
                        return_value={'uuid': 'notreallyauuid', 'agent_state': 'ready'}), \
                    mock.patch.object(client, 'get_console_data', return_value='console'):
                request_url.return_value.json.return_value = {
                    'uuid': 'op1', 'state': state}
                e = self.assertRaises(
                    apiclient.AgentOperationFailed,
                    client.await_agent_fetch, 'notreallyauuid', '/tmp/f')

            get_op.assert_not_called()
            self.assertEqual('op1', e.op_uuid)
            self.assertEqual(state, e.op_view['state'])
            self.assertIn(state, str(e))

    def test_await_agent_fetch_never_settling_raises_timeout(self):
        # await_agent_fetch's operation-wait loop bounds itself by `timeout`
        # (the default of 120 seconds here, since none is passed), so the
        # clock is moved past that budget rather than waited out. Creating
        # the operation is what burns the time, which is also the only
        # honest way to spend it while `instance_get` is mocked.
        clock = _FakeClock()
        client = self._client()
        op = {'uuid': 'op1', 'state': 'queued'}
        with mock.patch('time.time', clock), \
                mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(
                    client, 'instance_get',
                    side_effect=clock.advancing(300, dict(op))), \
                mock.patch.object(client, 'get_agent_operation') as get_op:
            self.assertRaises(
                apiclient.AgentAwaitTimeout,
                client.await_agent_fetch, 'notreallyauuid', '/tmp/f')

        get_op.assert_not_called()

    def test_await_agent_fetch_passes_its_budget_as_deadline_and_wait(self):
        # await_agent_fetch's own budget (its timeout argument) is what it
        # hands instance_get, as both the server side deadline and the
        # client side wait.
        clock = _FakeClock()
        client = self._client()
        op = {'uuid': 'op1', 'state': 'complete',
              'results': {'0': {'content_blob': 'blob1'}}}
        with mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(
                    client, 'instance_get', return_value=dict(op)) as instance_get, \
                mock.patch.object(client, 'get_agent_operation'), \
                mock.patch.object(client, 'get_blob', return_value={'state': 'created'}), \
                mock.patch.object(client, 'get_blob_data', return_value=[b'hello']), \
                mock.patch('time.time', clock):
            client.await_agent_fetch('notreallyauuid', '/tmp/f', timeout=42)

        instance_get.assert_called_with(
            'notreallyauuid', '/tmp/f', deadline_seconds=42, await_seconds=42)

    def test_await_agent_fetch_slow_operation_still_reaches_results(self):
        # Decision 6 / survey finding 5: every loop in this method must
        # share the same `timeout` budget, measured from the same
        # start_time. Before that repair, the results-wait and blob-wait
        # loops were hardcoded to 60 seconds from start_time regardless
        # of `timeout`, so an operation that took longer than a minute
        # to reach 'complete' entered those loops with their window
        # already expired -- even though the default `timeout` of 120
        # seconds had plenty of budget left. Here the operation reaches
        # 'complete' 90 seconds in (comfortably inside the 120 second
        # default) with its results still empty, and only fills them in
        # on a later poll. Against the pre-fix code this raises
        # AgentCommandError('operation returned no results'), because the
        # second loop's hardcoded `< 60` bound is already false by the
        # time it is reached; against the fix it keeps polling and returns
        # the fetched data. The real instance_get() runs, so the 90
        # seconds are spent where they really would be -- inside
        # _await_agentop()'s poll -- rather than inside a mock. The clock
        # advances where the simulated work happens rather than once per
        # read, so an added or removed time.time() call cannot turn this
        # into a StopIteration.
        clock = _FakeClock()
        client = self._client()
        client.root_html = 'instance-get agentoperation-deadlines'
        op_executing = {'uuid': 'op1', 'state': 'executing', 'results': {}}
        op_complete_no_results = {'uuid': 'op1', 'state': 'complete', 'results': {}}
        op_complete_with_results = {
            'uuid': 'op1', 'state': 'complete',
            'results': {'0': {'content_blob': 'blob1'}}}

        polls = [dict(op_complete_no_results), dict(op_complete_with_results)]

        def poll(_op_uuid):
            if len(polls) == 2:
                # The operation itself takes 90 seconds to settle.
                clock.advance(90)
            return polls.pop(0)

        with mock.patch('time.time', clock), \
                mock.patch.object(client, '_request_url') as request_url, \
                mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(
                    client, 'get_agent_operation', side_effect=poll) as get_op, \
                mock.patch.object(client, 'get_blob', return_value={'state': 'created'}), \
                mock.patch.object(client, 'get_blob_data', return_value=[b'hello']):
            request_url.return_value.json.return_value = dict(op_executing)
            data = client.await_agent_fetch('notreallyauuid', '/tmp/f')

        self.assertEqual('hello', data)
        self.assertEqual(2, get_op.call_count)

    # -- _enriched_agent_failure ---------------------------------------

    def test_enriched_failure_survives_a_vanished_instance(self):
        # The enrichment is best effort; the report of the failure is not.
        # A "deleted" operation is often an instance which went away
        # underneath it, so get_instance() fails for the same reason the
        # operation did -- and a ResourceNotFoundException raised while
        # decorating a failure must not replace the failure, taking the
        # operation uuid and state with it.
        client = self._client()
        op = {'uuid': 'op1', 'state': 'deleted'}
        with mock.patch.object(
                client, 'get_instance',
                side_effect=apiclient.ResourceNotFoundException(
                    'API request failed', 'GET',
                    'http://localhost:13000/instances/notreallyauuid', 404,
                    '{"error": "instance not found"}')), \
                mock.patch.object(client, 'get_console_data') as console:
            e = client._enriched_agent_failure('fetch', op, 'notreallyauuid')

        self.assertIsInstance(e, apiclient.AgentOperationFailed)
        self.assertEqual('op1', e.op_uuid)
        self.assertEqual('deleted', e.op_view['state'])
        self.assertIn('notreallyauuid', str(e))
        self.assertIn('deleted', str(e))
        self.assertIn('Console data: could not be gathered', str(e))
        console.assert_not_called()

    def test_enriched_failure_survives_unavailable_console_data(self):
        client = self._client()
        op = {'uuid': 'op1', 'state': 'error'}
        with mock.patch.object(
                client, 'get_instance',
                return_value={'uuid': 'notreallyauuid',
                              'agent_state': 'ready'}), \
                mock.patch.object(
                    client, 'get_console_data',
                    side_effect=apiclient.ResourceNotFoundException(
                        'API request failed', 'GET',
                        'http://localhost:13000/instances/notreallyauuid/'
                        'consoledata', 404, '{"error": "no console"}')):
            e = client._enriched_agent_failure('execute', op, 'notreallyauuid')

        self.assertIsInstance(e, apiclient.AgentOperationFailed)
        self.assertEqual('op1', e.op_uuid)
        self.assertEqual('error', e.op_view['state'])
        self.assertIn('Console data: could not be gathered', str(e))

    def test_a_vanished_instance_does_not_mask_the_failure_for_a_caller(self):
        # The same thing again through the whole chain, because this is
        # the shape an operator actually meets: the operation fails, the
        # instance is gone, and what must come back is still
        # AgentOperationFailed rather than the 404 from the decoration.
        client = self._client()
        client.root_html = 'instance-get agentoperation-deadlines'
        with mock.patch.object(client, '_request_url') as request_url, \
                mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(
                    client, 'get_instance',
                    side_effect=apiclient.ResourceNotFoundException(
                        'API request failed', 'GET',
                        'http://localhost:13000/instances/notreallyauuid', 404,
                        '{"error": "instance not found"}')):
            request_url.return_value.json.return_value = {
                'uuid': 'op1', 'state': 'deleted'}
            e = self.assertRaises(
                apiclient.AgentOperationFailed,
                client.await_agent_fetch, 'notreallyauuid', '/tmp/f')

        self.assertEqual('op1', e.op_uuid)
        self.assertEqual('deleted', e.op_view['state'])

    def test_a_vanished_instance_does_not_mask_a_timeout(self):
        # The timeout report gets the same treatment as the failure
        # report, and for the same reason: the console data is a
        # decoration, the timeout is the news, and an instance which went
        # away underneath a slow operation breaks the decoration first.
        client = self._client()
        with mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(
                    client, 'instance_execute',
                    return_value={'uuid': 'op1', 'state': 'queued'}), \
                mock.patch.object(client, 'get_agent_operation') as get_op, \
                mock.patch.object(
                    client, 'get_instance',
                    side_effect=apiclient.ResourceNotFoundException(
                        'API request failed', 'GET',
                        'http://localhost:13000/instances/notreallyauuid', 404,
                        '{"error": "instance not found"}')):
            e = self.assertRaises(
                apiclient.AgentAwaitTimeout,
                client.await_agent_command, 'notreallyauuid', 'true', timeout=0)

        get_op.assert_not_called()
        self.assertIn('notreallyauuid', str(e))
        self.assertIn('could not be gathered', str(e))

    # -- await_seconds forwarding --------------------------------------

    def _assert_await_seconds_is_forwarded(self, call):
        """The helper must hand await_seconds to _await_agentop, not drop it.

        A helper which silently dropped the kwarg would pass every
        propagation test above -- they only look at the request body -- while
        restoring the bug it exists to fix: ASYNC_BLOCK blocking for an hour
        inside a call which asked to wait five seconds.
        """
        clock = _FakeClock()
        self.mock_sleep.side_effect = lambda seconds: clock.advance(seconds)
        client = self._client(async_strategy=apiclient.ASYNC_BLOCK)
        client.root_html = ('instance-put-blob instance-execute instance-get '
                            'agentoperation-deadlines')
        with mock.patch('time.time', clock), \
                mock.patch.object(client, '_request_url') as request_url, \
                mock.patch.object(
                    client, 'get_agent_operation',
                    return_value={'uuid': 'op1', 'state': 'queued'}) as get_op:
            request_url.return_value.json.return_value = {
                'uuid': 'op1', 'state': 'queued'}
            result = call(client)

        self.assertEqual('queued', result['state'])
        # One poll per second of the five asked for, give or take the
        # boundary check. Unbounded, this would be ASYNC_BLOCK's 3600.
        self.assertGreater(get_op.call_count, 1)
        self.assertLess(get_op.call_count, 10)

    def test_put_blob_forwards_await_seconds(self):
        self._assert_await_seconds_is_forwarded(
            lambda client: client.instance_put_blob(
                'notreallyauuid', 'blob1', '/tmp/f', 0o644, await_seconds=5))

    def test_execute_forwards_await_seconds(self):
        self._assert_await_seconds_is_forwarded(
            lambda client: client.instance_execute(
                'notreallyauuid', 'true', await_seconds=5))

    def test_get_forwards_await_seconds(self):
        self._assert_await_seconds_is_forwarded(
            lambda client: client.instance_get(
                'notreallyauuid', '/tmp/f', await_seconds=5))


class AgentOperationDeadlineTestCase(testtools.TestCase):
    """The three agent-operation creating helpers propagate a deadline
    (and, for put/get, a progress timeout) to the server -- but only once
    the server has advertised it can accept them via the
    'agentoperation-deadlines' capability token (decision 2 and 7 of
    PLAN-agent-operation-deadlines-phase-06-client.md).
    """

    def setUp(self):
        super().setUp()

        self.request_url = mock.patch(
            'shakenfist_client.apiclient.Client._request_url')
        self.mock_request = self.request_url.start()
        self.addCleanup(self.request_url.stop)
        self.mock_request.return_value.json.return_value = {
            'uuid': 'op1', 'state': 'complete'}

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

    def _client(self, capable, **kwargs):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000', **kwargs)
        # check_capability() is a substring match against the server's root
        # HTML page (apiclient.py:check_capability), so this is the same
        # thing a real server response looks like to the client.
        client.root_html = 'agentoperation-deadlines' if capable else ''
        client.root_html += ' instance-put-blob instance-execute instance-get'
        return client

    def _sent_data(self):
        return self.mock_request.call_args.kwargs['data']

    # -- instance_put_blob ----------------------------------------------

    def test_put_blob_sends_both_when_capable(self):
        client = self._client(True)
        client.instance_put_blob('inst1', 'blob1', '/path', 0o644,
                                 deadline_seconds=42, progress_timeout_seconds=7)

        self.assertEqual(
            {'blob_uuid': 'blob1', 'path': '/path', 'mode': 0o644,
             'deadline_seconds': 42, 'progress_timeout_seconds': 7},
            self._sent_data())

    def test_put_blob_sends_neither_when_not_capable(self):
        client = self._client(False)
        client.instance_put_blob('inst1', 'blob1', '/path', 0o644,
                                 deadline_seconds=42, progress_timeout_seconds=7)

        self.assertEqual(
            {'blob_uuid': 'blob1', 'path': '/path', 'mode': 0o644},
            self._sent_data())

    def test_put_blob_omitted_deadline_sends_nothing(self):
        # Nothing is derived from the async strategy. That strategy says how
        # long this client will block; the deadline says how long the
        # operation may live on the server. Deriving one from the other gave
        # every CLI upload a 60 second server side kill under the default
        # ASYNC_PAUSE, in place of the server's own, far longer, default.
        client = self._client(True, async_strategy=apiclient.ASYNC_PAUSE)
        client.instance_put_blob('inst1', 'blob1', '/path', 0o644)

        self.assertEqual(
            {'blob_uuid': 'blob1', 'path': '/path', 'mode': 0o644},
            self._sent_data())

    def test_put_blob_async_block_omitted_deadline_sends_nothing(self):
        client = self._client(True, async_strategy=apiclient.ASYNC_BLOCK)
        client.instance_put_blob('inst1', 'blob1', '/path', 0o644)

        self.assertNotIn('deadline_seconds', self._sent_data())

    def test_put_blob_sends_zero_deadline(self):
        # Zero means "no wall clock deadline at all" to the server, which is
        # not the same as omitting the key. The propagation must test
        # `is not None`, never truthiness, or --deadline 0 silently becomes
        # the server default.
        client = self._client(True)
        client.instance_put_blob('inst1', 'blob1', '/path', 0o644,
                                 deadline_seconds=0, progress_timeout_seconds=0)

        sent = self._sent_data()
        self.assertEqual(0, sent['deadline_seconds'])
        self.assertEqual(0, sent['progress_timeout_seconds'])

    # -- instance_execute -------------------------------------------------

    def test_execute_sends_deadline_when_capable(self):
        client = self._client(True)
        client.instance_execute('inst1', 'true', deadline_seconds=42)

        self.assertEqual(
            {'command_line': 'true', 'deadline_seconds': 42}, self._sent_data())

    def test_execute_sends_nothing_when_not_capable(self):
        client = self._client(False)
        client.instance_execute('inst1', 'true', deadline_seconds=42)

        self.assertEqual({'command_line': 'true'}, self._sent_data())

    def test_execute_omitted_deadline_sends_nothing(self):
        client = self._client(True, async_strategy=apiclient.ASYNC_BLOCK)
        client.instance_execute('inst1', 'true')

        self.assertEqual({'command_line': 'true'}, self._sent_data())

    def test_execute_sends_zero_deadline(self):
        client = self._client(True)
        client.instance_execute('inst1', 'true', deadline_seconds=0)

        self.assertEqual(0, self._sent_data()['deadline_seconds'])

    def test_execute_has_no_progress_timeout_kwarg(self):
        # The server refuses a progress timeout on execute (decision 4;
        # shakenfist/external_api/instance.py) -- instance_execute must
        # not grow a way to send one, even if a caller asks for it.
        client = self._client(True)
        self.assertRaises(
            TypeError, client.instance_execute, 'inst1', 'true',
            progress_timeout_seconds=7)

    # -- instance_get -----------------------------------------------------

    def test_get_sends_both_when_capable(self):
        client = self._client(True)
        client.instance_get('inst1', '/path',
                            deadline_seconds=42, progress_timeout_seconds=7)

        self.assertEqual(
            {'path': '/path', 'deadline_seconds': 42, 'progress_timeout_seconds': 7},
            self._sent_data())

    def test_get_sends_neither_when_not_capable(self):
        client = self._client(False)
        client.instance_get('inst1', '/path',
                            deadline_seconds=42, progress_timeout_seconds=7)

        self.assertEqual({'path': '/path'}, self._sent_data())

    def test_get_omitted_deadline_sends_nothing(self):
        client = self._client(True, async_strategy=apiclient.ASYNC_PAUSE)
        client.instance_get('inst1', '/path')

        self.assertEqual({'path': '/path'}, self._sent_data())

    def test_get_sends_zero_progress_timeout(self):
        client = self._client(True)
        client.instance_get('inst1', '/path', progress_timeout_seconds=0)

        sent = self._sent_data()
        self.assertEqual(0, sent['progress_timeout_seconds'])
        self.assertNotIn('deadline_seconds', sent)


class APIExceptionTestCase(testtools.TestCase):
    # Downstream repositories construct APIException with five positional
    # arguments and no headers, so that form must keep working even though
    # this phase adds a headers keyword. See shakenfist's
    # docs/plans/PLAN-transient-capacity-refusals-phase-04-retry-after.md,
    # step 4d.
    def test_old_five_positional_form_still_works(self):
        exc = apiclient.APIException('m', 'GET', 'u', 507, 't')

        self.assertEqual('m', exc.message)
        self.assertEqual('GET', exc.method)
        self.assertEqual('u', exc.url)
        self.assertEqual(507, exc.status_code)
        self.assertEqual('t', exc.text)
        self.assertEqual({}, exc.headers)

    def test_headers_keyword_is_stored(self):
        headers = {'Retry-After': '15'}
        exc = apiclient.APIException('m', 'GET', 'u', 507, 't', headers=headers)

        self.assertEqual(headers, exc.headers)

    def test_headers_defaults_to_empty_dict_not_none(self):
        exc = apiclient.APIException('m', 'GET', 'u', 507, 't', headers=None)

        self.assertEqual({}, exc.headers)


class AgentDependencyTestCase(testtools.TestCase):
    OP_UUID = '4b7f2f96-1b93-4f4c-9b0e-0f19d1a6f2a1'

    def test_operation_view_becomes_an_edge(self):
        op = {'uuid': self.OP_UUID, 'state': 'complete'}

        self.assertEqual({'op_uuid': self.OP_UUID},
                         apiclient.agent_dependency(op))

    def test_uuid_string_becomes_an_edge(self):
        self.assertEqual({'op_uuid': self.OP_UUID},
                         apiclient.agent_dependency(self.OP_UUID))

    def test_op_type_is_never_emitted(self):
        # The server stores op_type as a constant it does not publish, and
        # the parameter it validates against refuses a key it does not
        # publish, so emitting one here would have the edge rejected.
        edge = apiclient.agent_dependency(self.OP_UUID)

        self.assertEqual(['op_uuid'], sorted(edge.keys()))

    def test_settle_seconds_is_included_when_passed(self):
        edge = apiclient.agent_dependency(self.OP_UUID, settle_seconds=30)

        self.assertEqual({'op_uuid': self.OP_UUID, 'settle_seconds': 30}, edge)

    def test_settle_seconds_is_absent_when_not_passed(self):
        # An absent key, not an explicit null: it is the server's business
        # what "no settle" means.
        self.assertNotIn('settle_seconds',
                         apiclient.agent_dependency(self.OP_UUID))

    def test_a_zero_settle_is_still_emitted(self):
        edge = apiclient.agent_dependency(self.OP_UUID, settle_seconds=0)

        self.assertEqual(0, edge['settle_seconds'])

    def test_an_operation_without_a_uuid_is_refused(self):
        # Rather than building an edge which names the string 'None' and
        # leaving the cluster to refuse it a request later.
        e = self.assertRaises(
            ValueError, apiclient.agent_dependency, {'state': 'complete'})

        self.assertIn('has no uuid', str(e))

    def test_something_which_is_not_a_uuid_is_refused(self):
        e = self.assertRaises(
            ValueError, apiclient.agent_dependency, 'the-first-one')

        self.assertIn('is not a valid UUID', str(e))

    def test_an_upper_case_uuid_is_canonicalised(self):
        # The server parses the UUID and hands it back in lower case, and a
        # creating call checks the edges it sent against the ones which came
        # back, so the two have to be the same string.
        edge = apiclient.agent_dependency(self.OP_UUID.upper())

        self.assertEqual({'op_uuid': self.OP_UUID}, edge)


class AgentDependencyEdgesTestCase(testtools.TestCase):
    OP_UUID = '4b7f2f96-1b93-4f4c-9b0e-0f19d1a6f2a1'
    OTHER_UUID = 'c0a8cbd6-3b02-4a0e-9e5a-8df0b0f8a991'

    def test_uuid_string_members_are_normalised(self):
        self.assertEqual(
            [{'op_uuid': self.OP_UUID}, {'op_uuid': self.OTHER_UUID}],
            apiclient._agentop_dependency_edges(
                'depends_on', [self.OP_UUID, self.OTHER_UUID]))

    def test_mapping_members_are_passed_through(self):
        edges = [{'op_uuid': self.OP_UUID, 'settle_seconds': 5}]

        self.assertEqual(edges,
                         apiclient._agentop_dependency_edges('depends_on', edges))

    def test_mapping_members_are_copied_not_mutated(self):
        edge = {'op_uuid': self.OP_UUID}
        result = apiclient._agentop_dependency_edges('runs_after', [edge])

        self.assertIsNot(edge, result[0])

    def test_an_empty_list_normalises_to_an_empty_list(self):
        self.assertEqual([], apiclient._agentop_dependency_edges('depends_on', []))

    def test_member_of_the_wrong_type_is_refused(self):
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'depends_on', [self.OP_UUID, 42])

        self.assertIn('depends_on[1]', str(e))
        self.assertIn('must be an agent operation UUID or a dependency edge', str(e))
        self.assertIn('int', str(e))

    def test_unknown_key_is_refused(self):
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'runs_after', [{'op_uuid': self.OP_UUID, 'op_type': 'agentoperation'}])

        self.assertIn('runs_after[0]', str(e))
        self.assertIn('not part of a dependency edge', str(e))
        self.assertIn('op_type', str(e))

    def test_missing_op_uuid_is_refused(self):
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'depends_on', [{'settle_seconds': 10}])

        self.assertIn('depends_on[0]', str(e))
        self.assertIn('does not name an agent operation', str(e))

    def test_null_op_uuid_is_refused_as_missing(self):
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'depends_on', [{'op_uuid': None}])

        self.assertIn('depends_on[0]', str(e))
        self.assertIn('does not name an agent operation', str(e))

    def test_non_uuid_string_member_is_refused(self):
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'runs_after', [self.OP_UUID, 'not-a-uuid'])

        self.assertIn('runs_after[1]', str(e))
        self.assertIn('is not a valid UUID', str(e))

    def test_non_uuid_op_uuid_in_a_mapping_is_refused(self):
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'depends_on', [{'op_uuid': 'not-a-uuid'}])

        self.assertIn('depends_on[0]', str(e))
        self.assertIn('is not a valid UUID', str(e))

    def test_an_empty_op_uuid_is_refused(self):
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'depends_on', [{'op_uuid': ''}])

        self.assertIn('depends_on[0]', str(e))
        self.assertIn('is not a valid UUID', str(e))

    def test_a_bare_uuid_instead_of_a_list_is_refused(self):
        # depends_on=op['uuid'] is the obvious mistake, and it is iterable,
        # so without a guard it is walked character by character and the
        # complaint is about the character '4'.
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'depends_on', self.OP_UUID)

        self.assertIn('must be a list of dependency edges', str(e))
        self.assertIn('not a single str', str(e))

    def test_a_single_edge_instead_of_a_list_is_refused(self):
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'runs_after', {'op_uuid': self.OP_UUID})

        self.assertIn('must be a list of dependency edges', str(e))
        self.assertIn('not a single dict', str(e))

    def test_a_boolean_settle_is_refused(self):
        # isinstance(True, int) is true in Python, so a bool would
        # otherwise arrive at the cluster as one second.
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'depends_on', [{'op_uuid': self.OP_UUID, 'settle_seconds': True}])

        self.assertIn('not a boolean', str(e))

    def test_a_non_numeric_settle_is_refused(self):
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'depends_on', [{'op_uuid': self.OP_UUID, 'settle_seconds': 'soon'}])

        self.assertIn('must be a number of seconds', str(e))

    def test_a_negative_settle_is_refused(self):
        # A settle pointing backwards would make the edge satisfied before
        # the thing it waits for finished. Whether a settle is a number and
        # which way it points are properties of the value; how large it may
        # be is the deployment's business and is still not checked here.
        e = self.assertRaises(
            ValueError, apiclient._agentop_dependency_edges,
            'depends_on', [{'op_uuid': self.OP_UUID, 'settle_seconds': -1}])

        self.assertIn('may not be negative', str(e))

    def test_an_upper_case_member_is_canonicalised(self):
        edges = apiclient._agentop_dependency_edges(
            'depends_on', [self.OP_UUID.upper()])

        self.assertEqual([{'op_uuid': self.OP_UUID}], edges)

    # The next two assert acceptances rather than refusals, which is the
    # point of them. How many edges an operation may have and how long a
    # settle may be are both deployment policy the server reads from its
    # configuration, and the settle ceiling has already moved by two orders
    # of magnitude in a single release. A client which refused either value
    # would be wrong about a cluster configured differently, so these tests
    # pin the absence of those bounds here.
    def test_a_large_settle_seconds_is_accepted(self):
        edges = apiclient._agentop_dependency_edges(
            'depends_on', [{'op_uuid': self.OP_UUID, 'settle_seconds': 86400}])

        self.assertEqual(86400, edges[0]['settle_seconds'])

    def test_a_long_list_of_edges_is_accepted(self):
        edges = apiclient._agentop_dependency_edges(
            'depends_on', [apiclient.agent_dependency(str(uuid.uuid4()))
                           for _ in range(100)])

        self.assertEqual(100, len(edges))


def _echoing_agentop_response(method, url, data=None, **kwargs):
    """Answer a creating POST the way a healthy cluster does.

    The response to one of the three agent operation creating calls is the
    created operation's own view, and a cluster whose database tier
    understands dependency edges hands back the ones it stored. So an
    operation created with edges must come back carrying them, or the
    client refuses it -- which means a test about anything else has to
    answer like a working cluster rather than with a bare operation view.
    """
    op = {'uuid': 'op1', 'state': 'complete'}
    for name in ('depends_on', 'runs_after'):
        if name in (data or {}):
            op[name] = data[name]

    r = mock.MagicMock()
    r.json.return_value = op
    return r


class AgentOperationDependencyParameterTestCase(testtools.TestCase):
    """The three agent operation creating helpers send dependency edges.

    They send them only when the caller declared some, and refuse to run at
    all when the caller declared some and the server has not advertised the
    'agentoperation-dependencies' capability. The refusal is the point: a
    dropped ordering edge would be reported to the caller as a success.
    """

    OP_UUID = '4b7f2f96-1b93-4f4c-9b0e-0f19d1a6f2a1'
    OTHER_UUID = 'c0a8cbd6-3b02-4a0e-9e5a-8df0b0f8a991'

    PUT_BODY = {'blob_uuid': 'blob1', 'path': '/path', 'mode': 0o644}
    EXECUTE_BODY = {'command_line': 'true'}
    GET_BODY = {'path': '/path'}

    def setUp(self):
        super().setUp()

        self.request_url = mock.patch(
            'shakenfist_client.apiclient.Client._request_url')
        self.mock_request = self.request_url.start()
        self.addCleanup(self.request_url.stop)
        self.mock_request.side_effect = _echoing_agentop_response

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

    def _client(self, capable):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        # check_capability() is a substring match against the server's root
        # HTML page, so this is what a real server response looks like to
        # the client.
        client.root_html = 'instance-put-blob instance-execute instance-get'
        if capable:
            client.root_html += ' agentoperation-dependencies'
        return client

    def _sent_data(self):
        return self.mock_request.call_args.kwargs['data']

    # The three calls under test, each with the request body it builds
    # before any dependency key is added.

    def _put(self, client, **kwargs):
        return client.instance_put_blob('inst1', 'blob1', '/path', 0o644, **kwargs)

    def _execute(self, client, **kwargs):
        return client.instance_execute('inst1', 'true', **kwargs)

    def _get(self, client, **kwargs):
        return client.instance_get('inst1', '/path', **kwargs)

    # -- the edges are sent ----------------------------------------------

    def _assert_sends_edges(self, call, body):
        client = self._client(True)
        call(client,
             depends_on=[self.OP_UUID],
             runs_after=[apiclient.agent_dependency(self.OTHER_UUID,
                                                    settle_seconds=5)])

        # A bare UUID string was normalised into an edge, so the list really
        # went through _agentop_dependency_edges() rather than being passed
        # to the server as the caller typed it.
        self.assertEqual(
            dict(body,
                 depends_on=[{'op_uuid': self.OP_UUID}],
                 runs_after=[{'op_uuid': self.OTHER_UUID, 'settle_seconds': 5}]),
            self._sent_data())

    def test_put_blob_sends_edges(self):
        self._assert_sends_edges(self._put, self.PUT_BODY)

    def test_execute_sends_edges(self):
        self._assert_sends_edges(self._execute, self.EXECUTE_BODY)

    def test_get_sends_edges(self):
        self._assert_sends_edges(self._get, self.GET_BODY)

    def _assert_sends_one_list_alone(self, call, body):
        # The list the caller did not pass must not appear at all. An empty
        # list is not the same request as an absent key.
        client = self._client(True)
        call(client, depends_on=[self.OP_UUID])

        self.assertEqual(
            dict(body, depends_on=[{'op_uuid': self.OP_UUID}]), self._sent_data())

    def test_put_blob_sends_depends_on_alone(self):
        self._assert_sends_one_list_alone(self._put, self.PUT_BODY)

    def test_execute_sends_depends_on_alone(self):
        self._assert_sends_one_list_alone(self._execute, self.EXECUTE_BODY)

    def test_get_sends_depends_on_alone(self):
        self._assert_sends_one_list_alone(self._get, self.GET_BODY)

    # -- an incapable server is refused -----------------------------------

    def _assert_refuses_an_incapable_server(self, call):
        client = self._client(False)

        for kwargs in ({'depends_on': [self.OP_UUID]},
                       {'runs_after': [self.OP_UUID]}):
            e = self.assertRaises(apiclient.IncapableException,
                                  call, client, **kwargs)

            self.assertIn('agentoperation-dependencies', str(e))
            # Refused before the request went out, not after an operation
            # was created which would have run unordered.
            self.mock_request.assert_not_called()

    def test_put_blob_refuses_an_incapable_server(self):
        self._assert_refuses_an_incapable_server(self._put)

    def test_execute_refuses_an_incapable_server(self):
        self._assert_refuses_an_incapable_server(self._execute)

    def test_get_refuses_an_incapable_server(self):
        self._assert_refuses_an_incapable_server(self._get)

    # -- a caller who declared nothing is unaffected ----------------------

    # Each of these drives an *incapable* client deliberately: if the
    # capability were consulted the call would raise, so these also prove
    # the check is never reached.

    def _assert_no_arguments_send_no_keys(self, call, body):
        client = self._client(False)
        call(client)

        self.assertEqual(body, self._sent_data())

    def test_put_blob_without_edges_sends_no_keys(self):
        self._assert_no_arguments_send_no_keys(self._put, self.PUT_BODY)

    def test_execute_without_edges_sends_no_keys(self):
        self._assert_no_arguments_send_no_keys(self._execute, self.EXECUTE_BODY)

    def test_get_without_edges_sends_no_keys(self):
        self._assert_no_arguments_send_no_keys(self._get, self.GET_BODY)

    def _assert_explicit_none_sends_no_keys(self, call, body):
        # A wrapper forwarding **kwargs passes None for an edge list it does
        # not use, and that must behave exactly as not passing it at all.
        client = self._client(False)
        call(client, depends_on=None, runs_after=None)

        self.assertEqual(body, self._sent_data())

    def test_put_blob_explicit_none_sends_no_keys(self):
        self._assert_explicit_none_sends_no_keys(self._put, self.PUT_BODY)

    def test_execute_explicit_none_sends_no_keys(self):
        self._assert_explicit_none_sends_no_keys(self._execute, self.EXECUTE_BODY)

    def test_get_explicit_none_sends_no_keys(self):
        self._assert_explicit_none_sends_no_keys(self._get, self.GET_BODY)

    def _assert_empty_lists_send_no_keys(self, call, body):
        # An empty list is a caller who declared no edges, which is not a
        # caller who needs ordering. Refusing it would be a refusal with no
        # cause, and sending an empty list would say "I checked, there is
        # nothing to wait for" to a server which cannot read the key.
        client = self._client(False)
        call(client, depends_on=[], runs_after=[])

        self.assertEqual(body, self._sent_data())

    def test_put_blob_empty_lists_send_no_keys(self):
        self._assert_empty_lists_send_no_keys(self._put, self.PUT_BODY)

    def test_execute_empty_lists_send_no_keys(self):
        self._assert_empty_lists_send_no_keys(self._execute, self.EXECUTE_BODY)

    def test_get_empty_lists_send_no_keys(self):
        self._assert_empty_lists_send_no_keys(self._get, self.GET_BODY)

    # -- a malformed edge is refused before the request -------------------

    def _assert_a_malformed_edge_is_refused(self, call):
        client = self._client(True)

        e = self.assertRaises(ValueError, call, client,
                              depends_on=[{'op_uuid': 'not-a-uuid'}])

        self.assertIn('depends_on[0]', str(e))
        self.mock_request.assert_not_called()

    def test_put_blob_refuses_a_malformed_edge(self):
        self._assert_a_malformed_edge_is_refused(self._put)

    def test_execute_refuses_a_malformed_edge(self):
        self._assert_a_malformed_edge_is_refused(self._execute)

    def test_get_refuses_a_malformed_edge(self):
        self._assert_a_malformed_edge_is_refused(self._get)


class AgentOperationStoredEdgeTestCase(testtools.TestCase):
    """A creating call checks that the edges it sent came back stored.

    The agentoperation-dependencies capability is advertised by sf-api,
    which is not the service that writes an edge. During a staged upgrade
    a new sf-api in front of an older sf-database accepts the parameters,
    answers 200, and stores empty edge lists -- so an operation which the
    caller ordered runs unordered and is reported as a success. The
    created operation's view is the POST response, so the client catches
    that without another request.
    """

    OP_UUID = '4b7f2f96-1b93-4f4c-9b0e-0f19d1a6f2a1'
    OTHER_UUID = 'c0a8cbd6-3b02-4a0e-9e5a-8df0b0f8a991'

    def setUp(self):
        super().setUp()

        self.request_url = mock.patch(
            'shakenfist_client.apiclient.Client._request_url')
        self.mock_request = self.request_url.start()
        self.addCleanup(self.request_url.stop)

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

        self.client = apiclient.Client(suppress_configuration_lookup=True,
                                       base_url='http://localhost:13000')
        # check_capability() is a substring match against the server's root
        # HTML page, and this cluster advertises dependencies -- which is
        # exactly the case where the database tier may still not store them.
        self.client.root_html = ('instance-put-blob instance-execute instance-get '
                                 'agentoperation-dependencies')

    def _answers(self, view):
        """Make the creating POST answer with this operation view."""
        self.mock_request.return_value.json.return_value = dict(
            view, uuid='op1', state='complete')

    # The three calls under test.

    def _put(self, **kwargs):
        return self.client.instance_put_blob('inst1', 'blob1', '/path', 0o644, **kwargs)

    def _execute(self, **kwargs):
        return self.client.instance_execute('inst1', 'true', **kwargs)

    def _get(self, **kwargs):
        return self.client.instance_get('inst1', '/path', **kwargs)

    # -- a cluster which stored the edges is accepted ----------------------

    def _assert_echoed_edges_pass(self, call):
        self._answers({'depends_on': [{'op_uuid': self.OP_UUID}],
                       'runs_after': [{'op_uuid': self.OTHER_UUID}]})

        op = call(depends_on=[self.OP_UUID], runs_after=[self.OTHER_UUID])

        self.assertEqual('op1', op['uuid'])

    def test_put_blob_accepts_echoed_edges(self):
        self._assert_echoed_edges_pass(self._put)

    def test_execute_accepts_echoed_edges(self):
        self._assert_echoed_edges_pass(self._execute)

    def test_get_accepts_echoed_edges(self):
        self._assert_echoed_edges_pass(self._get)

    # -- the three shapes an older database tier produces ------------------

    def _assert_refused(self, call, view):
        self._answers(view)

        e = self.assertRaises(apiclient.IncapableException, call,
                              depends_on=[self.OP_UUID])

        # The operator reading this needs to know which component to
        # upgrade, and that the call they made was not the mistake.
        self.assertIn('sf-database', str(e))
        self.assertIn('Upgrade sf-database', str(e))
        self.assertIn('depends_on', str(e))
        self.assertIn(self.OP_UUID, str(e))
        return e

    def _assert_empty_list_refused(self, call):
        # What an older sf-database actually produces: the fields reach it,
        # are unknown to it, and are dropped, so the view carries the key
        # with nothing in it.
        self._assert_refused(call, {'depends_on': [], 'runs_after': []})

    def test_put_blob_refuses_empty_edge_lists(self):
        self._assert_empty_list_refused(self._put)

    def test_execute_refuses_empty_edge_lists(self):
        self._assert_empty_list_refused(self._execute)

    def test_get_refuses_empty_edge_lists(self):
        self._assert_empty_list_refused(self._get)

    def _assert_subset_refused(self, call):
        self._answers({'depends_on': [{'op_uuid': self.OP_UUID}]})

        e = self.assertRaises(apiclient.IncapableException, call,
                              depends_on=[self.OP_UUID, self.OTHER_UUID])

        # Partly stored is not stored: the edge which went missing is the
        # one whose ordering will not be honoured.
        self.assertIn('sf-database', str(e))
        self.assertIn(self.OTHER_UUID, str(e))

    def test_put_blob_refuses_a_subset_of_the_edges(self):
        self._assert_subset_refused(self._put)

    def test_execute_refuses_a_subset_of_the_edges(self):
        self._assert_subset_refused(self._execute)

    def test_get_refuses_a_subset_of_the_edges(self):
        self._assert_subset_refused(self._get)

    def _assert_missing_key_refused(self, call):
        # An older service's view may not carry the field at all, which is
        # not distinguishable from an edge list it threw away.
        self._assert_refused(call, {})

    def test_put_blob_refuses_a_view_without_the_key(self):
        self._assert_missing_key_refused(self._put)

    def test_execute_refuses_a_view_without_the_key(self):
        self._assert_missing_key_refused(self._execute)

    def test_get_refuses_a_view_without_the_key(self):
        self._assert_missing_key_refused(self._get)

    # -- what is deliberately not compared --------------------------------

    def test_a_dropped_settle_seconds_is_not_a_mismatch(self):
        # Only the op_uuid sets are compared. A tier which does not
        # understand an edge drops the whole edge, so a settle which did
        # not come back says nothing about storage, and refusing on it
        # would refuse a healthy cluster.
        self._answers({'depends_on': [{'op_uuid': self.OP_UUID}]})

        op = self._execute(depends_on=[apiclient.agent_dependency(self.OP_UUID,
                                                                  settle_seconds=5)])

        self.assertEqual('op1', op['uuid'])

    def test_reordered_edges_are_not_a_mismatch(self):
        # The server does not promise to return the edges in the order they
        # were sent, so the comparison is between sets.
        self._answers({'depends_on': [{'op_uuid': self.OTHER_UUID},
                                      {'op_uuid': self.OP_UUID}]})

        op = self._execute(depends_on=[self.OP_UUID, self.OTHER_UUID])

        self.assertEqual('op1', op['uuid'])

    def test_an_upper_case_uuid_is_not_a_mismatch(self):
        # The edge was canonicalised when it was built and the server's view
        # is canonical too, which is why the comparison needs no
        # normalisation of its own.
        self._answers({'depends_on': [{'op_uuid': self.OP_UUID}]})

        op = self._execute(depends_on=[self.OP_UUID.upper()])

        self.assertEqual('op1', op['uuid'])

    # -- a caller who sent no edges reaches no comparison ------------------

    def _assert_no_edges_ignores_the_response(self, call):
        # Whatever the view carries on an operation the caller did not order
        # is not this client's business: an unrelated edge in the response,
        # or none at all, must not raise.
        self._answers({'depends_on': [{'op_uuid': self.OTHER_UUID}]})

        op = call()

        self.assertEqual('op1', op['uuid'])

    def test_put_blob_without_edges_ignores_the_response(self):
        self._assert_no_edges_ignores_the_response(self._put)

    def test_execute_without_edges_ignores_the_response(self):
        self._assert_no_edges_ignores_the_response(self._execute)

    def test_get_without_edges_ignores_the_response(self):
        self._assert_no_edges_ignores_the_response(self._get)

    def test_edges_echoed_as_bare_uuids_are_not_a_mismatch(self):
        # The server sends each edge as a mapping, because it stores the
        # schema model dumped to JSON. Reading a surprising shape must not
        # raise from inside the check whose job is to report a surprising
        # answer.
        self.mock_request.return_value.json.return_value = {
            'uuid': 'op1', 'state': 'complete', 'depends_on': [self.OP_UUID]}

        op = self._execute(depends_on=[self.OP_UUID])

        self.assertEqual('op1', op['uuid'])

    # -- whose mistake gets reported first ---------------------------------

    def test_a_malformed_edge_is_reported_before_the_capability(self):
        # Against a server which cannot take edges at all, a caller who
        # also built a broken one hears about the broken one. The caller
        # can fix that; what the cluster on the other end is capable of is
        # not theirs to fix, and is the less useful half of the answer.
        self.client.root_html = 'instance-execute'

        self.assertRaises(ValueError, self._execute,
                          depends_on=[{'op_uuid': self.OP_UUID, 'nonsense': 1}])

    # -- the refusal has to stop the operation, not just the await ---------

    def test_a_dropped_edge_deletes_the_operation_it_created(self):
        # Refusing to await an operation does not stop it running. By the
        # time the response reveals the dropped edges the operation is
        # queued and will run with no ordering at all, which is the harm
        # this check is for, so the only thing which averts it is deleting
        # the operation.
        self._answers({'depends_on': []})

        with mock.patch.object(self.client, 'delete_agent_operation') as delete:
            e = self.assertRaises(apiclient.IncapableException,
                                  self._execute, depends_on=[self.OP_UUID])

        delete.assert_called_once_with('op1')
        self.assertIn('has been deleted', str(e))

    def test_a_failed_cleanup_is_reported_in_the_refusal(self):
        # A cleanup which fails must not replace the explanation of what
        # went wrong, and the caller still has to be told an operation is
        # out there and which one.
        self._answers({'depends_on': []})

        with mock.patch.object(self.client, 'delete_agent_operation',
                               side_effect=Exception('no such capability')):
            e = self.assertRaises(apiclient.IncapableException,
                                  self._execute, depends_on=[self.OP_UUID])

        self.assertIn('could not be deleted', str(e))
        self.assertIn('no such capability', str(e))
        self.assertIn('op1', str(e))
        # Still the refusal it was, not a report about the cleanup.
        self.assertIn('sf-database', str(e))

    # -- the idiom the repository publishes for submitting a chain ---------

    def test_await_seconds_zero_submits_without_waiting(self):
        # AGENTS.md publishes this as how a chain link is submitted, so it
        # is pinned here: a deadline already reached means the operation
        # comes back as the POST described it, with no polling at all.
        #
        # The response is deliberately a *queued* operation. A complete one
        # would leave the await loop with nothing to do whatever the
        # deadline said, so the test would pass even if await_seconds=0
        # were ignored -- which is what it exists to catch.
        self.mock_request.return_value.json.return_value = {
            'uuid': 'op1', 'state': 'queued',
            'depends_on': [{'op_uuid': self.OP_UUID}]}

        with mock.patch.object(
                self.client, 'get_agent_operation',
                return_value={'uuid': 'op1', 'state': 'complete'}) as get_op:
            op = self._execute(depends_on=[self.OP_UUID], await_seconds=0)

        self.assertEqual('queued', op['state'])
        get_op.assert_not_called()


class AgentOperationChainAwaitTestCase(testtools.TestCase):
    """A chain which fails must name the operation that actually failed.

    A dependent is aborted by the cascade when the operation it declared a
    dependency on did not succeed, and that dependent may itself be the
    dependency of the next link -- so the operation a caller awaited is the
    last link rather than the cause. await_agent_chain() walks back to the
    cause, and says which of several reasons it cannot when the walk cannot
    get there. Every branch is tested here against fabricated views,
    because two of them -- a swept dependency and an unreadable one -- need
    an hour of sweep latency or a database fault to produce on a cluster.
    """

    # Operation uuids, in submission order, plus one which was never part
    # of the chain the caller handed in.
    OP_A = 'a0b5a1d6-0c50-4a1f-9f42-1f6e2ad1c001'
    OP_B = 'b1c6b2e7-1d61-4b20-8a53-2a7f3be2d002'
    OP_C = 'c2d7c3f8-2e72-4c31-9b64-3b806cf3e003'
    OP_ELSEWHERE = 'd3e8d409-3f83-4d42-8c75-4c917d04f004'

    def setUp(self):
        super().setUp()

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

        self.sleep = mock.patch('time.sleep')
        self.mock_sleep = self.sleep.start()
        self.addCleanup(self.sleep.stop)

        self.client = apiclient.Client(suppress_configuration_lookup=True,
                                       base_url='http://localhost:13000')

    # -- building the views a submitted chain is made of -------------------

    def _submitted(self, op_uuid):
        """An operation view as a creating POST returns one.

        Deliberately carrying the state the operation *started* in, which
        is what makes the walk's refresh of a link necessary rather than
        decorative: nothing in the list a caller submitted knows how any
        of it ended.
        """
        return {'uuid': op_uuid, 'state': 'queued'}

    def _aborted(self, op_uuid, dependency_failure=None):
        view = {'uuid': op_uuid, 'state': 'abort'}
        if dependency_failure is not None:
            view['dependency_failure'] = dependency_failure
        return view

    def _cascade(self, op_uuid, state):
        """A dependency_failure as the cascade writes one."""
        return {'op_type': 'agent', 'op_uuid': op_uuid, 'state': state}

    def _await(self, operations, **kwargs):
        return self.client.await_agent_chain(operations, **kwargs)

    # -- the chain worked --------------------------------------------------

    def test_chain_success_returns_the_last_operation(self):
        # Only the last link is awaited: every other one has to finish
        # before it can run, so awaiting them individually is what
        # declaring the order was supposed to avoid.
        clock = _FakeClock()
        self.mock_sleep.side_effect = lambda seconds: clock.advance(seconds)
        complete = {'uuid': self.OP_C, 'state': 'complete'}
        operations = [self._submitted(self.OP_A), self._submitted(self.OP_B),
                      self._submitted(self.OP_C)]

        with mock.patch('time.time', clock), \
                mock.patch.object(self.client, 'get_agent_operation',
                                  return_value=complete) as get_op:
            result = self._await(operations, timeout=30)

        self.assertEqual(complete, result)
        # One poll, of the last operation and nothing else.
        get_op.assert_called_once_with(self.OP_C)

    def test_an_empty_chain_is_refused(self):
        self.assertRaises(ValueError, self._await, [])

    # -- the chain failed, and the cause can be named ----------------------

    def test_abort_names_the_immediately_preceding_operation(self):
        operations = [self._submitted(self.OP_A),
                      self._aborted(self.OP_B, self._cascade(self.OP_A, 'error'))]

        with mock.patch.object(
                self.client, 'get_agent_operation',
                return_value={'uuid': self.OP_A, 'state': 'error'}) as get_op:
            e = self.assertRaises(apiclient.AgentOperationFailed, self._await, operations)

        # The cause, not the last link: a caller reading op_uuid is told
        # which operation to go and look at.
        self.assertEqual(self.OP_A, e.op_uuid)
        self.assertEqual('error', e.op_view['state'])
        self.assertIn('actually failed', str(e))
        self.assertIn('1 operation back in the chain', str(e))
        self.assertIn('"error"', str(e))
        get_op.assert_called_once_with(self.OP_A)

    def test_abort_names_an_operation_two_hops_back(self):
        # B was aborted by the cascade in its turn, so B is a link and not
        # a cause -- the walk has to keep going to reach A.
        operations = [self._submitted(self.OP_A), self._submitted(self.OP_B),
                      self._aborted(self.OP_C, self._cascade(self.OP_B, 'abort'))]
        views = {
            self.OP_B: self._aborted(self.OP_B, self._cascade(self.OP_A, 'expired')),
            self.OP_A: {'uuid': self.OP_A, 'state': 'expired'},
        }

        with mock.patch.object(self.client, 'get_agent_operation',
                               side_effect=lambda u: views[u]) as get_op:
            e = self.assertRaises(apiclient.AgentOperationFailed, self._await, operations)

        self.assertEqual(self.OP_A, e.op_uuid)
        self.assertIn('2 operations back in the chain', str(e))
        self.assertIn('"expired"', str(e))
        self.assertEqual([mock.call(self.OP_B), mock.call(self.OP_A)],
                         get_op.call_args_list)

    def test_an_unreadable_link_falls_back_to_the_recorded_state(self):
        # The refresh of a link is best effort: a read which fails while
        # decorating a failure must not replace it, or the caller loses the
        # failure in favour of the error from reporting it.
        operations = [self._submitted(self.OP_A),
                      self._aborted(self.OP_B, self._cascade(self.OP_A, 'error'))]
        gone = apiclient.ResourceNotFoundException(
            'not found', 'GET', '/agentoperations/' + self.OP_A, 404, '')

        with mock.patch.object(self.client, 'get_agent_operation', side_effect=gone):
            e = self.assertRaises(apiclient.AgentOperationFailed, self._await, operations)

        self.assertEqual(self.OP_A, e.op_uuid)
        self.assertIn('actually failed', str(e))
        self.assertIn('"error"', str(e))

    # -- the chain failed, and the cause cannot be named -------------------

    def _assert_unresolvable(self, dependency_failure, phrase, named=None):
        """Walk a chain whose cause cannot be resolved, and read the reason.

        The assertion is on the phrase which distinguishes this branch from
        the others rather than on the whole message, because the wording
        around it is for an operator and will be improved; what must not
        change is that the four cases remain four different answers.
        """
        operations = [self._submitted(self.OP_A), self._submitted(self.OP_B),
                      self._aborted(self.OP_C, dependency_failure)]

        with mock.patch.object(self.client, 'get_agent_operation') as get_op:
            e = self.assertRaises(apiclient.AgentOperationFailed, self._await, operations)

        self.assertIn(phrase, str(e))
        # None of these branches needs a link refreshed: each is decided
        # entirely from what the cascade already recorded.
        get_op.assert_not_called()
        if named:
            self.assertEqual(named, e.op_uuid)
        return e

    def test_a_pending_dependency_reports_the_wait_budget(self):
        # Not a failed step: the dependency was still working when the
        # dependent ran out of patience, and the remedy is a budget rather
        # than a fix.
        for state in ('initial', 'preflight', 'queued', 'executing'):
            e = self._assert_unresolvable(
                self._cascade(self.OP_B, state),
                'ran out of its dependency wait budget', named=self.OP_B)
            # The setting is named rather than its value asserted: the
            # default is the cluster's to change, and a client test which
            # pinned it would fail for something this client does not own.
            self.assertIn('AGENT_OPERATION_DEPENDENCY_WAIT', str(e))
            self.assertIn(f'"{state}"', str(e))

    def test_a_swept_dependency_says_so(self):
        e = self._assert_unresolvable(
            self._cascade(self.OP_B, 'missing'),
            'had been swept or deleted', named=self.OP_B)

        # Why it cannot be recovered, rather than a guess in either
        # direction.
        self.assertIn('never swept', str(e))

    def test_an_unreadable_dependency_points_at_the_cluster(self):
        e = self._assert_unresolvable(
            self._cascade(self.OP_B, 'unreadable'),
            'could not read the state', named=self.OP_B)

        self.assertIn('cluster', str(e))

    def test_a_dependency_outside_the_chain_is_not_resolved(self):
        self._assert_unresolvable(
            self._cascade(self.OP_ELSEWHERE, 'error'),
            'was not submitted with this chain', named=self.OP_ELSEWHERE)

    def test_an_abort_without_a_cause_says_so(self):
        operations = [self._submitted(self.OP_A),
                      self._aborted(self.OP_B)]

        with mock.patch.object(self.client, 'get_agent_operation') as get_op:
            e = self.assertRaises(apiclient.AgentOperationFailed, self._await, operations)

        self.assertIn('carries no dependency_failure', str(e))
        self.assertIn('the operation which was awaited', str(e))
        self.assertEqual(self.OP_B, e.op_uuid)
        get_op.assert_not_called()

    # -- the walk's two hard limits ----------------------------------------

    def test_a_cascade_naming_no_operation_says_so(self):
        # The cascade writes the dependency's uuid before it writes the
        # abort, so an abort whose cause names nothing is as unanswerable
        # as one with no cause at all -- and must not be reported as a
        # dependency called None.
        operations = [self._submitted(self.OP_A),
                      self._aborted(self.OP_B, {'op_type': 'agent', 'state': 'error'})]

        e = self.assertRaises(apiclient.AgentOperationFailed,
                              self._await, operations)

        self.assertIn('names no operation', str(e))
        self.assertNotIn('None', str(e))
        self.assertEqual(self.OP_B, e.op_uuid)

    def test_an_unresolvable_branch_carries_the_state_the_cascade_saw(self):
        # AgentOperationFailed promises op_view is the operation's last
        # known view, and the view a caller submitted says "queued"
        # forever -- so handing that back would contradict the message
        # beside it, which says the dependency was executing, swept or
        # unreadable.
        for state in ('executing', 'missing', 'unreadable'):
            operations = [self._submitted(self.OP_A),
                          self._aborted(self.OP_B, self._cascade(self.OP_A, state))]

            e = self.assertRaises(apiclient.AgentOperationFailed,
                                  self._await, operations)

            self.assertEqual(self.OP_A, e.op_uuid)
            self.assertEqual(state, e.op_view['state'])
            self.assertEqual(self.OP_A, e.op_view['uuid'])

    def test_a_dependency_outside_the_chain_still_reports_a_view(self):
        operations = [self._submitted(self.OP_A),
                      self._aborted(self.OP_B,
                                    self._cascade(self.OP_ELSEWHERE, 'error'))]

        e = self.assertRaises(apiclient.AgentOperationFailed,
                              self._await, operations)

        self.assertEqual(self.OP_ELSEWHERE, e.op_uuid)
        self.assertEqual({'uuid': self.OP_ELSEWHERE, 'state': 'error'}, e.op_view)

    def test_the_walk_never_requests_an_operation_outside_the_chain(self):
        # Counted rather than inspected, because this is an authorisation
        # property and not a tidiness one: a chain may cross namespaces, so
        # a walk which chased a uuid the caller did not submit would be
        # asking the cluster a question on the caller's behalf that the
        # caller never asked.
        operations = [self._submitted(self.OP_A), self._submitted(self.OP_B),
                      self._aborted(self.OP_C, self._cascade(self.OP_B, 'abort'))]
        # B was aborted by something from outside the submitted chain.
        refreshed_b = self._aborted(self.OP_B,
                                    self._cascade(self.OP_ELSEWHERE, 'error'))

        with mock.patch.object(self.client, 'get_agent_operation',
                               return_value=refreshed_b) as get_op:
            e = self.assertRaises(apiclient.AgentOperationFailed, self._await, operations)

        self.assertEqual(1, get_op.call_count)
        self.assertEqual([mock.call(self.OP_B)], get_op.call_args_list)
        requested = {call.args[0] for call in get_op.call_args_list}
        self.assertEqual(set(), requested - {self.OP_A, self.OP_B, self.OP_C})
        self.assertIn('was not submitted with this chain', str(e))

    def test_the_walk_is_bounded_by_the_chain_it_was_given(self):
        # A client cannot build a cycle, because an edge may only name an
        # operation which already exists. This asserts the walk stops
        # anyway, since it is reading state a cluster wrote and a reporting
        # helper which could be hung is a worse answer than one which says
        # it stopped.
        operations = [self._submitted(self.OP_A),
                      self._aborted(self.OP_B, self._cascade(self.OP_A, 'abort'))]
        views = {
            self.OP_A: self._aborted(self.OP_A, self._cascade(self.OP_B, 'abort')),
            self.OP_B: self._aborted(self.OP_B, self._cascade(self.OP_A, 'abort')),
        }

        with mock.patch.object(self.client, 'get_agent_operation',
                               side_effect=lambda u: views[u]) as get_op:
            e = self.assertRaises(apiclient.AgentOperationFailed, self._await, operations)

        self.assertIn('longer than the 2 operations submitted', str(e))
        self.assertEqual(2, get_op.call_count)

    # -- a failure which is not a chain question ---------------------------

    def test_a_non_abort_failure_passes_through_unchanged(self):
        # Only the cascade writes an abort, so any other terminal failure
        # is the awaited operation's own and is already reported as well as
        # it can be. Rewriting it as a chain failure would bury the real
        # one under a walk with nothing to walk.
        operations = [self._submitted(self.OP_A),
                      {'uuid': self.OP_B, 'state': 'error'}]

        with mock.patch.object(self.client, 'get_agent_operation') as get_op:
            e = self.assertRaises(apiclient.AgentOperationFailed, self._await, operations)

        self.assertEqual(self.OP_B, e.op_uuid)
        self.assertIn('entered terminal state', str(e))
        self.assertNotIn('chain', str(e))
        get_op.assert_not_called()
