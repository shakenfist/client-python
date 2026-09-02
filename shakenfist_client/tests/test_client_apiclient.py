import json
import time
from unittest import mock

import requests
import testtools

from shakenfist_client import apiclient


class ApiClientTestCase(testtools.TestCase):
    def setUp(self):
        super().setUp()

        self.request_url = mock.patch(
            'shakenfist_client.apiclient.Client._request_url')
        self.mock_request = self.request_url.start()
        self.addCleanup(self.request_url.stop)

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities = self.capabilities.start()
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
        self.capabilities = self.capabilities.start()
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
    def setUp(self):
        super().setUp()

        self.capabilities = mock.patch(
            'shakenfist_client.apiclient.Client._collect_capabilities')
        self.capabilities = self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

        # Ensure the config files on the test machine are not consulted
        self.exists = mock.patch(
            'shakenfist_client.apiclient.os.path.exists', return_value=False)
        self.mock_exists = self.exists.start()
        self.addCleanup(self.exists.stop)

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
        self.capabilities = self.capabilities.start()
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
        self.capabilities = self.capabilities.start()
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
        self.capabilities = self.capabilities.start()
        self.addCleanup(self.capabilities.stop)

    def _client(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.cached_auth = 'Bearer notreallyatoken'
        return client

    def _respond(self, client, status_code):
        response = mock.MagicMock()
        response.status_code = status_code
        response.text = '{"error": "nope"}'
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

    TERMINAL_FAILURE_STATES = ('error', 'expired', 'deleted')

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
        for state in self.TERMINAL_FAILURE_STATES:
            client = self._client()
            op = {'uuid': 'op1', 'state': state}
            with mock.patch.object(client, 'await_agent_ready'), \
                    mock.patch.object(client, 'instance_execute', return_value=dict(op)), \
                    mock.patch.object(client, 'get_agent_operation') as get_op, \
                    mock.patch.object(
                        client, 'get_instance',
                        return_value={'uuid': 'notreallyauuid', 'agent_state': 'ready'}), \
                    mock.patch.object(client, 'get_console_data', return_value='console'):
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
        for state in self.TERMINAL_FAILURE_STATES:
            client = self._client()
            op = {'uuid': 'op1', 'state': state}
            with mock.patch.object(client, 'await_agent_ready'), \
                    mock.patch.object(client, 'instance_get', return_value=dict(op)), \
                    mock.patch.object(client, 'get_agent_operation') as get_op, \
                    mock.patch.object(
                        client, 'get_instance',
                        return_value={'uuid': 'notreallyauuid', 'agent_state': 'ready'}), \
                    mock.patch.object(client, 'get_console_data', return_value='console'):
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
        # Decision 6 / survey finding 5: all three loops in this method
        # must share the same `timeout` budget, measured from the same
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
        # the fetched data. The clock advances where the simulated work
        # happens rather than once per read, so an added or removed
        # time.time() call cannot turn this into a StopIteration.
        clock = _FakeClock()
        client = self._client()
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
                mock.patch.object(client, 'await_agent_ready'), \
                mock.patch.object(
                    client, 'instance_get', return_value=dict(op_executing)), \
                mock.patch.object(
                    client, 'get_agent_operation', side_effect=poll) as get_op, \
                mock.patch.object(client, 'get_blob', return_value={'state': 'created'}), \
                mock.patch.object(client, 'get_blob_data', return_value=[b'hello']):
            data = client.await_agent_fetch('notreallyauuid', '/tmp/f')

        self.assertEqual('hello', data)
        self.assertEqual(2, get_op.call_count)


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
