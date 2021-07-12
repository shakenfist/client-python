import json
import mock
import testtools


from shakenfist_client import apiclient


class ApiClientTestCase(testtools.TestCase):
    def setUp(self):
        super(ApiClientTestCase, self).setUp()

        self.request_url = mock.patch(
            'shakenfist_client.apiclient.Client._request_url')
        self.mock_request = self.request_url.start()
        self.addCleanup(self.request_url.stop)

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
            'GET', '/instances/notreallyauuid')

    def test_get_instance_interfaces(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_instance_interfaces('notreallyauuid')

        self.mock_request.assert_called_with(
            'GET', '/instances/notreallyauuid/interfaces')

    def test_create_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.create_instance('foo', 1, 2048, ['netuuid1'], ['8@cirros'],
                               'sshkey', None, namespace=None, force_placement=None,
                               video={'model': 'cirrus', 'memory': 16384})

        self.mock_request.assert_called_with(
            'POST', '/instances',
            data={
                'name': 'foo',
                'cpus': 1,
                'memory': 2048,
                'network': ['netuuid1'],
                'disk': ['8@cirros'],
                'ssh_key': 'sshkey',
                'user_data': None,
                'namespace': None,
                'video': {'memory': 16384, 'model': 'cirrus'},
                'uefi': False
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
            data={
                'name': 'foo',
                'cpus': 1,
                'memory': 2048,
                'network': ['netuuid1'],
                'disk': ['8@cirros'],
                'ssh_key': 'sshkey',
                'user_data': 'userdatabeforebase64',
                'namespace': None,
                'video': {'memory': 16384, 'model': 'cirrus'},
                'uefi': False
            })

    def test_snapshot_instance(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.snapshot_instance('notreallyauuid', all=True)

        self.mock_request.assert_called_with(
            'POST', '/instances/notreallyauuid/snapshot',
            data={'all': True})

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
            'DELETE', '/instances/notreallyauuid')

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

    def test_cache_image(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.cache_image('imageurl')

        self.mock_request.assert_called_with(
            'POST', '/images',
            data={'url': 'imageurl'})

    def test_get_images(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.get_images('sf-2')

        self.mock_request.assert_called_with(
            'GET', '/images',
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

    def test_delete_namespace_key(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.delete_namespace_key('testspace', 'keyname')

        self.mock_request.assert_called_with(
            'DELETE', '/auth/namespaces/testspace/keys/keyname')

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
            'GET', '/networks/notreallyauuid')

    def test_delete_network(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.delete_network('notreallyauuid')

        self.mock_request.assert_called_with(
            'DELETE', '/networks/notreallyauuid')

    def test_delete_all_networks(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.delete_all_networks(None)

        self.mock_request.assert_called_with(
            'DELETE', '/networks',
            data={'confirm': True, 'namespace': None})

    def test_delete_all_networks_namespace(self):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        client.delete_all_networks('bobspace')

        self.mock_request.assert_called_with(
            'DELETE', '/networks',
            data={'confirm': True, 'namespace': 'bobspace'})

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
    @mock.patch('shakenfist_client.apiclient.Client._request_url',
                return_value=GetNodesMock())
    def test_get_nodes(self, mock_request):
        client = apiclient.Client(suppress_configuration_lookup=True,
                                  base_url='http://localhost:13000')
        list(client.get_nodes())

        mock_request.assert_called_with(
            'GET', '/nodes')
