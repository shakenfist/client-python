from unittest import mock

import testtools
from click.testing import CliRunner

from shakenfist_client.commandline import node as node_cmd


NODE = {
    'name': 'sf-1',
    'ip': '192.168.1.1',
    'state': 'created',
    'lastseen': 1755213600.0,
    'version': '0.9.0',
    'release': '0.9.0',
    'is_hypervisor': True,
    'is_network_node': False,
    'is_cluster_maintainer': False,
    'daemon-sf-api-state': 'running',
    'daemon-cluster-state': None,
    'daemon-agent-state': 'degraded',
}


class NodeShowTestCase(testtools.TestCase):
    """Tests for ``sf-client node show``."""

    def _show(self, node, output='pretty'):
        client = mock.MagicMock()
        client.get_node.return_value = node
        client.check_capability.return_value = True
        client.get_node_metadata.return_value = {'k': 'v'}
        result = CliRunner().invoke(
            node_cmd.node, ['show', 'sf-1'],
            obj={'CLIENT': client, 'OUTPUT': output}, catch_exceptions=False)
        self.assertEqual(0, result.exit_code)
        return result.output.splitlines()

    def test_simple_lists_daemons_sorted_before_metadata(self):
        lines = self._show(NODE, output='simple')
        daemons = [x for x in lines if x.startswith('daemon,')]
        self.assertEqual(['daemon,name,state', 'daemon,agent,degraded', 'daemon,cluster,-',
                          'daemon,sf-api,running'], daemons)
        self.assertLess(lines.index('daemon,agent,degraded'), lines.index('metadata,key,value'))

    def test_pretty_indents_daemons_like_metadata(self):
        lines = self._show(NODE)
        start = lines.index('Daemon states:')
        self.assertEqual(['    agent   : degraded', '    cluster : -', '    sf-api  : running'],
                         lines[start + 1:start + 4])
        self.assertLess(start, lines.index('Metadata:'))

    def test_no_daemon_keys(self):
        node = {k: v for k, v in NODE.items() if not k.startswith('daemon-')}
        lines = self._show(node, output='simple')
        self.assertEqual(['daemon,name,state'], [x for x in lines if x.startswith('daemon,')])
