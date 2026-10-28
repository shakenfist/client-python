import json
from unittest import mock

import testtools
from click.testing import CliRunner

from shakenfist_client.apiclient import IncapableException
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


class NodeResourcesTestCase(testtools.TestCase):
    """Tests for ``sf-client node resources``."""

    def _resources(self, events, output='pretty'):
        client = mock.MagicMock()
        client.get_node_events.return_value = events
        return CliRunner().invoke(
            node_cmd.node, ['resources', 'sf-1'],
            obj={'CLIENT': client, 'OUTPUT': output}, catch_exceptions=False)

    def test_empty_events_exits_1(self):
        for output in ['pretty', 'simple', 'json']:
            result = self._resources([], output=output)
            self.assertEqual(1, result.exit_code)
            self.assertIn('No resources event found', result.output)

    def test_pretty_renders_every_key(self):
        result = self._resources([{'extra': {'cpu_max': 16, 'memory_max': 64000}}])
        self.assertEqual(0, result.exit_code)
        self.assertRegex(result.output, r'cpu_max\s*\|\s*16\s*\|')
        self.assertRegex(result.output, r'memory_max\s*\|\s*64000\s*\|')

    def test_simple_renders_every_key(self):
        result = self._resources([{'extra': {'cpu_max': 16, 'memory_max': 64000}}], output='simple')
        self.assertEqual(0, result.exit_code)
        self.assertEqual(['node,resource,value', 'sf-1,cpu_max,16', 'sf-1,memory_max,64000'],
                         result.output.splitlines())


def _uuid(n):
    return '00000000-0000-4000-8000-%012d' % n


UNKNOWN_UUID = _uuid(99)


def capacity_nodes():
    """The node list joined against the capacity fixture."""
    return [
        {'uuid': _uuid(1), 'name': 'sf-1', 'is_hypervisor': True, 'state': 'created'},
        {'uuid': _uuid(2), 'name': 'sf-2', 'is_hypervisor': True, 'state': 'created'},
        {'uuid': _uuid(3), 'name': 'sf-3', 'is_hypervisor': True, 'state': 'created'},
        {'uuid': _uuid(4), 'name': 'sf-4', 'is_hypervisor': True, 'state': 'created'},
        {'uuid': _uuid(5), 'name': 'sf-5', 'is_hypervisor': True, 'state': 'created'},
        {'uuid': _uuid(6), 'name': 'sf-6', 'is_hypervisor': True, 'state': 'created'},
        {'uuid': _uuid(7), 'name': 'sf-7', 'is_hypervisor': True, 'state': 'created'},
        {'uuid': _uuid(8), 'name': 'net-1', 'is_hypervisor': False, 'state': 'created'},
    ]


def capacity_node(**overrides):
    """One per_node entry, shaped like the server's admin_resources_get_example."""
    n = {
        'cpu_max_per_instance': 32, 'cpu_schedulable': 30, 'cpu_reservation_clamped': False,
        'memory_reservation_clamped': False, 'cpu_committed_row_present': True, 'cpu_limit': 90,
        'cpu_hard_max': 90.0, 'cpu_measured': 18, 'domains_enumerated_at': 1792886390.12,
        'cpu_committed': 24, 'cpu_available': 66.0, 'cpu_load_1': 2.41, 'cpu_load_5': 2.13,
        'cpu_load_15': 1.87, 'memory_reserved_mb': 8192, 'ram_max_per_instance': 81920,
        'ram_max': 393216.0, 'ram_committed': 40960, 'ram_available': 344064,
        'disk_available': 1780.0, 'disk_limit_gb': 1900, 'disk_committed_gb': 700,
        'instances_total': 9, 'instances_active': 8,
    }
    n.update(overrides)
    return n


# The keys an older server did not send yet, which sf-5 lacks.
OLDER_SERVER_MISSING = (
    'cpu_reservation_clamped', 'memory_reservation_clamped', 'cpu_committed_row_present',
    'cpu_limit', 'cpu_committed', 'domains_enumerated_at', 'disk_limit_gb', 'disk_committed_gb')


def capacity_resources():
    """A /admin/resources payload covering every case the capacity view renders.

    sf-1: committed binds against the limit. sf-2: measured binds against the
    limit, both reservations clamped. sf-3: no capacity row, so measured binds
    against hard max. sf-4: packed beyond its limit, negative headroom in all
    three dimensions. sf-5: an older server's dialect, missing keys. A uuid the
    node list does not know, where committed and measured tie. sf-6 and sf-7
    are hypervisors absent from per_node, and net-1 is not a hypervisor.
    """
    older = capacity_node(cpu_measured=10, cpu_available=80.0, cpu_load_1=0.5, cpu_load_5=0.4,
                          cpu_load_15=0.3)
    for key in OLDER_SERVER_MISSING:
        del older[key]

    return {
        'total': {'cpu_available': 317.0, 'ram_available': 1414144, 'capacity_degraded': False},
        'per_node': {
            _uuid(1): capacity_node(),
            _uuid(2): capacity_node(
                cpu_schedulable=1, cpu_reservation_clamped=True, memory_reservation_clamped=True,
                cpu_limit=3, cpu_hard_max=3.0, cpu_measured=2, cpu_committed=1, cpu_available=1.0,
                cpu_load_1=0.9, cpu_load_5=0.8, cpu_load_15=0.7, memory_reserved_mb=14336,
                ram_max_per_instance=1024, ram_max=16384.0, ram_committed=2048, ram_available=1024,
                disk_available=80.0, disk_limit_gb=100, disk_committed_gb=20, instances_total=1,
                instances_active=1),
            _uuid(3): capacity_node(
                cpu_committed_row_present=False, cpu_limit=None, cpu_measured=6, cpu_committed=0,
                cpu_available=84.0, ram_committed=0, ram_available=380928, disk_limit_gb=None,
                disk_committed_gb=0, instances_total=2, instances_active=2),
            _uuid(4): capacity_node(
                cpu_limit=60, cpu_hard_max=60.0, cpu_measured=64, cpu_committed=72, cpu_available=-12.0,
                cpu_load_1=21.3, cpu_load_5=19.8, cpu_load_15=18.2, ram_committed=405504,
                ram_available=-12288, disk_available=-15.0, disk_committed_gb=1950, instances_total=24,
                instances_active=24),
            _uuid(5): older,
            UNKNOWN_UUID: capacity_node(cpu_measured=4, cpu_committed=4, cpu_available=86.0,
                                        instances_total=1, instances_active=1),
        },
    }


# The clock the exclusion ages are computed against.
CAPACITY_NOW = 1792886400.0

# Every capability the capacity view reads, and the one an older server has.
CAPACITY_TOKENS = ('cluster-resources', 'cluster-resources-excluded', 'cluster-capacity')
OLDER_SERVER_TOKENS = ('cluster-resources',)


def capacity_excluded():
    """An excluded mapping, shaped like the server's admin_resources_get_example.

    sf-6's metrics are 312 seconds stale at CAPACITY_NOW, and sf-7's queue is
    too long. net-1 has no metrics, which the server reports whatever a node's
    role, and the view omits because it is not a hypervisor.
    """
    return {
        _uuid(6): {'reason': 'stale metrics', 'metrics_timestamp': 1792886087.53},
        _uuid(7): {'reason': 'queue too long', 'node_queue_waiting': 31, 'unreasonable_threshold': 20},
        _uuid(8): {'reason': 'no metrics'},
    }


def capacity_ledger(**overrides):
    """A cluster_capacity mapping, shaped like the server's admin_resources_get_example."""
    ledger = {
        'row_present': True, 'degraded': False, 'updated_at': 1792886100.0,
        'total_cpus': 180, 'total_memory_mb': 770048, 'total_disk_gb': 3800,
        'claimed_cpus': 40, 'claimed_memory_mb': 81920, 'claimed_disk_gb': 2000,
        'unclaimed_used_cpus': 12, 'unclaimed_used_memory_mb': 16384, 'unclaimed_used_disk_gb': 100,
        'unallocated_cpus': 128, 'unallocated_memory_mb': 671744, 'unallocated_disk_gb': 1700,
    }
    ledger.update(overrides)
    return ledger


def capacity_unread_ledger(degraded):
    """A ledger with every figure None: not written yet, or not readable."""
    ledger = {key: None for key in capacity_ledger()}
    ledger.update({'row_present': False, 'degraded': degraded})
    return ledger


def capacity_current_resources():
    """capacity_resources() as a server with both newer keys sends it."""
    resources = capacity_resources()
    resources['excluded'] = capacity_excluded()
    resources['cluster_capacity'] = capacity_ledger()
    return resources


def capacity_degraded_resources():
    """A degraded read: every node reports as having no capacity row."""
    unsized = {'cpu_committed_row_present': False, 'cpu_limit': None, 'cpu_committed': 0,
               'ram_committed': 0, 'ram_available': 380928, 'disk_limit_gb': None,
               'disk_committed_gb': 0}
    return {
        'total': {'cpu_available': 158.0, 'ram_available': 761856, 'capacity_degraded': True},
        'per_node': {
            _uuid(1): capacity_node(cpu_available=72.0, **unsized),
            _uuid(3): capacity_node(cpu_measured=4, cpu_available=86.0, **unsized),
        },
    }


def _table_rows(output, heading):
    """The data rows of the PrettyTable under heading, as lists of cells."""
    lines = output.splitlines()
    start = lines.index(heading)
    rows = []
    # heading, border, header, border, then rows until the closing border.
    for line in lines[start + 4:]:
        if line.startswith('+'):
            break
        rows.append([cell.strip() for cell in line.strip('|').split('|')])
    return rows


CPU_HEADING = 'CPU (vCPUs; * marks what the scheduler admits against):'
CPU_COLUMNS = ['node', 'committed', 'measured', 'limit', 'hard max', 'available', 'schedulable',
               'load 1/5/15', 'notes']


class NodeCapacityTestCase(testtools.TestCase):
    """Tests for ``sf-client node capacity``."""

    def _capacity(self, resources=None, nodes=None, output='pretty', args=None,
                  capabilities=OLDER_SERVER_TOKENS):
        client = mock.MagicMock()
        client.get_cluster_resources.return_value = (
            capacity_resources() if resources is None else resources)
        client.get_nodes.return_value = capacity_nodes() if nodes is None else nodes
        client.check_capability.side_effect = lambda token: token in capabilities
        with mock.patch.object(node_cmd, '_capacity_now', return_value=CAPACITY_NOW):
            return CliRunner().invoke(
                node_cmd.node, ['capacity'] + (args or []),
                obj={'CLIENT': client, 'OUTPUT': output}, catch_exceptions=False)

    def _cpu_rows(self, output):
        return {row[0]: dict(zip(CPU_COLUMNS, row)) for row in _table_rows(output, CPU_HEADING)}

    def test_binding_marker_needs_numbers(self):
        for overrides in ({'cpu_measured': None}, {'cpu_committed': None}, {'cpu_limit': 'ninety'},
                          {'cpu_committed_row_present': False, 'cpu_hard_max': None}):
            self.assertEqual(set(), node_cmd._capacity_binding_cpu(capacity_node(**overrides)), overrides)

    def test_binding_marker_on_committed(self):
        row = self._cpu_rows(self._capacity().output)['sf-1']
        self.assertEqual(('24*', '18', '90*', '90.0'),
                         (row['committed'], row['measured'], row['limit'], row['hard max']))

    def test_binding_marker_on_measured(self):
        row = self._cpu_rows(self._capacity().output)['sf-2']
        self.assertEqual(('1', '2*', '3*', '3.0'),
                         (row['committed'], row['measured'], row['limit'], row['hard max']))

    def test_binding_marker_on_limit_not_hard_max(self):
        # Limit and hard max are equal here, and only the limit is a ceiling.
        row = self._cpu_rows(self._capacity().output)['sf-4']
        self.assertEqual(('72*', '64', '60*', '60.0'),
                         (row['committed'], row['measured'], row['limit'], row['hard max']))

    def test_limit_replaces_hard_max_even_when_higher(self):
        resources = capacity_resources()
        resources['per_node'][_uuid(1)].update({'cpu_limit': 120, 'cpu_hard_max': 90.0})
        row = self._cpu_rows(self._capacity(resources=resources).output)['sf-1']
        self.assertEqual(('120*', '90.0'), (row['limit'], row['hard max']))

    def test_binding_marker_on_hard_max_without_a_row(self):
        row = self._cpu_rows(self._capacity().output)['sf-3']
        self.assertEqual(('0', '6*', '-', '90.0*'),
                         (row['committed'], row['measured'], row['limit'], row['hard max']))
        self.assertEqual('no capacity row', row['notes'])

    def test_binding_marker_on_both_charges_when_they_tie(self):
        row = self._cpu_rows(self._capacity().output)[UNKNOWN_UUID]
        self.assertEqual(('4*', '4*', '90*', '90.0'),
                         (row['committed'], row['measured'], row['limit'], row['hard max']))

    def test_missing_keys_render_na_and_mark_nothing(self):
        result = self._capacity()
        self.assertEqual(0, result.exit_code)
        row = self._cpu_rows(result.output)['sf-5']
        self.assertEqual(('n/a', '10', 'n/a', '90.0'),
                         (row['committed'], row['measured'], row['limit'], row['hard max']))
        self.assertNotIn('*', ''.join(row.values()))
        self.assertEqual('', row['notes'])
        disk = {r[0]: r for r in _table_rows(result.output, 'Disk (GB):')}
        self.assertEqual(['sf-5', 'n/a', 'n/a', '1780.0', ''], disk['sf-5'])

    def test_clamped_notes(self):
        output = self._capacity().output
        self.assertEqual('cpu clamped', self._cpu_rows(output)['sf-2']['notes'])
        memory = {r[0]: r for r in _table_rows(output, 'Memory (MB):')}
        self.assertEqual('memory clamped', memory['sf-2'][-1])

    def test_negative_headroom_is_over_limit_in_every_table(self):
        output = self._capacity().output
        self.assertEqual(('-12.0', 'over limit'),
                         (self._cpu_rows(output)['sf-4']['available'], self._cpu_rows(output)['sf-4']['notes']))
        memory = {r[0]: r for r in _table_rows(output, 'Memory (MB):')}
        self.assertEqual(['sf-4', '405504', '-12288', '393216.0', '8192', '81920', 'over limit'],
                         memory['sf-4'])
        disk = {r[0]: r for r in _table_rows(output, 'Disk (GB):')}
        self.assertEqual(['sf-4', '1950', '1900', '-15.0', 'over limit'], disk['sf-4'])

    def test_no_capacity_row_noted_in_every_table(self):
        output = self._capacity().output
        for heading in [CPU_HEADING, 'Memory (MB):', 'Disk (GB):']:
            rows = {r[0]: r for r in _table_rows(output, heading)}
            self.assertEqual('no capacity row', rows['sf-3'][-1])

    def test_unknown_uuid_renders_as_itself_last(self):
        output = self._capacity().output
        self.assertEqual(['sf-1', 'sf-2', 'sf-3', 'sf-4', 'sf-5', UNKNOWN_UUID],
                         [r[0] for r in _table_rows(output, CPU_HEADING)])

    def test_totals_line(self):
        self.assertIn('Total available: 317.0 vCPUs, 1414144 MB RAM', self._capacity().output.splitlines())

    def test_not_reported_hypervisors_are_listed(self):
        lines = self._capacity().output.splitlines()
        start = lines.index('Hypervisors not reported by the scheduler (queue length or stale metrics):')
        self.assertEqual(['    sf-6', '    sf-7'], lines[start + 1:])
        self.assertNotIn('net-1', '\n'.join(lines))

    def test_every_hypervisor_is_printed(self):
        lines = self._capacity().output.splitlines()
        printed = {r[0] for r in _table_rows('\n'.join(lines), CPU_HEADING)}
        start = lines.index('Hypervisors not reported by the scheduler (queue length or stale metrics):')
        printed |= {line.strip() for line in lines[start + 1:]}
        hypervisors = {n['name'] for n in capacity_nodes() if n['is_hypervisor']}
        # The unknown uuid is the only label which is not a hypervisor's name.
        self.assertEqual(hypervisors | {UNKNOWN_UUID}, printed)

    def test_no_not_reported_section_when_every_hypervisor_reports(self):
        nodes = [n for n in capacity_nodes() if n['name'] not in ('sf-6', 'sf-7')]
        self.assertNotIn('not reported', self._capacity(nodes=nodes).output)

    def test_degraded_banner_comes_first(self):
        result = self._capacity(resources=capacity_degraded_resources())
        self.assertEqual(0, result.exit_code)
        lines = result.output.splitlines()
        self.assertTrue(lines[0].startswith('WARNING: the scheduler could not read the capacity counters.'))
        self.assertLess(2, lines.index(CPU_HEADING))
        # The failed read hides the rows admission still tests, so nothing
        # is marked.
        self.assertEqual(('0', '18', '-', '90.0', ''),
                         tuple(self._cpu_rows(result.output)['sf-1'][k]
                               for k in ['committed', 'measured', 'limit', 'hard max', 'notes']))
        for row in self._cpu_rows(result.output).values():
            self.assertNotIn('*', ''.join(row.values()))
        self.assertNotIn('no capacity row', result.output)

    def test_no_banner_when_not_degraded(self):
        self.assertNotIn('WARNING', self._capacity().output)

    def test_filter_by_name_and_uuid(self):
        for ref in ['sf-2', _uuid(2)]:
            result = self._capacity(args=[ref])
            self.assertEqual(0, result.exit_code)
            self.assertEqual(['sf-2'], [r[0] for r in _table_rows(result.output, CPU_HEADING)])
            self.assertNotIn('Total available', result.output)
            self.assertNotIn('not reported', result.output)

    def test_filter_to_unreported_hypervisor_exits_1(self):
        for output in ['pretty', 'simple', 'json']:
            result = self._capacity(args=['sf-6'], output=output)
            self.assertEqual(1, result.exit_code)
            self.assertEqual('sf-6: not reported by the scheduler (queue length or stale metrics)\n',
                             result.output)

    def test_filter_to_non_hypervisor_exits_1(self):
        result = self._capacity(args=['net-1'])
        self.assertEqual(1, result.exit_code)
        self.assertEqual('net-1 is not a hypervisor, so the scheduler does not report it\n', result.output)

    def test_filter_to_unknown_node_exits_1(self):
        result = self._capacity(args=['sf-42'])
        self.assertEqual(1, result.exit_code)
        self.assertEqual('Node sf-42 not found\n', result.output)

    def test_simple(self):
        result = self._capacity(output='simple')
        self.assertEqual(0, result.exit_code)
        lines = result.output.splitlines()
        self.assertEqual(','.join(('name', 'uuid', 'capacity_degraded') + node_cmd.CAPACITY_SIMPLE_KEYS),
                         lines[0])
        self.assertEqual(27, len(lines[0].split(',')))
        self.assertEqual(
            'sf-1,%s,False,32,30,False,False,True,90,90.0,18,1792886390.12,24,66.0,2.41,2.13,1.87,'
            '8192,81920,393216.0,40960,344064,1780.0,1900,700,9,8' % _uuid(1), lines[1])
        self.assertEqual(
            'sf-3,%s,False,32,30,False,False,False,-,90.0,6,1792886390.12,0,84.0,2.41,2.13,1.87,'
            '8192,81920,393216.0,0,380928,1780.0,-,0,2,2' % _uuid(3), lines[3])
        self.assertEqual(
            'sf-5,%s,False,32,30,n/a,n/a,n/a,n/a,90.0,10,n/a,n/a,80.0,0.5,0.4,0.3,'
            '8192,81920,393216.0,40960,344064,1780.0,n/a,n/a,9,8' % _uuid(5), lines[5])
        self.assertTrue(lines[6].startswith(UNKNOWN_UUID + ',' + UNKNOWN_UUID + ','))
        self.assertEqual(['', 'not_reported,name,uuid,reason',
                          'not_reported,sf-6,%s,n/a' % _uuid(6),
                          'not_reported,sf-7,%s,n/a' % _uuid(7)], lines[7:])

    def test_simple_repeats_degraded(self):
        lines = self._capacity(resources=capacity_degraded_resources(), output='simple').output.splitlines()
        self.assertEqual(['True', 'True'], [line.split(',')[2] for line in lines[1:3]])

    def test_simple_filtered_has_no_sections(self):
        lines = self._capacity(output='simple', args=['sf-2']).output.splitlines()
        self.assertEqual(2, len(lines))
        self.assertTrue(lines[1].startswith('sf-2,'))

    def test_json_is_the_payload_verbatim(self):
        result = self._capacity(output='json')
        self.assertEqual(0, result.exit_code)
        self.assertEqual(capacity_resources(), json.loads(result.output))

    def test_json_filtered_keeps_the_shape(self):
        result = self._capacity(output='json', args=['sf-2'])
        expected = capacity_resources()
        expected['per_node'] = {_uuid(2): expected['per_node'][_uuid(2)]}
        self.assertEqual(expected, json.loads(result.output))

    def test_incapable_exits_1(self):
        client = mock.MagicMock()
        client.get_cluster_resources.side_effect = IncapableException('no resources for you')
        result = CliRunner().invoke(
            node_cmd.node, ['capacity'], obj={'CLIENT': client, 'OUTPUT': 'pretty'},
            catch_exceptions=False)
        self.assertEqual(1, result.exit_code)
        self.assertEqual('no resources for you\n', result.output)


NOT_REPORTED_HEADING = 'Hypervisors not reported by the scheduler:'
LEDGER_HEADING = 'Cluster claims ledger:'
LEDGER_BANNER = 'WARNING: the cluster claims ledger could not be read. Its figures are unknown, not'
LEDGER_UNWRITTEN = 'The reconciler has not written the cluster ledger yet, so unclaimed placements are'


class NodeCapacityExclusionsAndLedgerTestCase(testtools.TestCase):
    """Tests for the exclusion reasons and the claims ledger in ``node capacity``."""

    def _capacity(self, resources=None, output='pretty', args=None, capabilities=CAPACITY_TOKENS,
                  nodes=None):
        client = mock.MagicMock()
        client.get_cluster_resources.return_value = (
            capacity_current_resources() if resources is None else resources)
        client.get_nodes.return_value = capacity_nodes() if nodes is None else nodes
        client.check_capability.side_effect = lambda token: token in capabilities
        with mock.patch.object(node_cmd, '_capacity_now', return_value=CAPACITY_NOW):
            return CliRunner().invoke(
                node_cmd.node, ['capacity'] + (args or []),
                obj={'CLIENT': client, 'OUTPUT': output}, catch_exceptions=False)

    def _not_reported(self, output):
        lines = output.splitlines()
        start = lines.index(NOT_REPORTED_HEADING)
        return lines[start + 1:lines.index('', start)]

    def _simple_section(self, output, header):
        lines = output.splitlines()
        start = lines.index(header)
        end = lines.index('', start) if '' in lines[start:] else len(lines)
        return lines[start + 1:end]

    def _with_excluded(self, excluded):
        resources = capacity_current_resources()
        resources['excluded'] = excluded
        return resources

    def _with_ledger(self, ledger):
        resources = capacity_current_resources()
        resources['cluster_capacity'] = ledger
        return resources

    def test_reasons_replace_the_sentence(self):
        result = self._capacity()
        self.assertEqual(0, result.exit_code)
        self.assertEqual(['    sf-6 : stale metrics (last published 312 seconds ago)',
                          '    sf-7 : queue too long (31 waiting, threshold 20)'],
                         self._not_reported(result.output))
        self.assertNotIn('queue length or stale metrics', result.output)
        # net-1 has an entry, but it is not a hypervisor.
        self.assertNotIn('net-1', result.output)

    def test_every_hypervisor_is_printed_once(self):
        output = self._capacity().output
        printed = [r[0] for r in _table_rows(output, CPU_HEADING)]
        printed += [line.split(':')[0].strip() for line in self._not_reported(output)]
        hypervisors = {n['name'] for n in capacity_nodes() if n['is_hypervisor']}
        self.assertEqual(sorted(hypervisors | {UNKNOWN_UUID}), sorted(printed))

    def test_stale_age_is_counted_from_now(self):
        entry = {'reason': 'stale metrics', 'metrics_timestamp': 1000.0}
        for now, age in [(1000.0, 0), (1312.9, 312), (5000.0, 4000)]:
            with mock.patch.object(node_cmd, '_capacity_now', return_value=now):
                self.assertEqual('stale metrics (last published %d seconds ago)' % age,
                                 node_cmd._capacity_exclusion_reason(entry))

    def test_stale_age_from_the_future_is_left_out(self):
        # This machine's clock is behind the server's.
        entry = {'reason': 'stale metrics', 'metrics_timestamp': 1040.0}
        with mock.patch.object(node_cmd, '_capacity_now', return_value=1000.0):
            self.assertEqual('stale metrics', node_cmd._capacity_exclusion_reason(entry))

    def test_unknown_reason_is_rendered_as_sent(self):
        excluded = dict(capacity_excluded())
        excluded[_uuid(6)] = {'reason': 'cosmic rays', 'metrics_timestamp': 1.0}
        resources = self._with_excluded(excluded)
        result = self._capacity(resources=resources)
        self.assertEqual(0, result.exit_code)
        self.assertEqual('    sf-6 : cosmic rays', self._not_reported(result.output)[0])
        simple = self._capacity(resources=resources, output='simple').output
        self.assertIn('not_reported,sf-6,%s,cosmic rays' % _uuid(6), simple.splitlines())

    def test_missing_details_are_left_out(self):
        cases = [
            ({'reason': 'stale metrics'}, 'stale metrics'),
            ({'reason': 'stale metrics', 'metrics_timestamp': None}, 'stale metrics'),
            ({'reason': 'stale metrics', 'metrics_timestamp': 'yesterday'}, 'stale metrics'),
            ({'reason': 'queue too long'}, 'queue too long'),
            ({'reason': 'queue too long', 'unreasonable_threshold': 20}, 'queue too long (threshold 20)'),
            ({'reason': 'queue too long', 'node_queue_waiting': 31, 'unreasonable_threshold': None},
             'queue too long (31 waiting)'),
            ({'reason': 'no metrics'}, 'no metrics'),
            ({'reason': 'metrics unreadable'}, 'metrics unreadable'),
            ({}, 'n/a'),
            # Not a mapping at all: the value is the reason.
            ('metrics unreadable', 'metrics unreadable'),
        ]
        for entry, expected in cases:
            excluded = dict(capacity_excluded())
            excluded[_uuid(7)] = entry
            result = self._capacity(resources=self._with_excluded(excluded))
            self.assertEqual(0, result.exit_code, entry)
            self.assertEqual('    sf-7 : %s' % expected, self._not_reported(result.output)[1], entry)
            self.assertNotIn('None', result.output, entry)

    def test_hypervisor_without_an_entry_shows_its_state(self):
        # The server gives every active node a reason, so the old sentence's
        # two causes would be a false claim here.
        excluded = capacity_excluded()
        del excluded[_uuid(7)]
        resources = self._with_excluded(excluded)
        nodes = capacity_nodes()
        nodes[6]['state'] = 'error'
        output = self._capacity(resources=resources, nodes=nodes).output
        self.assertEqual(['    sf-6 : stale metrics (last published 312 seconds ago)',
                          '    sf-7 : no reason given (node state error)'],
                         self._not_reported(output))
        self.assertNotIn('queue length or stale metrics', output)
        simple = self._capacity(resources=resources, nodes=nodes, output='simple').output
        self.assertEqual(['not_reported,sf-6,%s,stale metrics' % _uuid(6),
                          'not_reported,sf-7,%s,n/a' % _uuid(7)],
                         self._simple_section(simple, 'not_reported,name,uuid,reason'))
        for output in ['pretty', 'simple', 'json']:
            result = self._capacity(resources=resources, nodes=nodes, args=['sf-7'], output=output)
            self.assertEqual(1, result.exit_code)
            self.assertEqual('sf-7: not reported by the scheduler: no reason given (node state error)\n',
                             result.output)

    def test_hypervisor_without_an_entry_or_a_state(self):
        excluded = capacity_excluded()
        del excluded[_uuid(7)]
        nodes = capacity_nodes()
        del nodes[6]['state']
        resources = self._with_excluded(excluded)
        self.assertEqual('    sf-7 : no reason given (node state n/a)',
                         self._not_reported(self._capacity(resources=resources, nodes=nodes).output)[1])
        result = self._capacity(resources=resources, nodes=nodes, args=['sf-7'])
        self.assertEqual('sf-7: not reported by the scheduler: no reason given (node state n/a)\n',
                         result.output)

    def test_names_are_padded_to_the_longest(self):
        nodes = capacity_nodes()
        nodes[6]['name'] = 'sf-primary'
        self.assertEqual(['    sf-6       : stale metrics (last published 312 seconds ago)',
                          '    sf-primary : queue too long (31 waiting, threshold 20)'],
                         self._not_reported(self._capacity(nodes=nodes).output))

    def test_entries_for_unknown_and_non_hypervisor_nodes_are_omitted(self):
        excluded = dict(capacity_excluded())
        excluded[_uuid(42)] = {'reason': 'no metrics'}
        result = self._capacity(resources=self._with_excluded(excluded))
        self.assertEqual(0, result.exit_code)
        self.assertNotIn(_uuid(42), result.output)
        self.assertEqual(2, len(self._not_reported(result.output)))
        simple = self._capacity(resources=self._with_excluded(excluded), output='simple').output
        self.assertNotIn(_uuid(42), simple)
        self.assertNotIn(_uuid(8), simple)

    def test_filter_to_excluded_hypervisor_exits_1_with_its_reason(self):
        for output in ['pretty', 'simple', 'json']:
            result = self._capacity(args=['sf-7'], output=output)
            self.assertEqual(1, result.exit_code)
            self.assertEqual('sf-7: not reported by the scheduler: queue too long (31 waiting, threshold 20)\n',
                             result.output)

    def test_excluded_is_ignored_without_its_token(self):
        tokens = ('cluster-resources', 'cluster-capacity')
        output = self._capacity(capabilities=tokens).output
        self.assertIn('Hypervisors not reported by the scheduler (queue length or stale metrics):',
                      output.splitlines())
        self.assertNotIn(NOT_REPORTED_HEADING, output.splitlines())
        self.assertNotIn('stale metrics (last', output)
        simple = self._capacity(capabilities=tokens, output='simple').output
        self.assertIn('not_reported,sf-6,%s,n/a' % _uuid(6), simple.splitlines())
        result = self._capacity(capabilities=tokens, args=['sf-7'])
        self.assertEqual('sf-7: not reported by the scheduler (queue length or stale metrics)\n',
                         result.output)

    def test_ledger_is_ignored_without_its_token(self):
        tokens = ('cluster-resources', 'cluster-resources-excluded')
        resources = self._with_ledger(capacity_unread_ledger(degraded=True))
        output = self._capacity(resources=resources, capabilities=tokens).output
        self.assertNotIn(LEDGER_HEADING, output)
        self.assertNotIn(LEDGER_BANNER, output)
        simple = self._capacity(resources=resources, capabilities=tokens, output='simple').output
        self.assertNotIn('cluster_capacity', simple)

    def test_older_server_gets_exactly_the_old_view(self):
        for output in ['pretty', 'simple']:
            old = self._capacity(resources=capacity_resources(), output=output,
                                 capabilities=OLDER_SERVER_TOKENS)
            self.assertEqual(0, old.exit_code)
            self.assertNotIn(LEDGER_HEADING, old.output)
            self.assertNotIn('cluster_capacity', old.output)
            # The newer keys without their tokens, and the tokens without the
            # keys, both render the older server's view.
            self.assertEqual(old.output, self._capacity(output=output, capabilities=OLDER_SERVER_TOKENS).output)
            self.assertEqual(old.output, self._capacity(resources=capacity_resources(), output=output).output)

    def test_ledger_table(self):
        lines = self._capacity().output.splitlines()
        self.assertLess(lines.index(NOT_REPORTED_HEADING), lines.index(LEDGER_HEADING))
        self.assertEqual([['cpus', '180', '40', '12', '128'],
                          ['memory mb', '770048', '81920', '16384', '671744'],
                          ['disk gb', '3800', '2000', '100', '1700']],
                         _table_rows('\n'.join(lines), LEDGER_HEADING))
        self.assertNotIn(LEDGER_BANNER, lines)
        self.assertNotIn(LEDGER_UNWRITTEN, lines)

    def test_ledger_follows_the_totals_when_every_hypervisor_reports(self):
        nodes = [n for n in capacity_nodes() if n['name'] not in ('sf-6', 'sf-7')]
        lines = self._capacity(nodes=nodes).output.splitlines()
        self.assertNotIn(NOT_REPORTED_HEADING, lines)
        self.assertEqual(['', LEDGER_HEADING], lines[lines.index(LEDGER_HEADING) - 1:lines.index(LEDGER_HEADING) + 1])

    def test_unwritten_ledger_renders_dashes_and_says_why(self):
        lines = self._capacity(resources=self._with_ledger(capacity_unread_ledger(degraded=False))).output.splitlines()
        rows = _table_rows('\n'.join(lines), LEDGER_HEADING)
        self.assertEqual([['cpus', '-', '-', '-', '-'], ['memory mb', '-', '-', '-', '-'],
                          ['disk gb', '-', '-', '-', '-']], rows)
        self.assertEqual([LEDGER_UNWRITTEN, 'not yet guarded against the cluster total.'], lines[-2:])
        self.assertNotIn(LEDGER_BANNER, lines)

    def test_degraded_ledger_banner_comes_first(self):
        result = self._capacity(resources=self._with_ledger(capacity_unread_ledger(degraded=True)))
        self.assertEqual(0, result.exit_code)
        lines = result.output.splitlines()
        self.assertEqual([LEDGER_BANNER, 'zero. Read it again.', '', CPU_HEADING], lines[:4])
        self.assertEqual(['cpus', '-', '-', '-', '-'], _table_rows(result.output, LEDGER_HEADING)[0])
        self.assertNotIn(LEDGER_UNWRITTEN, lines)

    def test_both_banners_capacity_first(self):
        resources = capacity_degraded_resources()
        resources['cluster_capacity'] = capacity_unread_ledger(degraded=True)
        lines = self._capacity(resources=resources).output.splitlines()
        self.assertTrue(lines[0].startswith('WARNING: the scheduler could not read the capacity counters.'))
        self.assertEqual(LEDGER_BANNER, lines[4])

    def test_one_node_has_no_ledger_and_no_ledger_banner(self):
        resources = self._with_ledger(capacity_unread_ledger(degraded=True))
        for output in ['pretty', 'simple']:
            result = self._capacity(resources=resources, args=['sf-2'], output=output)
            self.assertEqual(0, result.exit_code)
            self.assertNotIn('ledger', result.output)
            self.assertNotIn('cluster_capacity', result.output)

    def test_simple_sections(self):
        result = self._capacity(output='simple')
        self.assertEqual(0, result.exit_code)
        self.assertEqual(['not_reported,sf-6,%s,stale metrics' % _uuid(6),
                          'not_reported,sf-7,%s,queue too long' % _uuid(7)],
                         self._simple_section(result.output, 'not_reported,name,uuid,reason'))
        ledger = self._simple_section(result.output, 'cluster_capacity,key,value')
        self.assertEqual(['cluster_capacity,%s' % key for key in node_cmd.CAPACITY_LEDGER_KEYS],
                         [line.rsplit(',', 1)[0] for line in ledger])
        self.assertEqual(['cluster_capacity,row_present,True', 'cluster_capacity,degraded,False',
                          'cluster_capacity,updated_at,1792886100.0', 'cluster_capacity,total_cpus,180'],
                         ledger[:4])
        self.assertEqual('cluster_capacity,unallocated_disk_gb,1700', ledger[-1])

    def test_simple_ledger_keys_are_the_servers(self):
        self.assertEqual(sorted(capacity_ledger()), sorted(node_cmd.CAPACITY_LEDGER_KEYS))

    def test_simple_unwritten_and_missing_ledger_figures(self):
        ledger = capacity_unread_ledger(degraded=False)
        del ledger['unallocated_disk_gb']
        lines = self._simple_section(self._capacity(resources=self._with_ledger(ledger), output='simple').output,
                                     'cluster_capacity,key,value')
        self.assertEqual('cluster_capacity,row_present,False', lines[0])
        self.assertEqual('cluster_capacity,total_cpus,-', lines[3])
        self.assertEqual('cluster_capacity,unallocated_disk_gb,n/a', lines[-1])

    def test_json_is_the_payload_verbatim(self):
        result = self._capacity(output='json')
        self.assertEqual(capacity_current_resources(), json.loads(result.output))
