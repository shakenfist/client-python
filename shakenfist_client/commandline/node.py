import datetime
import json
import sys
import time

import click
from prettytable import PrettyTable

from shakenfist_client import util
from shakenfist_client.apiclient import IncapableException


@click.group(help='Node commands')
def node():
    pass


def _get_nodes(ctx, args, incomplete):
    choices = [n['uuid'] for n in util.get_client(ctx).get_nodes()]
    return [arg for arg in choices if arg.startswith(incomplete)]


@node.command(name='show', help='Show a node')
@click.argument('node', type=click.STRING, shell_complete=_get_nodes)
@click.pass_context
def node_show(ctx, node=None):
    n = ctx.obj['CLIENT'].get_node(node)
    if not n:
        print('Node not found')
        sys.exit(1)

    if not ctx.obj['CLIENT'].check_capability('node-metadata'):
        metadata = {}
    else:
        metadata = ctx.obj['CLIENT'].get_node_metadata(n['name'])

    if ctx.obj['OUTPUT'] == 'json':
        n['metadata'] = metadata
        print(json.dumps(n, indent=4, sort_keys=True))
        return

    if ctx.obj['OUTPUT'] == 'simple':
        format_string = '%s:%s'
    else:
        format_string = '%-25s: %s'

    print(format_string % ('name', n['name']))
    print(format_string % ('ip', n['ip']))
    print(format_string % ('state', n['state']))
    print(format_string % ('last seen',
                           '%s (%d seconds ago)'
                           % (n['lastseen'], time.time() - n['lastseen'])))
    print(format_string % ('version', n['version']))
    print(format_string % ('release', n['release']))
    # is_database_node arrives with servers from v0.8; .get() keeps older
    # servers working. The etcd master and event log roles were vestigial
    # (always false) and are no longer shown.
    print(format_string % ('database node', n.get('is_database_node', False)))
    print(format_string % ('hypervisor', n['is_hypervisor']))
    print(format_string % ('network node', n['is_network_node']))
    print(format_string % ('cluster maintainer', n['is_cluster_maintainer']))

    daemon_states = {}
    for key in n:
        if key.startswith('daemon-') and key.endswith('-state'):
            daemon_states[key[len('daemon-'):-len('-state')]] = n[key]

    print()
    if ctx.obj['OUTPUT'] == 'pretty':
        format_string = '    %-8s: %s'
        print('Daemon states:')
        for name in sorted(daemon_states):
            print(format_string % (name, util.render_optional(daemon_states, name)))
        print()
        print('Metadata:')
        for key in metadata:
            print(format_string % (key, metadata[key]))
    else:
        print('daemon,name,state')
        for name in sorted(daemon_states):
            print(f'daemon,{name},{util.render_optional(daemon_states, name)}')
        print()
        print('metadata,key,value')
        for key in metadata:
            print(f'metadata,{key},{metadata[key]}')


def _roles_to_string(n):
    roles = []
    for role, symbol in [('is_database_node', 'D'), ('is_hypervisor', 'H'),
                         ('is_network_node', 'N'),
                         ('is_cluster_maintainer', 'M')]:
        if n.get(role, False):
            roles.append(symbol)
        else:
            roles.append(' ')
    return ''.join(roles)


@node.command(name='list', help='List nodes')
@click.pass_context
def node_list(ctx):
    nodes = list(ctx.obj['CLIENT'].get_nodes())

    if ctx.obj['OUTPUT'] == 'pretty':
        print('Roles: D = database node; H = hypervisor; N = network node;')
        print('       M = cluster maintenance node')
        print()

        x = PrettyTable()
        x.field_names = ['name', 'ip', 'lastseen', 'state', 'roles', 'release']
        for n in nodes:
            last_seen = '%s (%d seconds ago)' % (time.ctime(n['lastseen']),
                                                 time.time() - n['lastseen'])
            roles = _roles_to_string(n)
            release = n.get('release')
            if not release:
                release = n.get('version')
            x.add_row([n['name'], n['ip'], last_seen, n.get('state', ''),
                       roles, release])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('name,ip,lastseen,state,roles,version')
        for n in nodes:
            roles = _roles_to_string(n)
            release = n.get('release')
            if not release:
                release = n.get('version')
            print('{},{},{},{},{},{}'.format(
                n['name'], n['ip'], n['lastseen'], n.get('state', ''),
                roles, release))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(nodes, indent=4, sort_keys=True))


@node.command(name='delete', help='Delete a node')
@click.argument('node', type=click.STRING, shell_complete=_get_nodes)
@click.pass_context
def network_delete(ctx, node=None):
    out = ctx.obj['CLIENT'].delete_node(node)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(out, indent=4, sort_keys=True))


@node.command(name='events', help='Display events for a node')
@click.argument('node', type=click.STRING, shell_complete=_get_nodes)
@click.option('-t', '--type', help='The event type to return')
@click.option('-l', '--limit', help='The maximum number of events to return')
@click.pass_context
def node_events(ctx, node=None, type=None, limit=None):
    events = ctx.obj['CLIENT'].get_node_events(node, event_type=type, limit=limit)
    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['timestamp', 'node', 'duration', 'message', 'extra']
        for e in events:
            e['timestamp'] = datetime.datetime.fromtimestamp(e['timestamp'])
            x.add_row([e['timestamp'], e['fqdn'], e['duration'], e['message'],
                       e.get('extra', '')])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('timestamp,node,duration,message,extra')
        for e in events:
            e['timestamp'] = datetime.datetime.fromtimestamp(e['timestamp'])
            print('%s,%s,%s,%s,%s'
                  % (e['timestamp'], e['fqdn'], e['duration'], e['message'],
                     e.get('extra', '')))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(events, indent=4, sort_keys=True))


@node.command(name='resources',
              help='Display resources for a node. This is the node\'s last raw measurement; '
                   '"node capacity" shows what the scheduler admits against.')
@click.argument('node', type=click.STRING, shell_complete=_get_nodes)
@click.pass_context
def node_resources(ctx, node=None):
    events = ctx.obj['CLIENT'].get_node_events(node, event_type='resources', limit=1)
    if not events:
        print('No resources event found')
        sys.exit(1)
    event = events[0]

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['node', 'resource', 'value']
        for resource in event.get('extra'):
            x.add_row([node, resource, event['extra'][resource]])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('node,resource,value')
        for resource in event.get('extra'):
            print('{},{},{}'.format(node, resource, event['extra'][resource]))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(event.get('extra'), indent=4, sort_keys=True))


# Every per_node key of /admin/resources, in the order the server publishes
# them, which is the column order of simple output. Scripts read that output
# by position, so a key is only ever appended here, never inserted.
CAPACITY_SIMPLE_KEYS = (
    'cpu_max_per_instance', 'cpu_schedulable', 'cpu_reservation_clamped',
    'memory_reservation_clamped', 'cpu_committed_row_present', 'cpu_limit',
    'cpu_hard_max', 'cpu_measured', 'domains_enumerated_at', 'cpu_committed',
    'cpu_available', 'cpu_load_1', 'cpu_load_5', 'cpu_load_15',
    'memory_reserved_mb', 'ram_max_per_instance', 'ram_max', 'ram_committed',
    'ram_available', 'disk_available', 'disk_limit_gb', 'disk_committed_gb',
    'instances_total', 'instances_active')

# Why a hypervisor is missing from per_node, when the server does not say.
# Either its queue was too long or its metrics were stale or absent, and
# nothing in the response tells those apart, so this names both rather than
# guessing one.
CAPACITY_NOT_REPORTED = 'not reported by the scheduler (queue length or stale metrics)'

_CAPACITY_BINDING_INPUTS = (
    'cpu_measured', 'cpu_committed', 'cpu_committed_row_present', 'cpu_hard_max', 'cpu_limit')


def _capacity_binding_cpu(n):
    """The per_node CPU keys the scheduler admits this node against.

    The arithmetic is copied from shakenfist's shakenfist/scheduler.py, where
    _has_sufficient_cpu() refuses a node when charge + requested exceeds the
    ceiling, and summarize_resources() publishes the same charge. The charge
    is the larger of measured and committed (both, on a tie). The ceiling is
    the capacity row's own limit when the node has a row, and hard max only
    when it does not: the limit replaces hard max rather than joining it. The
    guarded UPDATE in place_instance() tests committed against the same
    limit, so the pre-filter is the stricter of the two and is what binds. A
    change to that arithmetic on the server must change it here.

    A wrong marker sends an operator to the wrong ledger, which is worse than
    none, so a payload missing any input marks nothing.
    """
    if any(key not in n for key in _CAPACITY_BINDING_INPUTS):
        return set()

    if not n['cpu_committed_row_present'] or n['cpu_limit'] is None:
        # No row: nothing committed is charged, and hard max is the ceiling.
        return {'cpu_measured', 'cpu_hard_max'}

    binding = {'cpu_limit'}
    charge = max(n['cpu_measured'], n['cpu_committed'])
    if n['cpu_measured'] == charge:
        binding.add('cpu_measured')
    if n['cpu_committed'] == charge:
        binding.add('cpu_committed')
    return binding


def _capacity_cpu_cell(n, key, binding):
    value = util.render_optional(n, key)
    if key in binding:
        return '%s*' % value
    return value


def _capacity_is_negative(n, key):
    value = n.get(key)
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value < 0


def _capacity_notes(n, clamp_key=None, clamp_note=None, available_key=None):
    notes = []
    # Compared with False rather than tested for falsehood: an older server
    # which never sent the flag has not said that the row is missing.
    if n.get('cpu_committed_row_present') is False:
        notes.append('no capacity row')
    if clamp_key and n.get(clamp_key):
        notes.append(clamp_note)
    if available_key and _capacity_is_negative(n, available_key):
        notes.append('over limit')
    return ', '.join(notes)


def _capacity_order(per_node, names):
    # By name, with any uuid the node list does not know last, as itself. A
    # guessed name would mislabel the row; the uuid cannot.
    return sorted(per_node, key=lambda u: (u not in names, names.get(u, u)))


def _capacity_not_reported(nodes, per_node):
    """The hypervisors in the node list which are missing from per_node."""
    return sorted((n for n in nodes if n.get('is_hypervisor') and n['uuid'] not in per_node),
                  key=lambda n: n['name'])


def _capacity_pretty_tables(per_node, order, names):
    print('CPU (vCPUs; * marks what the scheduler admits against):')
    x = PrettyTable()
    x.field_names = ['node', 'committed', 'measured', 'limit', 'hard max', 'available',
                     'schedulable', 'load 1/5/15', 'notes']
    x.align = 'l'
    for u in order:
        n = per_node[u]
        binding = _capacity_binding_cpu(n)
        x.add_row([names.get(u, u),
                   _capacity_cpu_cell(n, 'cpu_committed', binding),
                   _capacity_cpu_cell(n, 'cpu_measured', binding),
                   _capacity_cpu_cell(n, 'cpu_limit', binding),
                   _capacity_cpu_cell(n, 'cpu_hard_max', binding),
                   util.render_optional(n, 'cpu_available'),
                   util.render_optional(n, 'cpu_schedulable'),
                   '%s / %s / %s' % (util.render_optional(n, 'cpu_load_1'),
                                     util.render_optional(n, 'cpu_load_5'),
                                     util.render_optional(n, 'cpu_load_15')),
                   _capacity_notes(n, 'cpu_reservation_clamped', 'cpu clamped', 'cpu_available')])
    print(x)
    print()

    print('Memory (MB):')
    x = PrettyTable()
    x.field_names = ['node', 'committed', 'available', 'max', 'reserved', 'max per instance', 'notes']
    x.align = 'l'
    for u in order:
        n = per_node[u]
        x.add_row([names.get(u, u),
                   util.render_optional(n, 'ram_committed'),
                   util.render_optional(n, 'ram_available'),
                   util.render_optional(n, 'ram_max'),
                   util.render_optional(n, 'memory_reserved_mb'),
                   util.render_optional(n, 'ram_max_per_instance'),
                   _capacity_notes(n, 'memory_reservation_clamped', 'memory clamped', 'ram_available')])
    print(x)
    print()

    print('Disk (GB):')
    x = PrettyTable()
    x.field_names = ['node', 'committed', 'limit', 'available', 'notes']
    x.align = 'l'
    for u in order:
        n = per_node[u]
        x.add_row([names.get(u, u),
                   util.render_optional(n, 'disk_committed_gb'),
                   util.render_optional(n, 'disk_limit_gb'),
                   util.render_optional(n, 'disk_available'),
                   _capacity_notes(n, available_key='disk_available')])
    print(x)


def _capacity_pretty_not_reported(resources, missing):
    if not missing:
        return
    print()
    print('Hypervisors %s:' % CAPACITY_NOT_REPORTED)
    for n in missing:
        print('    %s' % n['name'])


def _capacity_pretty(resources, per_node, nodes, names, order, filtered):
    total = resources.get('total', {})
    if total.get('capacity_degraded'):
        print('WARNING: the scheduler could not read the capacity counters. Every committed figure')
        print('below reads as zero, and every limit as -, because of that failed read and not because')
        print('the nodes are idle. Read it again before acting on these figures.')
        print()

    _capacity_pretty_tables(per_node, order, names)

    # Everything after the tables is cluster-wide, so a view of one node
    # stops here.
    if filtered:
        return

    print()
    print('Total available: %s vCPUs, %s MB RAM'
          % (util.render_optional(total, 'cpu_available'), util.render_optional(total, 'ram_available')))
    _capacity_pretty_not_reported(resources, _capacity_not_reported(nodes, per_node))


def _capacity_simple_not_reported(resources, missing):
    # The server gave no reason, so the reason column is n/a: never sent.
    print()
    print('not_reported,name,uuid,reason')
    for n in missing:
        print('not_reported,%s,%s,n/a' % (n['name'], n['uuid']))


def _capacity_simple(resources, per_node, nodes, names, order, filtered):
    degraded = util.render_optional(resources.get('total', {}), 'capacity_degraded')
    print(','.join(('name', 'uuid', 'capacity_degraded') + CAPACITY_SIMPLE_KEYS))
    for u in order:
        row = [names.get(u, u), u, degraded]
        row.extend(util.render_optional(per_node[u], key) for key in CAPACITY_SIMPLE_KEYS)
        print(','.join(str(f) for f in row))

    if filtered:
        return

    _capacity_simple_not_reported(resources, _capacity_not_reported(nodes, per_node))


@node.command(name='capacity',
              help=('Show what each hypervisor has committed, measured and available, as the '
                    'scheduler admits against it.\n\n'
                    'NODE: Optionally, the name or UUID of one node to show.\n\n'
                    'Pretty output is three tables: CPU, memory and disk. In the CPU table, * marks the '
                    'charge (the larger of committed and measured) and the ceiling (the capacity row\'s '
                    'limit, or hard max for a node without a row) which the scheduler admits against. '
                    'n/a means this server does not send that figure, and - that it sent no value. '
                    'Only hypervisors are shown: a hypervisor the scheduler does not report is named '
                    'under the tables, and every other node is omitted. Simple output carries every '
                    'figure the server sends, one row per node, and JSON output is the server\'s own '
                    'response, keyed by node UUID.'))
@click.argument('node', type=click.STRING, required=False, shell_complete=_get_nodes)
@click.pass_context
def node_capacity(ctx, node=None):
    try:
        resources = ctx.obj['CLIENT'].get_cluster_resources()
    except IncapableException as e:
        print(str(e))
        sys.exit(1)

    nodes = list(ctx.obj['CLIENT'].get_nodes())
    names = {n['uuid']: n['name'] for n in nodes}
    per_node = resources.get('per_node', {})
    order = _capacity_order(per_node, names)

    if node:
        order = [u for u in order if node in (u, names.get(u))]
        if not order:
            known = [n for n in nodes if node in (n['uuid'], n['name'])]
            if not known:
                print('Node %s not found' % node)
            elif known[0].get('is_hypervisor'):
                print('%s: %s' % (known[0]['name'], CAPACITY_NOT_REPORTED))
            else:
                print('%s is not a hypervisor, so the scheduler does not report it' % known[0]['name'])
            sys.exit(1)

    if ctx.obj['OUTPUT'] == 'json':
        if node:
            resources = dict(resources)
            resources['per_node'] = {u: per_node[u] for u in order}
        print(json.dumps(resources, indent=4, sort_keys=True))
    elif ctx.obj['OUTPUT'] == 'simple':
        _capacity_simple(resources, per_node, nodes, names, order, bool(node))
    else:
        _capacity_pretty(resources, per_node, nodes, names, order, bool(node))


# This command is primarily intended for a CI check to ensure we're not spinning
# too hard in a processing loop, but it may be useful elsewhere.
@node.command(name='cpuhogs', help='List processes consuming "too much" CPU')
@click.option('-t', '--threshold', default=0.25,
              help='The CPU fraction above which to complain.')
@click.option('-i', '--ignore', default='',
              help='A comma separated list of processes to ignore')
@click.pass_context
def node_cpuhogs(ctx, threshold=0.25, ignore=None):
    hogs = []
    ignore_processes = ignore.split(',')

    for node in ctx.obj['CLIENT'].get_nodes():
        try:
            metrics = ctx.obj['CLIENT'].get_node_process_metrics(node['name'])
            for resource in metrics:
                value = metrics[resource]
                if resource.startswith('process_cpu_fraction_') and value > threshold:
                    process = resource[len('process_cpu_fraction_'):]
                    if process in ignore_processes:
                        continue
                    hogs.append('%s on node %s has consumed %.02f of a CPU, threshold is %.02f'
                                % (process, node['name'], value, threshold))

        except IncapableException:
            # Fall back to the old way
            event = ctx.obj['CLIENT'].get_node_events(node['name'], event_type='resources',
                                                      limit=1)[0]
            for resource in event.get('extra', {}):
                value = event['extra'][resource]
                if resource.startswith('process_cpu_fraction_') and value > threshold:
                    process = resource[len('process_cpu_fraction_'):]
                    if process in ignore_processes:
                        continue
                    hogs.append('%s on node %s has consumed %.02f of a CPU, threshold is %.02f'
                                % (process, node['name'], value, threshold))

    if hogs:
        print('\n'.join(hogs))
        sys.exit(1)
