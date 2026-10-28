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

# Why a hypervisor is missing from per_node, from a server too old to say.
# Its response cannot tell a queue too long from metrics stale or absent, so
# this names both rather than guessing one.
CAPACITY_NOT_REPORTED = 'not reported by the scheduler (queue length or stale metrics)'

# The two exclusion reasons which carry details. The server's reasons are a
# closed set of exact strings (EXCLUSION_REASONS in shakenfist's
# shakenfist/scheduler.py), but one this client does not know is printed as
# sent rather than refused: the server is the authority on why.
CAPACITY_EXCLUSION_QUEUE_TOO_LONG = 'queue too long'
CAPACITY_EXCLUSION_STALE_METRICS = 'stale metrics'

# The cluster claims ledger's keys in simple output, in the order the server
# publishes them. As with CAPACITY_SIMPLE_KEYS, a key is only ever appended.
CAPACITY_LEDGER_DIMENSIONS = (('cpus', 'cpus'), ('memory_mb', 'memory mb'), ('disk_gb', 'disk gb'))
CAPACITY_LEDGER_KEYS = (
    ('row_present', 'degraded', 'updated_at')
    + tuple('%s_%s' % (figure, dim)
            for figure in ('total', 'claimed', 'unclaimed_used')
            for dim, _ in CAPACITY_LEDGER_DIMENSIONS)
    + tuple('unallocated_%s' % dim for dim, _ in CAPACITY_LEDGER_DIMENSIONS))

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


def _capacity_now():
    """Now, as the exclusion ages are computed against. Tests patch this."""
    return time.time()


def _capacity_is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _capacity_gated(resources, key, capable):
    """A key of /admin/resources which only a server with its capability sends.

    None when the server does not advertise the capability, or did not send a
    mapping under the key: either way this client renders what it rendered
    before the key existed.
    """
    if not capable:
        return None
    value = resources.get(key)
    if not isinstance(value, dict):
        return None
    return value


def _capacity_bare_reason(entry):
    """The reason string of one entry of `excluded`, as the server sent it."""
    if not isinstance(entry, dict):
        return entry
    return util.render_optional(entry, 'reason')


def _capacity_exclusion_reason(entry):
    """One entry of `excluded`, as a reason and whichever details it carries.

    A detail which is missing, or is not a number, is left out rather than
    rendered: an age computed from nothing is worse than no age.
    """
    reason = str(_capacity_bare_reason(entry))
    if not isinstance(entry, dict):
        return reason

    details = []
    if reason == CAPACITY_EXCLUSION_STALE_METRICS:
        # The server sends when the node last published, not an age, because
        # it may have held the reading for as long as its metrics cache lives.
        published = entry.get('metrics_timestamp')
        if _capacity_is_number(published):
            details.append('last published %d seconds ago' % (_capacity_now() - published))
    elif reason == CAPACITY_EXCLUSION_QUEUE_TOO_LONG:
        if entry.get('node_queue_waiting') is not None:
            details.append('%s waiting' % entry['node_queue_waiting'])
        if entry.get('unreasonable_threshold') is not None:
            details.append('threshold %s' % entry['unreasonable_threshold'])

    if details:
        return '%s (%s)' % (reason, ', '.join(details))
    return reason


def _capacity_reported_reason(n, excluded):
    """Why a hypervisor in the node list is missing, from a server which says.

    A server which publishes `excluded` gives every active node its queue or
    metrics reason, so a hypervisor with no entry is not one whose queue was
    long or whose metrics were stale, and naming those causes would be a
    false claim. It is most likely one which was not active when the
    scheduler last read its metrics, so its state from the node list is
    shown instead.
    """
    if n['uuid'] in excluded:
        return _capacity_exclusion_reason(excluded[n['uuid']])
    return 'no reason given (node state %s)' % util.render_optional(n, 'state')


def _capacity_pretty_not_reported(excluded, missing):
    if not missing:
        return
    print()
    if excluded is None:
        print('Hypervisors %s:' % CAPACITY_NOT_REPORTED)
        for n in missing:
            print('    %s' % n['name'])
        return

    # Names are padded to the longest in the list, so the reasons line up.
    print('Hypervisors not reported by the scheduler:')
    width = max(len(n['name']) for n in missing)
    for n in missing:
        print('    %-*s : %s' % (width, n['name'], _capacity_reported_reason(n, excluded)))


def _capacity_pretty_ledger_banner(ledger):
    if ledger is not None and ledger.get('degraded'):
        print('WARNING: the cluster claims ledger could not be read. Its figures are unknown, not')
        print('zero. Read it again.')
        print()


def _capacity_pretty_ledger(ledger):
    if ledger is None:
        return
    print()
    print('Cluster claims ledger:')
    x = PrettyTable()
    x.field_names = ['dimension', 'total', 'claimed', 'unclaimed used', 'unallocated']
    x.align = 'l'
    for dim, label in CAPACITY_LEDGER_DIMENSIONS:
        x.add_row([label] + [util.render_optional(ledger, '%s_%s' % (figure, dim))
                             for figure in ('total', 'claimed', 'unclaimed_used', 'unallocated')])
    print(x)
    # Compared with False: a ledger which never sent the flag has not said
    # that the row is missing.
    if ledger.get('row_present') is False and not ledger.get('degraded'):
        print('The reconciler has not written the cluster ledger yet, so unclaimed placements are')
        print('not yet guarded against the cluster total.')


def _capacity_pretty(resources, per_node, nodes, names, order, filtered, excluded, ledger):
    total = resources.get('total', {})
    if total.get('capacity_degraded'):
        print('WARNING: the scheduler could not read the capacity counters. Every committed figure')
        print('below reads as zero, and every limit as -, because of that failed read and not because')
        print('the nodes are idle. Read it again before acting on these figures.')
        print()
    if not filtered:
        _capacity_pretty_ledger_banner(ledger)

    _capacity_pretty_tables(per_node, order, names)

    # Everything after the tables is cluster-wide, so a view of one node
    # stops here.
    if filtered:
        return

    print()
    print('Total available: %s vCPUs, %s MB RAM'
          % (util.render_optional(total, 'cpu_available'), util.render_optional(total, 'ram_available')))
    _capacity_pretty_not_reported(excluded, _capacity_not_reported(nodes, per_node))
    _capacity_pretty_ledger(ledger)


def _capacity_simple_not_reported(excluded, missing):
    # The reason column is the server's bare reason. An older server gives
    # none, and neither does a newer one for a node it has no entry for, so
    # both are n/a: never sent.
    print()
    print('not_reported,name,uuid,reason')
    for n in missing:
        reason = 'n/a'
        if excluded is not None and n['uuid'] in excluded:
            reason = _capacity_bare_reason(excluded[n['uuid']])
        print('not_reported,%s,%s,%s' % (n['name'], n['uuid'], reason))


def _capacity_simple_ledger(ledger):
    if ledger is None:
        return
    print()
    print('cluster_capacity,key,value')
    for key in CAPACITY_LEDGER_KEYS:
        print('cluster_capacity,%s,%s' % (key, util.render_optional(ledger, key)))


def _capacity_simple(resources, per_node, nodes, names, order, filtered, excluded, ledger):
    degraded = util.render_optional(resources.get('total', {}), 'capacity_degraded')
    print(','.join(('name', 'uuid', 'capacity_degraded') + CAPACITY_SIMPLE_KEYS))
    for u in order:
        row = [names.get(u, u), u, degraded]
        row.extend(util.render_optional(per_node[u], key) for key in CAPACITY_SIMPLE_KEYS)
        print(','.join(str(f) for f in row))

    if filtered:
        return

    _capacity_simple_not_reported(excluded, _capacity_not_reported(nodes, per_node))
    _capacity_simple_ledger(ledger)


@node.command(name='capacity',
              help=('Show what each hypervisor has committed, measured and available, as the '
                    'scheduler admits against it.\n\n'
                    'NODE: Optionally, the name or UUID of one node to show.\n\n'
                    'Pretty output is three tables: CPU, memory and disk. In the CPU table, * marks the '
                    'charge (the larger of committed and measured) and the ceiling (the capacity row\'s '
                    'limit, or hard max for a node without a row) which the scheduler admits against. '
                    'n/a means this server does not send that figure, and - that it sent no value. '
                    'Only hypervisors are shown: a hypervisor the scheduler does not report is named '
                    'under the tables with the reason the server gives, and every other node is '
                    'omitted. A server too old to give reasons gets one sentence naming both possible '
                    'causes. The cluster claims ledger follows, for the whole cluster only; a server '
                    'without the cluster-capacity capability has no ledger, and none is printed. '
                    'Simple output carries every figure the server sends, one row per node, then the '
                    'not reported hypervisors and the ledger as sections. JSON output is the server\'s '
                    'own response, keyed by node UUID.'))
@click.argument('node', type=click.STRING, required=False, shell_complete=_get_nodes)
@click.pass_context
def node_capacity(ctx, node=None):
    client = ctx.obj['CLIENT']
    try:
        resources = client.get_cluster_resources()
    except IncapableException as e:
        print(str(e))
        sys.exit(1)

    nodes = list(client.get_nodes())
    names = {n['uuid']: n['name'] for n in nodes}
    per_node = resources.get('per_node', {})
    order = _capacity_order(per_node, names)

    # Rendered only when the server advertises them. Without the tokens the
    # view is exactly what it was before the server sent either key.
    excluded = _capacity_gated(resources, 'excluded', client.check_capability('cluster-resources-excluded'))
    ledger = _capacity_gated(resources, 'cluster_capacity', client.check_capability('cluster-capacity'))

    if node:
        order = [u for u in order if node in (u, names.get(u))]
        if not order:
            known = [n for n in nodes if node in (n['uuid'], n['name'])]
            if not known:
                print('Node %s not found' % node)
            elif not known[0].get('is_hypervisor'):
                print('%s is not a hypervisor, so the scheduler does not report it' % known[0]['name'])
            elif excluded is not None:
                print('%s: not reported by the scheduler: %s'
                      % (known[0]['name'], _capacity_reported_reason(known[0], excluded)))
            else:
                print('%s: %s' % (known[0]['name'], CAPACITY_NOT_REPORTED))
            sys.exit(1)

    if ctx.obj['OUTPUT'] == 'json':
        if node:
            resources = dict(resources)
            resources['per_node'] = {u: per_node[u] for u in order}
        print(json.dumps(resources, indent=4, sort_keys=True))
    elif ctx.obj['OUTPUT'] == 'simple':
        _capacity_simple(resources, per_node, nodes, names, order, bool(node), excluded, ledger)
    else:
        _capacity_pretty(resources, per_node, nodes, names, order, bool(node), excluded, ledger)


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
