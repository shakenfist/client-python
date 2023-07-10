import click
import datetime
import json
from prettytable import PrettyTable
import sys
import time

from shakenfist_client import util


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
    print(format_string % ('etcd master', n['is_etcd_master']))
    print(format_string % ('hypervisor', n['is_hypervisor']))
    print(format_string % ('network node', n['is_network_node']))
    print(format_string % ('event log node', n['is_eventlog_node']))
    print(format_string % ('cluster maintainer', n['is_cluster_maintainer']))

    print()
    if ctx.obj['OUTPUT'] == 'pretty':
        format_string = '    %-8s: %s'
        print('Metadata:')
        for key in metadata:
            print(format_string % (key, metadata[key]))
    else:
        print('metadata,key,value')
        for key in metadata:
            print('metadata,%s,%s' % (key, metadata[key]))


def _roles_to_string(n):
    roles = []
    for role, symbol in [('is_etcd_master', 'D'), ('is_hypervisor', 'H'),
                         ('is_network_node', 'N'), ('is_eventlog_node', 'E'),
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
        print('Roles: D = etcd master; H = hypervisor; N = network node;')
        print('       E = eventlog node, M = cluster maintenance node')
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
            print('%s,%s,%s,%s,%s,%s' % (
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


@node.command(name='resources', help='Display resources for a node')
@click.argument('node', type=click.STRING, shell_complete=_get_nodes)
@click.pass_context
def node_resources(ctx, node=None):
    event = ctx.obj['CLIENT'].get_node_events(node, event_type='resources', limit=1)[0]
    if not event:
        print('No resources event found')

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['node', 'resource', 'value']
        for resource in event.get('extra'):
            x.add_row([node, resource, event['extra']['resource']])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('node,resource,value')
        for resource in event.get('extra'):
            print('%s,%s,%s' % (node, resource, event['extra']['resource']))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(event.get('extra'), indent=4, sort_keys=True))


# This command is primarily intended for a CI check to ensure we're not spinning
# too hard in a processing loop, but it may be useful elsewhere.
@node.command(name='cpuhogs', help='List processes consuming "too much" CPU')
@click.option('-t', '--threshold', default=0.25,
              help='The CPU fraction above which to complain.')
@click.pass_context
def node_cpuhogs(ctx, threshold=0.25):
    hogs = []

    for node in ctx.obj['CLIENT'].get_nodes():
        event = ctx.obj['CLIENT'].get_node_events(node['name'], event_type='resources', limit=1)[0]
        for resource in event.get('extra', {}):
            value = event['extra'][resource]
            if resource.startswith('process_cpu_fraction_') and value > threshold:
                hogs.append('%s on node %s has consumed %.02f of a CPU, threshold is %.02f'
                            % (resource[len('process_cpu_fraction_'):],
                               node['name'], value, threshold))

    if hogs:
        print('\n'.join(hogs))
        sys.exit(1)
