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
@click.pass_context
def artifact_events(ctx, node=None):
    events = ctx.obj['CLIENT'].get_node_events(node)
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
