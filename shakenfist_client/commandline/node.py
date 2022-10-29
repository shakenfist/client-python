import click
import json
from prettytable import PrettyTable
import time


@click.group(help='Node commands')
def node():
    pass


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
        x.field_names = ['name', 'ip', 'lastseen', 'state', 'roles', 'version']
        for n in nodes:
            last_seen = '%s (%d seconds ago)' % (time.ctime(n['lastseen']),
                                                 time.time() - n['lastseen'])
            roles = _roles_to_string(n)
            x.add_row([n['name'], n['ip'], last_seen, n.get('state', ''),
                       roles, n['version']])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('name,ip,lastseen,state,roles,version')
        for n in nodes:
            roles = _roles_to_string(n)
            print('%s,%s,%s,%s,%s,%s' % (
                n['name'], n['ip'], n['lastseen'], n.get('state', ''),
                roles, n['version']))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(nodes, indent=4, sort_keys=True))


@node.command(name='delete', help='Delete a node')
@click.argument('node', type=click.STRING)
@click.pass_context
def network_delete(ctx, node=None):
    out = ctx.obj['CLIENT'].delete_node(node)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(out, indent=4, sort_keys=True))
