import click
import datetime
import json
from prettytable import PrettyTable
import sys


from shakenfist_client import util


@click.group(help='Network commands')
def network():
    pass


@network.command(name='list', help='List networks')
@click.option('-a', '--all', is_flag=True,
              help='Include networks in error and deleted networks')
@click.pass_context
def network_list(ctx, all=False):
    nets = list(ctx.obj['CLIENT'].get_networks(all=all))

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['uuid', 'name', 'namespace', 'netblock', 'state']
        for n in nets:
            x.add_row([n['uuid'], n['name'], n['namespace'],
                       n['netblock'], n['state']])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('uuid,name,namespace,netblock')
        for n in nets:
            print('%s,%s,%s,%s,%s' %
                  (n['uuid'], n['name'], n['namespace'], n['netblock'], n['state']))

    elif ctx.obj['OUTPUT'] == 'json':
        filtered_nets = []
        for n in nets:
            filtered_nets.append(util.filter_dict(
                n, ['uuid', 'name', 'namespace', 'netblock', 'state']))
        print(json.dumps({'networks': filtered_nets},
                         indent=4, sort_keys=True))


def _show_network(ctx, n):
    if not n:
        print('Network not found')
        sys.exit(1)

    metadata = ctx.obj['CLIENT'].get_network_metadata(n['uuid'])

    if ctx.obj['OUTPUT'] == 'json':
        filtered = util.filter_dict(n, ['uuid', 'name', 'vxid', 'netblock',
                                        'provide_dhcp', 'provide_nat',
                                        'floating_gateway', 'namespace'])
        filtered['metadata'] = metadata
        print(json.dumps(filtered, indent=4, sort_keys=True))
        return

    format_string = '%-16s: %s'
    if ctx.obj['OUTPUT'] == 'simple':
        format_string = '%s:%s'

    print(format_string % ('uuid', n['uuid']))
    print(format_string % ('name', n['name']))
    print(format_string % ('vxlan id', n['vxid']))
    print(format_string % ('netblock', n['netblock']))
    print(format_string % ('provide dhcp', n['provide_dhcp']))
    print(format_string % ('provide nat', n['provide_nat']))
    print(format_string % ('floating gateway', n['floating_gateway']))
    print(format_string % ('namespace', n['namespace']))
    print(format_string % ('state', n['state']))

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


@network.command(name='show', help='Show a network')
@click.argument('network_uuid', type=click.STRING, autocompletion=util.get_networks)
@click.pass_context
def network_show(ctx, network_uuid=None):
    _show_network(ctx, ctx.obj['CLIENT'].get_network(network_uuid))


@network.command(name='create',
                 help=('Create a network.\n\n'
                       'NAME:             The name of the network\n'
                       'NETBLOCK:         The IP address block to use, as a CIDR\n'
                       '                  range -- for example 192.168.200.1/24\n'
                       '--dhcp/--no-dhcp: Should this network have DHCP?\n'
                       '--nat/--no-nat:   Should this network be able to access'
                       '                  the Internet via NAT?\n'
                       '\n'
                       '--namespace:     If you are an admin, you can create this object in a\n'
                       '                 different namespace.\n'))
@click.argument('name', type=click.STRING)
@click.argument('netblock', type=click.STRING)
@click.option('--dhcp/--no-dhcp', default=True)
@click.option('--nat/--no-nat', default=True)
@click.option('--namespace', type=click.STRING)
@click.pass_context
def network_create(ctx, netblock=None, name=None, dhcp=None, nat=None, namespace=None):
    _show_network(ctx, ctx.obj['CLIENT'].allocate_network(
        netblock, dhcp, nat, name, namespace))


@network.command(name='delete-all', help='Delete ALL networks')
@click.option('--confirm',  is_flag=True)
@click.option('--namespace', type=click.STRING)
@click.pass_context
def network_delete_all(ctx, confirm=False, namespace=None):
    if not confirm:
        print('You must be sure. Use option --confirm.')
        return

    ctx.obj['CLIENT'].delete_all_networks(namespace)


@network.command(name='events', help='Display events for a network')
@click.argument('network_uuid', type=click.STRING, autocompletion=util.get_networks)
@click.pass_context
def network_events(ctx, network_uuid=None):
    events = ctx.obj['CLIENT'].get_network_events(network_uuid)
    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['timestamp', 'node',
                         'operation', 'phase', 'duration', 'message']
        for e in events:
            e['timestamp'] = datetime.datetime.fromtimestamp(e['timestamp'])
            x.add_row([e['timestamp'], e['fqdn'], e['operation'], e['phase'],
                       e['duration'], e['message']])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('timestamp,node,operation,phase,duration,message')
        for e in events:
            e['timestamp'] = datetime.datetime.fromtimestamp(e['timestamp'])
            print('%s,%s,%s,%s,%s,%s'
                  % (e['timestamp'], e['fqdn'], e['operation'], e['phase'],
                     e['duration'], e['message']))

    elif ctx.obj['OUTPUT'] == 'json':
        filtered_events = []
        for e in events:
            filtered_events.append(util.filter_dict(
                e, ['timestamp', 'fqdn', 'operation', 'phase', 'duration', 'message']))
        print(json.dumps({'networks': filtered_events},
                         indent=4, sort_keys=True))


@network.command(name='delete', help='Delete a network')
@click.argument('network_uuid', type=click.STRING, autocompletion=util.get_networks)
@click.option('--namespace', type=click.STRING)
@click.pass_context
def network_delete(ctx, network_uuid=None, namespace=None):
    ctx.obj['CLIENT'].delete_network(network_uuid, namespace=None)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@network.command(name='instances', help='List instances on a network')
@click.argument('network_uuid',
                type=click.STRING, autocompletion=util.get_networks)
@click.pass_context
def network_list_instances(ctx, network_uuid=None):
    interfaces = ctx.obj['CLIENT'].get_network_interfaces(network_uuid)

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['instance_uuid', 'ipv4', 'floating']
        for ni in interfaces:
            x.add_row([ni['instance_uuid'], ni['ipv4'], ni['floating']])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('instance_uuid,ipv4,floating')
        for ni in interfaces:
            print('%s,%s,%s' %
                  (ni['instance_uuid'], ni['ipv4'], ni['floating']))

    elif ctx.obj['OUTPUT'] == 'json':
        filtered_ni = []
        for ni in interfaces:
            filtered_ni.append(util.filter_dict(
                ni, ['instance_uuid', 'ipv4', 'floating']))
        print(json.dumps({'instances': filtered_ni},
                         indent=4, sort_keys=True))


@network.command(name='set-metadata', help='Set a metadata item')
@click.argument('network_uuid', type=click.STRING, autocompletion=util.get_networks)
@click.argument('key', type=click.STRING)
@click.argument('value', type=click.STRING)
@click.pass_context
def network_set_metadata(ctx, network_uuid=None, key=None, value=None):
    ctx.obj['CLIENT'].set_network_metadata_item(network_uuid, key, value)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@network.command(name='delete-metadata', help='Delete a metadata item')
@click.argument('network_uuid', type=click.STRING, autocompletion=util.get_networks)
@click.argument('key', type=click.STRING)
@click.pass_context
def network_delete_metadata(ctx, network_uuid=None, key=None, value=None):
    ctx.obj['CLIENT'].delete_network_metadata_item(network_uuid, key)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@network.command(name='ping', help='Ping on this network')
@click.argument('network_uuid', type=click.STRING, autocompletion=util.get_networks)
@click.argument('address', type=click.STRING)
@click.pass_context
def network_ping(ctx, network_uuid=None, address=None):
    output = ctx.obj['CLIENT'].ping(network_uuid, address)
    if ctx.obj['OUTPUT'] in ['pretty', 'simple']:
        for line in output.get('stdout', '').split('\n'):
            print('stdout: %s' % line)
        for line in output.get('stderr', '').split('\n'):
            print('stderr: %s' % line)

    elif ctx.obj['OUTPUT'] == 'json':
        print(output)
