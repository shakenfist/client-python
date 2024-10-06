import datetime
import ipaddress
import json
import sys

import click
from prettytable import PrettyTable

from shakenfist_client import util
from shakenfist_client.apiclient import IncapableException


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

        print(json.dumps(nets, indent=4, sort_keys=True))


def _show_network(ctx, n):
    if not n:
        print('Network not found')
        sys.exit(1)

    metadata = ctx.obj['CLIENT'].get_network_metadata(n['uuid'])

    if ctx.obj['OUTPUT'] == 'json':
        n['metadata'] = metadata
        print(json.dumps(n, indent=4, sort_keys=True))
        return

    format_string = '%-16s: %s'
    if ctx.obj['OUTPUT'] == 'simple':
        format_string = '%s:%s'

    vxid = None
    if 'vxid' in n:
        vxid = n['vxid']
    else:
        vxid = n.get('vxlan_id')

    print(format_string % ('uuid', n['uuid']))
    print(format_string % ('name', n['name']))
    print(format_string % ('vxlan id', vxid))
    print(format_string % ('netblock', n['netblock']))
    print(format_string % ('provide dhcp', n['provide_dhcp']))
    print(format_string % ('provide nat', n['provide_nat']))
    print(format_string % ('provide DNS', n.get('provide_dns', False)))
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
            print(f'metadata,{key},{metadata[key]}')


@network.command(name='show', help='Show a network')
@click.argument('network_ref', type=click.STRING, shell_complete=util.get_networks)
@click.pass_context
def network_show(ctx, network_ref=None):
    _show_network(ctx, ctx.obj['CLIENT'].get_network(network_ref))


@network.command(name='create',
                 help="""
Create a network.

\b
NAME:             The name of the network
NETBLOCK:         The IP address block to use, as a CIDR range -- for
                  example 192.168.200.1/24
--dhcp/--no-dhcp: Should this network have DHCP?
--nat/--no-nat:   Should this network be able to access the Internet via
                  NAT?
--dns/--no-dns:   Should this network provide DNS entries for instances on
                  the virtual network?

\b
--namespace:      If you are an admin, you can create this object in a
                  different namespace.
""")
@click.argument('name', type=click.STRING)
@click.argument('netblock', type=click.STRING)
@click.option('--dhcp/--no-dhcp', default=True)
@click.option('--nat/--no-nat', default=True)
@click.option('--dns/--no-dns', default=False)
@click.option('--namespace', type=click.STRING)
@click.pass_context
def network_create(ctx, netblock=None, name=None, dhcp=None, nat=None, dns=None,
                   namespace=None):
    try:
        _show_network(ctx, ctx.obj['CLIENT'].allocate_network(
            netblock, dhcp, nat, name, namespace, provide_dns=dns))
    except IncapableException as e:
        if dns:
            # You asked for DNS and we can't do that
            raise e

        # Otherwise, we can just go with the previous default of no DNS.
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
@click.argument('network_ref', type=click.STRING, shell_complete=util.get_networks)
@click.option('-t', '--type', help='The event type to return')
@click.option('-l', '--limit', help='The maximum number of events to return')
@click.pass_context
def network_events(ctx, network_ref=None, type=None, limit=None):
    events = ctx.obj['CLIENT'].get_network_events(network_ref, event_type=type, limit=limit)
    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['timestamp', 'node', 'duration', 'message', 'extra']
        for e in events:
            e['timestamp'] = datetime.datetime.fromtimestamp(e['timestamp'])
            x.add_row([e['timestamp'], e['fqdn'], e['duration'],
                       e['message'], e.get('extra', '')])
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


@network.command(name='delete', help='Delete a network')
@click.argument('network_ref', type=click.STRING, shell_complete=util.get_networks)
@click.option('--namespace', type=click.STRING)
@click.pass_context
def network_delete(ctx, network_ref=None, namespace=None):
    out = ctx.obj['CLIENT'].delete_network(network_ref, namespace=None)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(out, indent=4, sort_keys=True))


@network.command(name='instances', help='List instances on a network')
@click.argument('network_ref',
                type=click.STRING, shell_complete=util.get_networks)
@click.pass_context
def network_list_instances(ctx, network_ref=None):
    interfaces = ctx.obj['CLIENT'].get_network_interfaces(network_ref)

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
        print(json.dumps(interfaces, indent=4, sort_keys=True))


@network.command(name='set-metadata', help='Set a metadata item')
@click.argument('network_ref', type=click.STRING, shell_complete=util.get_networks)
@click.argument('key', type=click.STRING)
@click.argument('value', type=click.STRING)
@click.pass_context
def network_set_metadata(ctx, network_ref=None, key=None, value=None):
    ctx.obj['CLIENT'].set_network_metadata_item(network_ref, key, value)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@network.command(name='delete-metadata', help='Delete a metadata item')
@click.argument('network_ref', type=click.STRING, shell_complete=util.get_networks)
@click.argument('key', type=click.STRING)
@click.pass_context
def network_delete_metadata(ctx, network_ref=None, key=None, value=None):
    ctx.obj['CLIENT'].delete_network_metadata_item(network_ref, key)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@network.command(name='ping', help='Ping on this network')
@click.argument('network_ref', type=click.STRING, shell_complete=util.get_networks)
@click.argument('address', type=click.STRING)
@click.pass_context
def network_ping(ctx, network_ref=None, address=None):
    output = ctx.obj['CLIENT'].ping(network_ref, address)
    if ctx.obj['OUTPUT'] in ['pretty', 'simple']:
        for line in output.get('stdout', '').split('\n'):
            print('stdout: %s' % line)
        for line in output.get('stderr', '').split('\n'):
            print('stderr: %s' % line)

    elif ctx.obj['OUTPUT'] == 'json':
        print(output)


@network.command(name='addresses', help='Display address allocations for a network')
@click.argument('network_ref', type=click.STRING, shell_complete=util.get_networks)
@click.pass_context
def network_addresses(ctx, network_ref=None):
    addresses = ctx.obj['CLIENT'].get_network_addresses(network_ref)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(addresses, indent=4, sort_keys=True))
        return

    info_by_addr = {}
    for addr in addresses:
        info_by_addr[int(ipaddress.IPv4Address(addr['address']))] = addr

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['address', 'type', 'user', 'comment']
    else:
        print('address,type,user,comment')

    for addr in sorted(info_by_addr.keys()):
        if not info_by_addr[addr]['user']:
            user = ''
        elif type(info_by_addr[addr]['user']) is list:
            user = ' '.join(info_by_addr[addr]['user'])
        else:
            user = info_by_addr[addr]['user']

        if ctx.obj['OUTPUT'] == 'pretty':
            x.add_row([info_by_addr[addr]['address'],
                       info_by_addr[addr]['type'],
                       user,
                       info_by_addr[addr]['comment']])
        else:
            print('%s,%s,%s,%s'
                  % (info_by_addr[addr]['address'],
                     info_by_addr[addr]['type'],
                     ' '.join(info_by_addr[addr]['user']),
                     info_by_addr[addr]['comment']))

    if ctx.obj['OUTPUT'] == 'pretty':
        print(x)


@network.command(name='add-routed', help='Add a routed address to the network')
@click.argument('network_ref', type=click.STRING, shell_complete=util.get_networks)
@click.pass_context
def network_route(ctx, network_ref=None):
    routed = ctx.obj['CLIENT'].route_network_address(network_ref)
    print(routed)


@network.command(name='delete-routed', help='Remove a routed address from the network')
@click.argument('network_ref', type=click.STRING, shell_complete=util.get_networks)
@click.argument('address', type=click.STRING)
@click.pass_context
def network_unroute(ctx, network_ref=None, address=None):
    ctx.obj['CLIENT'].unroute_network_address(network_ref, address)
