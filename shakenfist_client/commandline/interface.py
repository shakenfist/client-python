import click
import json
import sys


from shakenfist_client import util


@click.group(help='Interface commands')
def interface():
    pass


def _get_instance_interfaces(ctx, args, incomplete):
    choices = []
    for i in ctx.obj['CLIENT'].get_instances():
        for interface in ctx.obj['CLIENT'].get_instance_interfaces(i['uuid']):
            choices.append(interface['uuid'])
    return [arg for arg in choices if arg.startswith(incomplete)]


@interface.command(name='show', help='Show an interface')
@click.argument('interface_uuid', type=click.STRING,
                shell_complete=_get_instance_interfaces)
@click.pass_context
def interface_show(ctx, interface_uuid=None):
    interface = ctx.obj['CLIENT'].get_interface(interface_uuid)

    if ctx.obj['OUTPUT'] == 'json':
        out = {'network_interfaces': []}
        util.show_interface(ctx, interface, out)
        print(json.dumps(out, indent=4, sort_keys=True))
        return

    if ctx.obj['OUTPUT'] == 'pretty':
        print('Interface:')
    else:
        print('iface,interface uuid,network uuid,'
              'macaddr,order,ipv4,floating,model')

    util.show_interface(ctx, interface)


@interface.command(name='float',
                   help='Add a floating IP to an interface')
@click.argument('interface_uuid', type=click.STRING,
                shell_complete=_get_instance_interfaces)
@click.pass_context
def interface_float(ctx, interface_uuid=None):
    ctx.obj['CLIENT'].float_interface(interface_uuid)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@interface.command(name='defloat',
                   help='Remove a floating IP to an interface')
@click.argument('interface_uuid', type=click.STRING,
                shell_complete=_get_instance_interfaces)
@click.pass_context
def interface_defloat(ctx, interface_uuid=None):
    ctx.obj['CLIENT'].defloat_interface(interface_uuid)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@interface.command(name='set-metadata', help='Set a metadata item')
@click.argument('interface_uuid', type=click.STRING, shell_complete=_get_instance_interfaces)
@click.argument('key', type=click.STRING)
@click.argument('value', type=click.STRING)
@click.pass_context
def interface_set_metadata(ctx, interface_uuid=None, key=None, value=None):
    if not ctx.obj['CLIENT'].check_capability('interface-metadata'):
        sys.stderr.write(
            'Unfortunately this server does not implement interface metadata.\n')
        sys.exit(1)
    ctx.obj['CLIENT'].set_interface_metadata_item(interface_uuid, key, value)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@interface.command(name='delete-metadata', help='Delete a metadata item')
@click.argument('interface_uuid', type=click.STRING, shell_complete=_get_instance_interfaces)
@click.argument('key', type=click.STRING)
@click.pass_context
def interface_delete_metadata(ctx, interface_uuid=None, key=None):
    if not ctx.obj['CLIENT'].check_capability('interface-metadata'):
        sys.stderr.write(
            'Unfortunately this server does not implement interface metadata.\n')
        sys.exit(1)
    ctx.obj['CLIENT'].delete_interface_metadata_item(interface_uuid, key)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')
