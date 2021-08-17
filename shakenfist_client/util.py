import sys


def filter_dict(d, allowed_keys):
    out = {}
    for key in allowed_keys:
        if key in d:
            out[key] = d[key]
    return out


def show_interface(ctx, interface, out=[]):
    if not interface:
        print('Interface not found')
        sys.exit(1)

    if ctx.obj['OUTPUT'] == 'json':
        if 'network_interfaces' not in out:
            out['network_interfaces'] = []

        out['network_interfaces'].append(
            filter_dict(
                interface, ['uuid', 'network_uuid', 'macaddr', 'order',
                            'ipv4', 'floating', 'model']))
        return

    if ctx.obj['OUTPUT'] == 'pretty':
        format_string = '    %-8s: %s'
        print(format_string % ('uuid', interface['uuid']))
        print(format_string % ('network', interface['network_uuid']))
        print(format_string % ('macaddr', interface['macaddr']))
        print(format_string % ('order', interface['order']))
        print(format_string % ('ipv4', interface.get('ipv4', '')))
        print(format_string % ('floating', interface.get('floating', '')))
        print(format_string % ('model', interface['model']))
    else:
        print('iface,%s,%s,%s,%s,%s,%s,%s'
              % (interface['uuid'], interface['network_uuid'],
                 interface['macaddr'], interface['order'], interface.get(
                     'ipv4', ''),
                 interface.get('floating', ''), interface['model']))


def get_networks(ctx, args, incomplete):
    choices = [i['uuid'] for i in ctx.obj['CLIENT'].get_networks()]
    return [arg for arg in choices if arg.startswith(incomplete)]
