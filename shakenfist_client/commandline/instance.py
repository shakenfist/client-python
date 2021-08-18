import base64
import click
import datetime
import json
from prettytable import PrettyTable
import sys


from shakenfist_client import util


@click.group(help='Instance commands')
def instance():
    pass


def _get_instances(ctx, args, incomplete):
    choices = [i['uuid'] for i in ctx.obj['CLIENT'].get_instances()]
    return [arg for arg in choices if arg.startswith(incomplete)]


def _summarize_interfaces(ctx, instance):
    interfaces = []
    if instance.get('interfaces'):
        interfaces = instance['interfaces']
    else:
        interfaces = ctx.obj['CLIENT'].get_instance_interfaces(
            instance['uuid'])

    out = []
    for iface in interfaces:
        if iface.get('floating'):
            out.append('%s: %s (%s)'
                       % (iface['order'], iface.get('ipv4', ''), iface.get('floating', '')))
        else:
            addr = iface.get('ipv4')
            if addr is None:
                addr = 'No address assigned'
            out.append('%s: %s' % (iface['order'], addr))
    return out


@instance.command(name='list', help='List instances')
@click.option('-a', '--all', is_flag=True,
              help='Include instances in error and deleted instances')
@click.pass_context
def instance_list(ctx, all=False):
    insts = ctx.obj['CLIENT'].get_instances(all=all)

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['uuid', 'name', 'namespace',
                         'cpus', 'memory', 'hypervisor',
                         'power state', 'state', 'interfaces']
        for i in insts:
            ifaces = _summarize_interfaces(ctx, i)
            x.add_row([i['uuid'], i['name'], i['namespace'],
                       i['cpus'], i['memory'], i['node'],
                       i.get('power_state', 'unknown'), i['state'],
                       '\n'.join(ifaces)])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('uuid,name,namespace,cpus,memory,hypervisor,power state,state')
        for i in insts:
            ifaces = _summarize_interfaces(ctx, i)
            print('%s,%s,%s,%s,%s,%s,%s,%s,%s'
                  % (i['uuid'], i['name'], i['namespace'],
                     i['cpus'], i['memory'], i['node'],
                     i.get('power_state', 'unknown'), i['state'],
                     ';'.join(ifaces)))

    elif ctx.obj['OUTPUT'] == 'json':
        filtered_insts = []
        for i in insts:
            filtered_insts.append(util.filter_dict(
                i, ['uuid', 'name', 'namespace', 'cpus', 'memory', 'node',
                    'power_state', 'state']))
        print(json.dumps({'instances': filtered_insts},
                         indent=4, sort_keys=True))


def _pretty_data(row, space_rules):
    ret = ''
    for key in space_rules:
        ret += key + '=' + str(row.get(key, '')).ljust(space_rules[key]) + '  '
    return ret


def _pretty_dict(lead_space, rows, space_rules):
    ret = ''

    if rows:
        ret += _pretty_data(rows[0], space_rules)
    for r in rows[1:]:
        ret += '\n'.ljust(lead_space + 1) + _pretty_data(r, space_rules)

    return ret


def _show_instance(ctx, i, include_snapshots=False):
    if not i:
        print('Instance not found')
        sys.exit(1)

    metadata = ctx.obj['CLIENT'].get_instance_metadata(i['uuid'])
    interfaces = ctx.obj['CLIENT'].get_instance_interfaces(i['uuid'])
    if include_snapshots:
        snapshots = ctx.obj['CLIENT'].get_instance_snapshots(i['uuid'])

    if ctx.obj['OUTPUT'] == 'json':
        out = util.filter_dict(i, ['uuid', 'name', 'namespace', 'cpus', 'memory',
                                   'disk_spec', 'video', 'node', 'console_port',
                                   'vdi_port', 'ssh_key', 'user_data',
                                   'power_state', 'state', 'uefi'])
        out['network_interfaces'] = []
        for interface in interfaces:
            util.show_interface(ctx, interface, out)

        out['metadata'] = metadata

        if include_snapshots:
            out['snapshots'] = []
            for snap in snapshots:
                out['snapshots'].append(util.filter_dict(
                    snap, ['uuid', 'device', 'created']))

        print(json.dumps(out, indent=4, sort_keys=True))
        return

    if ctx.obj['OUTPUT'] == 'simple':
        format_string = '%s:%s'
    else:
        format_string = '%-13s: %s'
        d_space = {'type': 5, 'bus': 4, 'size': 2, 'base': 0}
        v_space = {'model': 0, 'memory': 0}

    print(format_string % ('uuid', i['uuid']))
    print(format_string % ('name', i['name']))
    print(format_string % ('namespace', i['namespace']))
    print(format_string % ('cpus', i['cpus']))
    print(format_string % ('memory', i['memory']))
    print(format_string % ('uefi', i.get('uefi', False)))
    if ctx.obj['OUTPUT'] == 'pretty':
        print(format_string % ('disk spec',
                               _pretty_dict(15, i['disk_spec'], d_space)))
    if ctx.obj['OUTPUT'] == 'pretty':
        print(format_string % ('video',
                               _pretty_dict(15, (i['video'],), v_space)))
    print(format_string % ('node', i.get('node', '')))
    print(format_string % ('power state', i.get('power_state', '')))
    print(format_string % ('state', i.get('state', '')))
    print(format_string % ('error message', i.get('error_message', '')))

    # NOTE(mikal): I am not sure we should expose this, but it will do
    # for now until a proxy is written.
    print(format_string % ('console port', i.get('console_port', '')))
    print(format_string % ('vdi port', i.get('vdi_port', '')))

    print()
    print(format_string % ('ssh key', i['ssh_key']))
    print(format_string % ('user data', i['user_data']))

    if ctx.obj['OUTPUT'] == 'simple':
        print()
        print('disk_spec,type,bus,size,base')
        for d in i['disk_spec']:
            print('disk_spec,%s,%s,%s,%s' % (
                d['type'], d['bus'], d['size'], d['base']))

    if ctx.obj['OUTPUT'] == 'simple':
        print()
        print('video,model,memory')
        print('video,%s,%s' % (i['video']['model'], i['video']['memory']))

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

    print()
    if ctx.obj['OUTPUT'] == 'pretty':
        print('Interfaces:')
        for interface in interfaces:
            print()
            util.show_interface(ctx, interface)

    else:
        print('iface,interface uuid,network uuid,macaddr,order,ipv4,floating,model')
        for interface in interfaces:
            util.show_interface(ctx, interface)

    if include_snapshots:
        print()
        if ctx.obj['OUTPUT'] == 'pretty':
            format_string = '    %-8s: %s'
            print('Snapshots:')
            for snap in snapshots:
                print()
                print(format_string % ('uuid', snap['uuid']))
                print(format_string % ('device', snap['device']))
                print(format_string % (
                    'created', datetime.datetime.fromtimestamp(snap['created'])))
        else:
            print('snapshot,uuid,device,created')
            for snap in snapshots:
                print('snapshot,%s,%s,%s'
                      % (snap['uuid'], snap['device'],
                         datetime.datetime.fromtimestamp(snap['created'])))


@instance.command(name='show', help='Show an instance')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.argument('snapshots', type=click.BOOL, default=False)
@click.pass_context
def instance_show(ctx, instance_uuid=None, snapshots=False):
    _show_instance(ctx, ctx.obj['CLIENT'].get_instance(
        instance_uuid), snapshots)


def _parse_spec(spec):
    if '@' not in spec:
        return spec, None
    return spec.split('@')


# TODO(mikal): this misses the detailed version of disk and network specs, as well
# as guidance on how to use the video command line. We need to rethink how we're
# doing this, as its getting pretty long.
@instance.command(name='create',
                  help=('Create an instance.\n\n'
                        'NAME:      The name of the instance.\n'
                        'CPUS:      The number of vCPUs for the instance.\n'
                        'MEMORY:    The amount of RAM for the instance in MB.\n'
                        '\n'
                        'Options (may be repeated, must be specified at least once):\n'
                        '--network/-n:     The short form definition of the network to attach\n'
                        '                  the instance to.\n'
                        '--floated/-f:     The short form definition of a network to attach to\n'
                        '                  with an implied floating IP assignment.\n'
                        '--networkspec/-N: A long form "networkspec" definition of a network\n'
                        '                  interface to create.\n'
                        '--disk/-d:        The disks attached to the instance, in this format:\n'
                        '                  size@image_url where size is in GB and @image_url\n'
                        '                  is optional.\n'
                        '--diskspec/-D:    A long form "diskspec" definition of a disk to create.\n'
                        '--video/-V:       The video configuration for the instance.\n'
                        '--sshkey/-i:      The path to a ssh public key to configure on the\n'
                        '                  instance via config drive / cloud-init.\n'
                        '--sshkeydata/-I:  A ssh public key as a string to configure on the\n'
                        '                   instance via config drive / cloud-init.\n'
                        '--userdata/-u:    The path to a file containing user data to provided\n'
                        '                  to the instance via config drive / cloud-init.'
                        '--encodeduserdata/-U:\n'
                        '                  Base64 encoded user data to provide to the instance\n'
                        '                  via config drive / cloud-init.\n'
                        '\n'
                        '--placement/-p:   Force placement of instance on specified node.\n'
                        '--namespace:      If you are an admin, you can create this object in a\n'
                        '                  different namespace.\n'
                        '--uefi:           Boot using UEFI instead of BIOS.\n'))
@click.argument('name', type=click.STRING)
@click.argument('cpus', type=click.INT)
@click.argument('memory', type=click.INT)
@click.option('-n', '--network', type=click.STRING, multiple=True,
              autocompletion=util.get_networks)
@click.option('-f', '--floated', type=click.STRING, multiple=True,
              autocompletion=util.get_networks)
@click.option('-N', '--networkspec', type=click.STRING, multiple=True)
@click.option('-d', '--disk', type=click.STRING, multiple=True)
@click.option('-D', '--diskspec', type=click.STRING, multiple=True)
@click.option('-i', '--sshkey', type=click.STRING)
@click.option('-I', '--sshkeydata', type=click.STRING)
@click.option('-u', '--userdata', type=click.STRING)
@click.option('-U', '--encodeduserdata', type=click.STRING)
@click.option('-p', '--placement', type=click.STRING)
@click.option('-V', '--videospec', type=click.STRING)
@click.option('--bios/--uefi', is_flag=True, default=True)
@click.option('--force', is_flag=True, default=False)
@click.option('--namespace', type=click.STRING)
@click.pass_context
def instance_create(ctx, name=None, cpus=None, memory=None, network=None, floated=None,
                    networkspec=None, disk=None, diskspec=None, sshkey=None, sshkeydata=None,
                    userdata=None, encodeduserdata=None, placement=None, videospec=None,
                    namespace=None, bios=True, force=False):
    if memory < 128 and not force:
        print('Specified memory size is %dMB. This is very small.' % memory)
        print('Use the --force flag if this is deliberate')
        return

    if len(disk) < 1 and len(diskspec) < 1:
        print('You must specify at least one disk')
        return

    # Because of the way click works, the logic for the BIOS vs UEFI boot flag is a
    # bit weird. Fix it up to what the API expects...
    uefi = not bios

    sshkey_content = None
    if sshkey:
        with open(sshkey) as f:
            sshkey_content = f.read()
    if sshkeydata:
        sshkey_content = sshkeydata

    userdata_content = None
    if userdata:
        with open(userdata) as f:
            userdata_content = f.read()
        userdata_content = str(base64.b64encode(
            userdata_content.encode('utf-8')), 'utf-8')
    if encodeduserdata:
        userdata_content = encodeduserdata

    diskdefs = []
    for d in disk:
        p = _parse_spec(d)
        size, base = p
        try:
            size_int = int(size)
        except Exception:
            print('Disk size is not an integer')
            return

        diskdefs.append({
            'size': size_int,
            'base': base,
            'bus': None,
            'type': 'disk',
        })
    for d in diskspec:
        defn = {}
        for elem in d.split(','):
            s = elem.split('=')
            if len(s) != 2:
                print('Error in disk specification -'
                      ' should be key=value: %s' % elem)
                return
            defn[s[0]] = s[1]
        diskdefs.append(defn)

    netdefs = []
    for n in floated:
        network_uuid, address = _parse_spec(n)
        netdefs.append({
            'network_uuid': network_uuid,
            'macaddress': None,
            'model': 'virtio',
            'float': True
        })
        if address:
            netdefs[-1]['address'] = address
    for n in network:
        network_uuid, address = _parse_spec(n)
        netdefs.append({
            'network_uuid': network_uuid,
            'macaddress': None,
            'model': 'virtio'
        })
        if address:
            netdefs[-1]['address'] = address
    for n in networkspec:
        defn = {}
        for elem in n.split(','):
            s = elem.split('=')
            if len(s) != 2:
                print('Error in network specification -'
                      ' should be key=value: %s' % elem)
                return

            value = s[1]
            if s[0] == 'float':
                value = s[1] in ['true', 'True']

            defn[s[0]] = value
        netdefs.append(defn)

    video = {'model': 'cirrus', 'memory': 16384}
    if videospec:
        for elem in videospec.split(','):
            s = elem.split('=')
            if len(s) != 2:
                print('Error in video specification - '
                      ' should be key=value: %s' % elem)
                return
            video[s[0]] = s[1]

    _show_instance(
        ctx,
        ctx.obj['CLIENT'].create_instance(name, cpus, memory, netdefs, diskdefs, sshkey_content,
                                          userdata_content, force_placement=placement,
                                          namespace=namespace, video=video, uefi=uefi))


@instance.command(name='delete', help='Delete an instance')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.pass_context
def instance_delete(ctx, instance_uuid=None):
    ctx.obj['CLIENT'].delete_instance(instance_uuid)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='delete-all', help='Delete ALL instances')
@click.option('--confirm',  is_flag=True)
@click.option('--namespace', type=click.STRING)
@click.pass_context
def instance_delete_all(ctx, confirm=False, namespace=None):
    if not confirm:
        print('You must be sure. Use option --confirm.')
        return

    ctx.obj['CLIENT'].delete_all_instances(namespace)


@instance.command(name='events', help='Display events for an instance')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.pass_context
def instance_events(ctx, instance_uuid=None):
    events = ctx.obj['CLIENT'].get_instance_events(instance_uuid)
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
        print(json.dumps({'events': filtered_events},
                         indent=4, sort_keys=True))


@instance.command(name='set-metadata', help='Set a metadata item')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.argument('key', type=click.STRING)
@click.argument('value', type=click.STRING)
@click.pass_context
def instance_set_metadata(ctx, instance_uuid=None, key=None, value=None):
    ctx.obj['CLIENT'].set_instance_metadata_item(instance_uuid, key, value)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='delete-metadata', help='Delete a metadata item')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.argument('key', type=click.STRING)
@click.pass_context
def instance_delete_metadata(ctx, instance_uuid=None, key=None):
    ctx.obj['CLIENT'].delete_instance_metadata_item(instance_uuid, key)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='reboot', help='Reboot instance')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.option('--hard/--soft', default=False)
@click.pass_context
def instance_reboot(ctx, instance_uuid=None, hard=False):
    ctx.obj['CLIENT'].reboot_instance(instance_uuid, hard=hard)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='poweron', help='Power on an instance')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.pass_context
def instance_power_on(ctx, instance_uuid=None):
    ctx.obj['CLIENT'].power_on_instance(instance_uuid)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='poweroff', help='Power off an instance')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.pass_context
def instance_power_off(ctx, instance_uuid=None):
    ctx.obj['CLIENT'].power_off_instance(instance_uuid)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='pause', help='Pause an instance')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.pass_context
def instance_pause(ctx, instance_uuid=None):
    ctx.obj['CLIENT'].pause_instance(instance_uuid)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='unpause', help='Unpause an instance')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.pass_context
def instance_unpause(ctx, instance_uuid=None):
    ctx.obj['CLIENT'].unpause_instance(instance_uuid)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='consoledata', help='Get console data for an instance')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.argument('length', type=click.INT, default=10240)
@click.pass_context
def instance_consoledata(ctx, instance_uuid=None, length=None):
    print(ctx.obj['CLIENT'].get_console_data(instance_uuid, length=length))


@instance.command(name='snapshot', help='Snapshot instance')
@click.argument('instance_uuid', type=click.STRING, autocompletion=_get_instances)
@click.option('-a', '--all', is_flag=True,
              help='Snapshot all disks, not just disk 0.')
@click.option('-l', '--label_name', default=None,
              help='Label this snapshot with the specified name.')
@click.pass_context
def instance_snapshot(ctx, instance_uuid=None, all=False, label_name=None):
    snapshot = ctx.obj['CLIENT'].snapshot_instance(
        instance_uuid, all, label_name=label_name)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(snapshot, indent=4, sort_keys=True))
    else:
        print('Created snapshot %s' % snapshot)
