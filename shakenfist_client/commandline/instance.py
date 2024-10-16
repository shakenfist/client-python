import base64
import datetime
import json
import os
import subprocess
import sys
import tempfile

import click
from prettytable import PrettyTable

from shakenfist_client import apiclient
from shakenfist_client import util


@click.group(help='Instance commands')
def instance():
    pass


def _get_instances(ctx, args, incomplete):
    instances = util.get_client(ctx).get_instances()
    choices = [i[key] for key in ['uuid', 'name'] for i in instances]
    return [arg for arg in choices if arg.startswith(incomplete)]


def _get_interfaces(ctx, instance):
    if instance.get('interfaces'):
        return instance['interfaces']
    try:
        return util.get_client(ctx).get_instance_interfaces(instance['uuid'])
    except apiclient.ResourceNotFoundException:
        return []


def _convert_metadata(key, value):
    RESERVED_TAGS = ['tags', 'affinity']
    if key in RESERVED_TAGS:
        try:
            value = json.loads(value)
        except json.decoder.JSONDecodeError as e:
            print('Reserved metadata keys ({}) must contain valid JSON: {}'.format(
                  ', '.join(RESERVED_TAGS), e))
            raise e
    return value


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
        x.align['interfaces'] = 'l'

        for i in insts:
            ifaces = []
            for interface in _get_interfaces(ctx, i):
                iface = (f"{interface['order']}: "
                         f"{interface.get('ipv4', 'No address assigned')}")
                if interface.get('floating'):
                    iface += ' ({})'.format(interface['floating'])
                ifaces.append(iface)

            x.add_row([i['uuid'], i['name'], i['namespace'],
                       i['cpus'], i['memory'], i['node'],
                       i.get('power_state', 'unknown'), i['state'],
                       '\n'.join(ifaces)])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('uuid,name,namespace,cpus,memory,hypervisor,power state,state,'
              'interfaces')
        for i in insts:
            ifaces = []
            for interface in _get_interfaces(ctx, i):
                iface = f"{interface['order']}:{interface.get('ipv4', 'None')}"
                if interface.get('floating'):
                    iface += '({})'.format(interface['floating'])
                ifaces.append(iface)

            print('%s,%s,%s,%s,%s,%s,%s,%s,%s'
                  % (i['uuid'], i['name'], i['namespace'],
                     i['cpus'], i['memory'], i['node'],
                     i.get('power_state', 'unknown'), i['state'],
                     ';'.join(ifaces)))

    elif ctx.obj['OUTPUT'] == 'json':
        export_insts = []
        for i in insts:
            i['interfaces'] = _get_interfaces(ctx, i)
            export_insts.append(i)
        print(json.dumps({'instances': export_insts},
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


def _show_instance(ctx, i, include_snapshots=False, include_agentoperations=False):
    if not i:
        print('Instance not found')
        sys.exit(1)

    metadata = ctx.obj['CLIENT'].get_instance_metadata(i['uuid'])
    interfaces = ctx.obj['CLIENT'].get_instance_interfaces(i['uuid'])
    if include_snapshots:
        snapshots = ctx.obj['CLIENT'].get_instance_snapshots(i['uuid'])
    if include_agentoperations:
        agentops = ctx.obj['CLIENT'].get_instance_agentoperations(i['uuid'], all=True)

    if ctx.obj['OUTPUT'] == 'json':
        out = util.filter_dict(i, ['uuid', 'name', 'namespace', 'cpus', 'memory',
                                   'disk_spec', 'video', 'node', 'console_port',
                                   'vdi_port', 'vdi_tls_port', 'ssh_key', 'user_data',
                                   'power_state', 'state', 'uefi', 'secure_boot',
                                   'nvram_template', 'side_channels', 'agent_state'])
        out['network_interfaces'] = []
        for interface in interfaces:
            util.show_interface(ctx, interface, out)

        out['metadata'] = metadata

        if include_snapshots:
            out['snapshots'] = []
            for snap in snapshots:
                out['snapshots'].append(util.filter_dict(
                    snap, ['uuid', 'device', 'created']))

        if include_agentoperations:
            out['agent_operations'] = agentops

        print(json.dumps(out, indent=4, sort_keys=True))
        return

    if ctx.obj['OUTPUT'] == 'simple':
        format_string = '%s:%s'
    else:
        format_string = '%-14s: %s'
        d_space = {'type': 5, 'bus': 4, 'size': 3, 'base': 0}
        v_space = {'model': 0, 'memory': 0, 'vdi': 0}

    print(format_string % ('uuid', i['uuid']))
    print(format_string % ('name', i['name']))
    print(format_string % ('namespace', i['namespace']))
    print(format_string % ('cpus', i['cpus']))
    print(format_string % ('memory', i['memory']))
    print(format_string % ('uefi', i.get('uefi', False)))
    print(format_string % ('secure boot', i.get('secure_boot', False)))
    print(format_string % ('nvram template', i.get('nvram_template')))

    if ctx.obj['OUTPUT'] == 'pretty':
        print(format_string % ('disk spec', _pretty_dict(16, i['disk_spec'], d_space)))
    if ctx.obj['OUTPUT'] == 'pretty':
        print(format_string % ('video', _pretty_dict(16, (i['video'],), v_space)))
    print(format_string % ('node', i.get('node', '')))
    print(format_string % ('power state', i.get('power_state', '')))
    print(format_string % ('state', i.get('state', '')))
    print(format_string % ('agent state', i.get('agent_state', '')))
    print(format_string % ('error message', i.get('error_message', '')))

    # NOTE(mikal): I am not sure we should expose this, but it will do
    # for now until a proxy is written.
    print(format_string % ('console port', i.get('console_port', '')))

    print(format_string % ('vdi port', i.get('vdi_port', '')))
    if i.get('vdi_tls_port'):
        print(format_string % ('vdi tls port', i.get('vdi_tls_port', '')))

    print(format_string % ('side channels', ' '.join(i.get('side_channels', []))))

    print()
    print(format_string % ('ssh key', i['ssh_key']))
    print(format_string % ('user data', i['user_data']))

    if ctx.obj['OUTPUT'] == 'simple':
        print()
        print('disk_spec,type,bus,size,base')
        for d in i['disk_spec']:
            print('disk_spec,{},{},{},{}'.format(
                d['type'], d['bus'], d['size'], d['base']))

    if ctx.obj['OUTPUT'] == 'simple':
        print()
        print('video,model,memory')
        print('video,{},{}'.format(i['video']['model'], i['video']['memory']))

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

    if include_agentoperations:
        print()
        if ctx.obj['OUTPUT'] == 'pretty':
            print('Agent Operations:')
            print()

            x = PrettyTable()
            x.field_names = ['uuid', 'state', 'commands']
            for agentop in agentops:
                cmds = []
                for cmd in agentop.get('commands', []):
                    cmds.append(cmd['command'])
                x.add_row([agentop['uuid'], agentop['state'], '; '.join(cmds)])
            print(x)

        else:
            print('agentop,uuid,state,commands')
            for agentop in agentops:
                cmds = []
                for cmd in agentop.get('commands', []):
                    cmds.append(cmd['command'])
                print('agentop, %s,%s,%s'
                      % (agentop['uuid'], agentop['state'], ';'.join(cmds)))


@instance.command(name='show', help='Show an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.option('-s', '--snapshots', type=click.BOOL, is_flag=True, default=False)
@click.option('-a', '--agentoperations', type=click.BOOL, is_flag=True, default=False)
@click.pass_context
def instance_show(ctx, instance_ref=None, snapshots=False, agentoperations=False):
    _show_instance(ctx, ctx.obj['CLIENT'].get_instance(
        instance_ref), include_snapshots=snapshots, include_agentoperations=agentoperations)


def _parse_spec(spec):
    if '@' not in spec:
        return spec, None
    return spec.split('@')


def _parse_detailed_netspec(spec):
    defn = {}
    for elem in spec.split(','):
        s = elem.split('=')
        if len(s) != 2:
            print('Error in network specification - should be key=value: %s' % elem)
            return

        value = s[1]
        if s[0] == 'float':
            value = s[1] in ['true', 'True']

        defn[s[0]] = value

    return defn


# TODO(mikal): this misses the detailed version of disk and network specs, as well
# as guidance on how to use the video command line. We need to rethink how we're
# doing this, as its getting pretty long.
@instance.command(name='create',
                  help=("""
Create an instance.

\b
NAME:   The name of the instance.
CPUS:   The number of vCPUs for the instance.
MEMORY: The amount of RAM for the instance in MB.

You may specify more than one disk or network for an instance, with the
order you specify them being significant.
"""))
@click.argument('name', type=click.STRING)
@click.argument('cpus', type=click.INT)
@click.argument('memory', type=click.INT)
@click.option('-n', '--network', type=click.STRING, multiple=True,
              shell_complete=util.get_networks,
              help='A short form definition of a network to attach.')
@click.option('-f', '--floated', type=click.STRING, multiple=True,
              shell_complete=util.get_networks,
              help='As for --networkspec, but with implied float of the interface.')
@click.option('-N', '--networkspec', type=click.STRING, multiple=True,
              help='A long form "networkspec" definition of a network to attach.')
@click.option('-d', '--disk', type=click.STRING, multiple=True,
              help='A short from definition of a disk to attach.')
@click.option('-D', '--diskspec', type=click.STRING, multiple=True,
              help='A long form "diskspec" definition of a disk to attach.')
@click.option('-i', '--sshkey', type=click.Path(exists=True),
              help='The path to a ssh public key to configure via config drive.')
@click.option('-I', '--sshkeydata', type=click.STRING,
              help='A ssh public key as a string to configure via config drive.')
@click.option('-u', '--userdata', type=click.Path(exists=True),
              help=('The path to a file containing user data provided '
                    'to the instance via config drive.'))
@click.option('-U', '--encodeduserdata', type=click.STRING,
              help=('Base64 encoded user data to provide to the instance via '
                    'config drive.'))
@click.option('-p', '--placement', type=click.STRING,
              help='Force placement of instance on specified node.')
@click.option('-V', '--videospec', type=click.STRING,
              help='A detailed "videospec" definition of the video configuration.')
@click.option('--configdrive', type=click.Choice(['openstack-disk', 'none'],
                                                 case_sensitive=False),
              default='openstack-disk', help='Select config drive format.')
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this object in a '
                    'different namespace.'))
@click.option('--bios/--uefi', is_flag=True, default=True,
              help='Pass --uefi to use UEFI boot instead of BIOS boot.')
@click.option('--no-secure-boot/--secure-boot', is_flag=True, default=True,
              help=('Pass --secure-boot to enable UEFI secure boot. This implies '
                    'UEFI boot.'))
@click.option('--nvram-template', type=click.STRING,
              help=('A label or blob UUID to use as a template for UEFI NVRAM. '
                    'This is sometimes required for secure boot.'))
@click.option('--force', is_flag=True, default=False,
              help='Allow very small memory instances.')
@click.option('-m', '--metadata', type=click.STRING, multiple=True,
              help='Add key=value pair to instance metadata eg. affinity=\'{"fastdisk":10}\'')
@click.option('-s', '--side-channel', type=click.STRING, multiple=True,
              help=('A named side channel which will appear inside the guest as a '
                    'virtio-serial port.'))
@click.pass_context
def instance_create(ctx, name=None, cpus=None, memory=None, network=None, floated=None,
                    networkspec=None, disk=None, diskspec=None, sshkey=None, sshkeydata=None,
                    userdata=None, encodeduserdata=None, placement=None, videospec=None,
                    namespace=None, bios=True, force=False, configdrive=None,
                    no_secure_boot=True, nvram_template=None, metadata=None, side_channel=None):
    if memory < 128 and not force:
        print('Specified memory size is %dMB. This is very small.' % memory)
        print('Use the --force flag if this is deliberate')
        return

    if len(disk) < 1 and len(diskspec) < 1:
        print('You must specify at least one disk')
        return

    # Because of the way click works, the logic for the BIOS vs UEFI boot flag and
    # the secure boot flag are a bit weird. Fix it up to what the API expects...
    uefi = not bios
    secure_boot = not no_secure_boot

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
        netdefs.append(_parse_detailed_netspec(n))

    video = {'model': 'cirrus', 'memory': 16384}
    if videospec:
        for elem in videospec.split(','):
            s = elem.split('=')
            if len(s) != 2:
                print('Error in video specification - '
                      ' should be key=value: %s' % elem)
                return
            video[s[0]] = s[1]

    metadata_def = {}
    for m in metadata:
        try:
            key, val = m.split('=')
        except ValueError:
            print('Unable to parse metadata, correct format is "key=value"')
            return
        try:
            val = _convert_metadata(key, val)
        except json.decoder.JSONDecodeError:
            return
        metadata_def[key] = val

    kwargs = {
        'force_placement': placement,
        'namespace': namespace,
        'video': video,
        'uefi': uefi,
        'configdrive': configdrive,
        'secure_boot': secure_boot,
        'nvram_template': nvram_template,
        'metadata': metadata_def,
        'side_channels': side_channel
    }

    _show_instance(ctx, ctx.obj['CLIENT'].create_instance(
            name, cpus, memory, netdefs, diskdefs, sshkey_content,
            userdata_content, **kwargs))


@instance.command(name='delete', help='Delete an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.option('--namespace', type=click.STRING)
@click.pass_context
def instance_delete(ctx, instance_ref=None, namespace=None):
    out = ctx.obj['CLIENT'].delete_instance(instance_ref, namespace=namespace)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(out, indent=4, sort_keys=True))


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
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.option('-t', '--type', help='The event type to return')
@click.option('-l', '--limit', help='The maximum number of events to return')
@click.pass_context
def instance_events(ctx, instance_ref=None, type=None, limit=None):
    events = ctx.obj['CLIENT'].get_instance_events(instance_ref, event_type=type, limit=limit)
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


@instance.command(name='set-metadata', help='Set a metadata item')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.argument('key', type=click.STRING)
@click.argument('value', type=click.STRING)
@click.pass_context
def instance_set_metadata(ctx, instance_ref=None, key=None, value=None):
    try:
        value = _convert_metadata(key, value)
    except json.decoder.JSONDecodeError:
        return
    ctx.obj['CLIENT'].set_instance_metadata_item(instance_ref, key, value)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='delete-metadata', help='Delete a metadata item')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.argument('key', type=click.STRING)
@click.pass_context
def instance_delete_metadata(ctx, instance_ref=None, key=None):
    ctx.obj['CLIENT'].delete_instance_metadata_item(instance_ref, key)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='reboot', help='Reboot instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.option('--hard/--soft', default=False)
@click.pass_context
def instance_reboot(ctx, instance_ref=None, hard=False):
    ctx.obj['CLIENT'].reboot_instance(instance_ref, hard=hard)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='poweron', help='Power on an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.pass_context
def instance_power_on(ctx, instance_ref=None):
    ctx.obj['CLIENT'].power_on_instance(instance_ref)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='poweroff', help='Power off an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.pass_context
def instance_power_off(ctx, instance_ref=None):
    ctx.obj['CLIENT'].power_off_instance(instance_ref)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='pause', help='Pause an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.pass_context
def instance_pause(ctx, instance_ref=None):
    ctx.obj['CLIENT'].pause_instance(instance_ref)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='unpause', help='Unpause an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.pass_context
def instance_unpause(ctx, instance_ref=None):
    ctx.obj['CLIENT'].unpause_instance(instance_ref)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@instance.command(name='consoledata', help='Get console data for an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.argument('length', type=click.INT, default=10240)
@click.pass_context
def instance_consoledata(ctx, instance_ref=None, length=None):
    print(ctx.obj['CLIENT'].get_console_data(instance_ref, length=length))


@instance.command(name='consoledelete', help='Clear the console log for this instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.pass_context
def instance_consoledelete(ctx, instance_ref=None):
    ctx.obj['CLIENT'].delete_console_data(instance_ref)


@instance.command(name='vdiconsole', help='Launch a VDI console for the instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.pass_context
def instance_vdiconsole(ctx, instance_ref=None):
    if not ctx.obj['CLIENT'].check_capability('vdi-console-helper'):
        sys.stderr.write(
            'Unfortunately this server does not implement VDI console helpers.\n')
        sys.exit(1)

    debug = ''
    if ctx.obj['VERBOSE']:
        debug = '--debug'

    # We don't use NamedTemporaryFile as a context manager as the .vv file
    # will also attempt to clean up the file.
    (temp_handle, temp_name) = tempfile.mkstemp()
    os.close(temp_handle)
    try:
        with open(temp_name, 'w') as f:
            f.write(ctx.obj['CLIENT'].get_vdi_console_helper(instance_ref))

        p = subprocess.run(f'remote-viewer {debug} {temp_name}', shell=True)
        if ctx.obj['VERBOSE']:
            print('Remote viewer process exited with %d return code'
                  % p.returncode)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


@instance.command(name='vdiconsolefile', help='Download a .vv file for the VDI console')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.pass_context
def instance_vdiconsolefile(ctx, instance_ref=None):
    if not ctx.obj['CLIENT'].check_capability('vdi-console-helper'):
        sys.stderr.write(
            'Unfortunately this server does not implement VDI console helpers.\n')
        sys.exit(1)

    print(ctx.obj['CLIENT'].get_vdi_console_helper(instance_ref))


@instance.command(name='snapshot', help='Snapshot instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.option('-a', '--all', is_flag=True,
              help='Snapshot all disks, not just disk 0.')
@click.option('-d', '--device', default=None,
              help='Snapshot this specific device, instead of disk 0.')
@click.option('-l', '--label_name', default=None,
              help='Label this snapshot with the specified name.')
@click.option('--delete-snapshot-after-label/--retain-snapshot-after-label',
              is_flag=True, default=False)
@click.option('--thin/--flatten', is_flag=True, default=False)
@click.pass_context
def instance_snapshot(ctx, instance_ref=None, all=False, device=None, label_name=None,
                      delete_snapshot_after_label=None, thin=None):
    snapshot = ctx.obj['CLIENT'].snapshot_instance(
        instance_ref, all, device=device, label_name=label_name,
        delete_snapshot_after_label=delete_snapshot_after_label,
        thin=thin)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(snapshot, indent=4, sort_keys=True))
    else:
        print('Created snapshot %s' % snapshot)


@instance.command(name='upload', help='Upload a file to an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.argument('source', type=click.Path(exists=True))
@click.argument('destination', type=click.Path())
@click.pass_context
def instance_upload(ctx, instance_ref=None, source=None, destination=None):
    if not ctx.obj['CLIENT'].check_capability('blob-search-by-hash'):
        blob = None
    else:
        # We can cheat here -- if we already have a blob in the cluster with the
        # checksum of the file we're uploading, we can skip the upload entirely and
        # just reuse that blob.
        blob = util.checksum_with_progress(ctx.obj['CLIENT'], source)

    if not blob:
        artifact = util.upload_artifact_with_progress(
            ctx.obj['CLIENT'], 'upload-to-%s' % instance_ref, source, None)
    else:
        print('Recycling existing blob')
        artifact = ctx.obj['CLIENT'].blob_artifact(
            'upload-to-%s' % instance_ref, blob['uuid'], source_url=None)
    print('Created artifact %s' % artifact['uuid'])

    st = os.stat(source)
    ctx.obj['CLIENT'].instance_put_blob(
            instance_ref, artifact['blob_uuid'], destination, st.st_mode)


@instance.command(name='execute', help='Execute a command on an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.argument('commandline', type=click.STRING)
@click.pass_context
def instance_execute(ctx, instance_ref=None, commandline=None):
    op = ctx.obj['CLIENT'].instance_execute(instance_ref, commandline)

    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(op, indent=4, sort_keys=True))
        return

    if '0' not in op.get('results', {}):
        print('Results not available.')
        sys.exit(1)

    if ctx.obj['OUTPUT'] == 'simple':
        format_string = '%s:%s'
        joiner_template = '\n%(file)s:'
    else:
        format_string = '%-14s: %s'
        joiner_template = '\n                '

    print(format_string % ('exit code', op['results']['0']['return-code']))

    joiner = joiner_template % {'file': 'stdout'}
    print(format_string %
          ('stdout', joiner.join(op['results']['0'].get('stdout', '').split('\n'))))

    joiner = joiner_template % {'file': 'stderr'}
    print(format_string %
          ('stderr', joiner.join(op['results']['0'].get('stderr', '').split('\n'))))


@instance.command(name='download', help='Download a file from an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.argument('source', type=click.Path())
@click.argument('destination', type=click.Path())
@click.pass_context
def instance_download(ctx, instance_ref=None, source=None, destination=None):
    op = ctx.obj['CLIENT'].instance_get(instance_ref, source)
    if '0' not in op.get('results', {}):
        print('Results not available.')
        sys.exit(1)

    blob_uuid = op['results']['0'].get('content_blob')
    if not blob_uuid:
        print('Results did not include content')
        sys.exit(1)

    with open(destination, 'wb') as f:
        for chunk in ctx.obj['CLIENT'].get_blob_data(blob_uuid):
            f.write(chunk)


@instance.command(name='screenshot',
                  help='Download a screenshot of the console of an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.argument('destination', type=click.Path())
@click.pass_context
def instance_screenshot(ctx, instance_ref=None, destination=None):
    with open(destination, 'wb') as f:
        for chunk in ctx.obj['CLIENT'].get_screenshot(instance_ref):
            f.write(chunk)


@instance.command(name='await',
                  help='Await an agent ready from the specified instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.pass_context
def instance_await(ctx, instance_ref=None):
    try:
        ctx.obj['CLIENT'].instance_await_agent_ready(instance_ref)

    except apiclient.InstanceWillNeverBeReady as e:
        print('Instance will never be ready: %s' % e)
        sys.exit(1)


@instance.command(name='add-interface', help='Add a network interface to an instance')
@click.argument('instance_ref', type=click.STRING, shell_complete=_get_instances)
@click.option('-n', '--network', type=click.STRING,
              shell_complete=util.get_networks,
              help='A short form definition of a network to attach.')
@click.option('-f', '--floated', type=click.STRING, shell_complete=util.get_networks,
              help='As for --networkspec, but with implied float of the interface.')
@click.option('-N', '--networkspec', type=click.STRING,
              help='A long form "networkspec" definition of a network to attach.')
@click.pass_context
def instance_add_interface(ctx, instance_ref=None, floated=None, network=None,
                           networkspec=None):
    requested = 0
    if network:
        requested += 1
    if floated:
        requested += 1
    if networkspec:
        requested += 1

    if requested == 0:
        print('Please specify an interface.')
        sys.exit(1)
    if requested > 1:
        print('Please specify only one interface.')
        sys.exit(1)

    netdesc = None
    if network:
        network_uuid, address = _parse_spec(network)
        netdesc = {
            'network_uuid': network_uuid,
            'macaddress': None,
            'model': 'virtio',
            'float': False
        }
        if address:
            netdesc['address'] = address

    elif floated:
        network_uuid, address = _parse_spec(floated)
        netdesc = {
            'network_uuid': network_uuid,
            'macaddress': None,
            'model': 'virtio',
            'float': True
        }
        if address:
            netdesc['address'] = address

    elif networkspec:
        netdesc = _parse_detailed_netspec(networkspec)

    ctx.obj['CLIENT'].add_instance_interface(instance_ref, netdesc)
