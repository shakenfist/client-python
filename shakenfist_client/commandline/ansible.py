import json
import sys
import time

import click

from shakenfist_client import apiclient


@click.group(help='Ansible commands, intended to be used as modules')
def ansible():
    pass


# For each of these commands we receive a single argument, which is the path
# to a JSON formatted file (by virtue of the indirection shim having WANT_JSON
# in it). We return a JSON string with our results.


def _make_client(client):
    # We need a quiet and patient blocking client
    return apiclient.Client(
        base_url=client.base_url, verbose=False, namespace=client.namespace,
        key=client.key, sync_request_timeout=1800,
        suppress_configuration_lookup=True,
        async_strategy=apiclient.ASYNC_BLOCK)


LOG = []


def _result(changed, error, meta, error_msg=None):
    global LOG
    sys.stdout.write(json.dumps(
        {
            'changed': changed,
            'failed': error,
            'meta': meta,
            'log': LOG,
            'msg': error_msg
        }, indent=4, sort_keys=True
    ))


def _log(msg):
    global LOG
    LOG.append(msg)
    sys.stderr.write(msg)
    sys.stderr.write('\n')


@ansible.command(name='namespace', help='Namespace module')
@click.argument('args', type=click.Path(exists=True))
@click.pass_context
def namespace(ctx, args):
    global LOG
    LOG = []
    with open(args) as f:
        input = f.read()
        _log('Input was: %s' % input)
        input = json.loads(input)

    state = input.get('state', 'present')
    client = _make_client(ctx.obj['CLIENT'])

    if 'name' not in input:
        return _result(
            False, True, None,
            error_msg={'error': 'You must specify a name'})
    name = input.get('name')

    if state == 'present':
        try:
            n = client.get_namespace(name)
        except apiclient.ResourceNotFoundException:
            n = {}

        if not n:
            # Namespace doesn't exist, so just make it
            _log('Namespace did not exist')
            n = client.create_namespace(name)
            return _result(True, False, n)

        # It already exists as we expect
        return _result(False, False, n)

    if state == 'absent':
        n = {}
        try:
            n = client.get_namespace(name)
            if n['state'] == 'deleted':
                _log('Namespace is already deleted')
                return (False, False, None)

        except apiclient.ResourceNotFoundException:
            _log('Namespace did not exist')
            return _result(
                False, False, None, error_msg='Namespace %s did not exist' % name)

        try:
            start_time = time.time()
            while time.time() - start_time < 180:
                try:
                    _log('Attempt deletion (state is %s)...' % n.get('state'))
                    client.delete_namespace(name)
                    time.sleep(1)
                    n = client.get_namespace(name)
                    if not n:
                        break
                    if n['state'] == 'deleted':
                        break
                except apiclient.ResourceNotFoundException:
                    n = {}

            if n and n['state'] != 'deleted':
                return _result(
                    True, True, n, error_msg='Deletion of namespace failed')

            return _result(True, False, n)
        except apiclient.ResourceNotFoundException:
            return _result(True, False, None)

    return _result(False, True, None, error_msg='Unknown state "%s"' % state)


@ansible.command(name='network', help='Network module')
@click.argument('args', type=click.Path(exists=True))
@click.pass_context
def network(ctx, args):
    global LOG
    LOG = []
    with open(args) as f:
        input = f.read()
        _log('Input was: %s' % input)
        input = json.loads(input)

    state = input.get('state', 'present')
    client = _make_client(ctx.obj['CLIENT'])

    identifier = None
    name = input.get('name')
    uuid = input.get('uuid')
    if uuid:
        identifier = uuid
    else:
        identifier = name

    _log('Will use identifier %s' % identifier)
    if not identifier:
        return _result(
            False, True, None,
            error_msg='You must specify one of name or uuid')

    if state == 'present':
        netblock = input.get('netblock')
        nat = input.get('nat', True)
        dhcp = input.get('dhcp', True)
        dns = input.get('dns', False)

        try:
            n = client.get_network(
                identifier, namespace=input.get('namespace'))
        except apiclient.ResourceNotFoundException:
            n = {}

        if not n:
            # Network doesn't exist, so just make it
            _log('Network did not exist')
            try:
                n = client.allocate_network(netblock, nat, dhcp, name,
                                            namespace=input.get('namespace'),
                                            provide_dns=dns)
                return _result(True, False, n)
            except apiclient.IncapableException:
                if dns:
                    return _result(
                        False, True, None,
                        error_msg={'error': 'This cloud does not support DNS services'})

                n = client.allocate_network(netblock, nat, dhcp, name,
                                            namespace=input.get('namespace'))
                return _result(True, False, n)

        # Check if the network has a changed specification
        dirty = False
        for key in ['name', 'netblock']:
            if key not in input:
                return _result(
                    False, True, None,
                    error_msg={'error': 'You must specify %s when creating a network' % key})

            if n[key] != input.get(key):
                _log('Network dirty, %s changed' % key)
                dirty = True

        # Optional specification elements with implied defaults
        if 'dhcp' in input and n['provide_dhcp'] != dhcp:
            _log('Network dirty, dhcp changed')
            dirty = True
        if 'nat' in input and n['provide_nat'] != nat:
            _log('Network dirty, nat changed')
            dirty = True
        if 'dns' in input and n['provide_dns'] != dns:
            _log('Network dirty, DNS changed')
            dirty = True

        if dirty:
            try:
                start_time = time.time()
                while time.time() - start_time < 180:
                    try:
                        _log('Attempt deletion...')
                        client.delete_network(
                            n['uuid'], namespace=input.get('namespace'))
                        time.sleep(1)
                        n = client.get_network(
                            n['uuid'], namespace=input.get('namespace'))
                        if not n or n['state'] == 'deleted':
                            _log('Deleted')
                            break
                    except apiclient.ResourceNotFoundException:
                        n = {}
                        break

                if n and n['state'] != 'deleted':
                    _log('Repeated attempts at deletion failed')
                    return _result(
                        True, True, None,
                        error_msg={'error': ('Deletion of network for update failed, '
                                             'does it have instances?')})

            except apiclient.ResourceNotFoundException:
                _log('Deleted')

            _log('Creating network')
            try:
                n = client.allocate_network(netblock, nat, dhcp, name,
                                            namespace=input.get('namespace'),
                                            provide_dns=dns)
                return _result(True, False, n)
            except apiclient.IncapableException:
                if dns:
                    return _result(
                        False, True, None,
                        error_msg={'error': 'This cloud does not support DNS services'})

                n = client.allocate_network(netblock, nat, dhcp, name,
                                            namespace=input.get('namespace'))
                return _result(True, False, n)

        # It already exists as we expect
        _log('Call was noop')
        return _result(False, False, n)

    if state == 'absent':
        try:
            n = client.get_network(
                identifier, namespace=input.get('namespace'))
        except apiclient.ResourceNotFoundException:
            _log('Network not found')
            return _result(
                False, False, None, error_msg='Network %s did not exist' % identifier)

        try:
            start_time = time.time()
            while time.time() - start_time < 180:
                try:
                    _log('Attempt deletion...')
                    client.delete_network(
                        identifier, namespace=input.get('namespace'))
                    time.sleep(1)
                    n = client.get_network(
                        identifier, namespace=input.get('namespace'))
                    if not n or n['state'] == 'deleted':
                        _log('Deleted')
                        return _result(True, False, None)
                except apiclient.ResourceNotFoundException:
                    return _result(True, False, None)

            _log('Repeated attempts at deletion failed')
            return _result(
                True, True, n,
                error_msg='Deletion of network failed, does it have instances?')

        except apiclient.ResourceNotFoundException:
            _log('Deleted')
            return _result(True, False, None)

    return _result(False, True, None, error_msg='Unknown state "%s"' % state)


class InstanceCreationException(Exception):
    ...


def _check_instance(client, existing, input):
    dirty = False
    instance_args = []
    instance_kwargs = {}

    # Required parameters are set and the right type. Note the names are different
    # in each dict for historical reasons.
    for input_required, existing_required in [('name', 'name'), ('cpu', 'cpus'),
                                              ('ram', 'memory')]:
        if input_required not in input:
            raise InstanceCreationException(
                'You must specify %s' % input_required)

        if input_required == 'name':
            if existing.get('name') != input['name']:
                _log('Instance dirty: name has changed from %s to %s'
                     % (existing.get('name'), input['name']))
                dirty = True
            instance_args.append(input['name'])
            continue

        try:
            instance_args.append(int(input[input_required]))
            if existing.get(existing_required) != input[input_required]:
                _log('Instance dirty: %s has changed from %s to %s'
                     % (input_required, existing.get(existing_required),
                        input[input_required]))
                dirty = True

        except ValueError:
            raise InstanceCreationException(
                '%s must be an integer' % input_required)

    # Networks. Networks in the REST API are represented by interfaces, so we
    # need to do some additional API lookups here. We represent everything as a
    # networkspec for the purposes of creation.
    requested_networks = []
    for n in input.get('networks', []):
        requested_networks.append({
            'network_uuid': n,
            'model': 'virtio',
            'float': False
        })

    for n in input.get('networkspecs', []):
        defn = {}
        for elem in n.split(','):
            s = elem.split('=')
            if len(s) != 2:
                raise InstanceCreationException(
                    'network specification should be key=value not %s' % elem)
            if s[0] == 'float':
                try:
                    s[1] = bool(s[1])
                except ValueError:
                    raise InstanceCreationException('float must be a boolean')
            defn[s[0]] = s[1]
        requested_networks.append(defn)

    # Painful dirtiness comparison...
    existing_interfaces = []
    for interface in existing.get('interfaces', []):
        iface = client.get_interface(interface['uuid'])
        existing_interfaces.append({
            'network_uuid': iface['network_uuid'],
            'macaddress': iface['macaddr'],
            'address': iface['ipv4'],
            'model': iface['model'],
            'float': iface['floating']
        })
    if len(existing_interfaces) != len(requested_networks):
        _log('Instance dirty: the number of interfaces changed')
        dirty = True
    else:
        for idx in range(len(existing_interfaces)):
            existing_iface = existing_interfaces[idx]
            requested_iface = requested_networks[idx]

            if existing_iface['network_uuid'] != requested_iface['network_uuid']:
                _log('Instance dirty: interface %d changed network' % idx)
                dirty = True
                break

            for attr in ['macaddress', 'address', 'model', 'float']:
                if (attr in requested_networks and
                        existing_iface[attr] != requested_iface[attr]):
                    _log('Instance dirty: interface %d changed %s' % (idx, attr))
                    dirty = True
                    break

    instance_args.append(requested_networks)

    # Disks. Convert everything to a disk spec because that's what's returned
    # by the REST API.
    requested_disks = []
    for d in input.get('disks', []):
        size = None
        base = None
        if '@' not in d:
            size = int(d)
        else:
            size, base = d.split('@')
            try:
                size = int(size)
            except ValueError:
                raise InstanceCreationException('disk size must be an integer')

        requested_disks.append({
            'base': base,
            'bus': None,
            'size': size,
            'type': 'disk'
        })

    for d in input.get('diskspecs', []):
        defn = {
            'base': None,
            'bus': None,
            'size': None,
            'type': 'disk'
        }
        for elem in d.split(','):
            s = elem.split('=')
            if len(s) != 2:
                raise InstanceCreationException(
                    'disk specification should be key=value not %s' % elem)

            if s[0] == 'size':
                try:
                    s[1] = int(s[1])
                except ValueError:
                    raise InstanceCreationException(
                        'disk size must be an integer')

            defn[s[0]] = s[1]
        requested_disks.append(defn)

    # Cleanup existing disk specifications. disk_base is an internal representation
    # of a cleaned up disk's base.
    cleaned_existing_disks = []
    for e in existing.get('disk_spec', []):
        if 'disk_base' in e:
            del e['disk_base']
        cleaned_existing_disks.append(e)

    json_requested = json.dumps(requested_disks, sort_keys=True)
    json_existing = json.dumps(cleaned_existing_disks, sort_keys=True)
    if json_requested != json_existing:
        _log('Instance dirty: disk specification has changed')
        _log('    Requested: %s' % json_requested)
        _log('    Existing: %s' % json_existing)
        dirty = True

    instance_args.append(requested_disks)

    # Does the instance definition specify single string values?
    for key in ['ssh_key', 'user_data']:
        if key in input:
            if existing.get(key) != input[key]:
                _log('Instance dirty: %s has changed' % key)
                dirty = True
            instance_args.append(input[key])
        else:
            instance_args.append(None)

    # Does the instance definition specify optional single string values?
    for key in ['placement', 'video', 'nvram_template', 'configdrive', 'namespace']:
        if key in input:
            if existing.get(key) != input[key]:
                _log('Instance dirty: %s has changed' % key)
                dirty = True

            if key == 'placement':
                key == 'force_placement'
            instance_kwargs[key] = input[key]

    # What about optional values which might be a list of strings?
    for key in ['side_channels']:
        values = input.get(key, [])
        if existing.get(key) != values:
            _log('Instance dirty: %s has changed' % key)
            dirty = True

        instance_kwargs[key] = values

    # Does the instance definition specify optional single boolean values?
    for key in ['uefi', 'secureboot']:
        if key in input:
            try:
                input[key] = bool(input[key])
            except ValueError:
                raise InstanceCreationException('%s must be a boolean' % key)

            if existing.get(key) != input[key]:
                _log('Instance dirty: %s has changed' % key)
                dirty = True

            if key == 'placement':
                key == 'force_placement'
            instance_kwargs[key] = input[key]

    # Metadata is a dict
    metadata = {}
    for k, v in input.get('metadata', {}).items():
        metadata[k] = v
    if metadata:
        instance_kwargs['metadata'] = metadata

    if dirty:
        return True, instance_args, instance_kwargs

    return False, None, None


@ansible.command(name='instance', help='Instance module')
@click.argument('args', type=click.Path(exists=True))
@click.pass_context
def instance(ctx, args):
    global LOG
    LOG = []
    with open(args) as f:
        input = f.read()
        _log('Input was: %s' % input)
        input = json.loads(input)

    state = input.get('state', 'present')
    client = _make_client(ctx.obj['CLIENT'])

    identifier = None
    name = input.get('name')
    uuid = input.get('uuid')
    if uuid:
        identifier = uuid
    else:
        identifier = name

    _log('Will use identifier %s' % identifier)
    if not identifier:
        return _result(
            False, True, None,
            error_msg='You must specify one of name or uuid')

    if state == 'present':
        try:
            i = client.get_instance(
                identifier, namespace=input.get('namespace'))
        except apiclient.ResourceNotFoundException:
            i = {}

        try:
            needs_replacement, instance_args, instance_kwargs = \
                _check_instance(client, i, input)
        except InstanceCreationException as e:
            return _result(False, True, None, error_msg=str(e))

        if needs_replacement:
            if i:
                start_time = time.time()
                while time.time() - start_time < 180:
                    try:
                        _log('Attempt deletion...')
                        client.delete_instance(
                            identifier, namespace=input.get('namespace'))
                        time.sleep(1)
                        i = client.get_instance(
                            identifier, namespace=input.get('namespace'))
                        if not i or i['state'] == 'deleted':
                            break
                    except apiclient.ResourceNotFoundException:
                        i = {}
                        break

            if i and i['state'] != 'deleted':
                _log('Repeated attempts at deletion failed')
                return _result(
                    True, True, None,
                    error_msg={'error': ('Deletion of instance for update failed.')})

            i = client.create_instance(*instance_args, **instance_kwargs)

        if not input.get('await', False):
            _log('Not awaiting instance')
        else:
            _log('Awaiting instance %s' % i['uuid'])
            try:
                client.await_instance_create(
                    i['uuid'], timeout=input.get('await_timeout', 600))
            except Exception as e:
                _log('Waiting for instance failed: %s' % e)
                return _result(
                    needs_replacement, True, None,
                    error_msg={'error': 'Waiting for instance failed: %s' % e})

        return _result(needs_replacement, False, client.get_instance(i['uuid']))

    if state == 'absent':
        try:
            n = client.get_instance(
                identifier, namespace=input.get('namespace'))
        except apiclient.ResourceNotFoundException:
            _log('Instance not found')
            return _result(
                False, False, None, error_msg='Instance %s did not exist' % identifier)

        try:
            start_time = time.time()
            while time.time() - start_time < 180:
                try:
                    _log('Attempt deletion...')
                    client.delete_instance(
                        identifier, namespace=input.get('namespace'))
                    time.sleep(1)
                    n = client.get_instance(
                        identifier, namespace=input.get('namespace'))
                    if not n or n['state'] == 'deleted':
                        _log('Deleted')
                        return _result(True, False, None)
                except apiclient.ResourceNotFoundException:
                    return _result(True, False, None)

            _log('Repeated attempts at deletion failed')
            return _result(
                True, True, n, error_msg='Deletion of instance failed')

        except apiclient.ResourceNotFoundException:
            _log('Deleted')
            return _result(True, False, None)

    return _result(False, True, None, error_msg='Unknown state "%s"' % state)


ansible.add_command(namespace)
ansible.add_command(network)
ansible.add_command(instance)
