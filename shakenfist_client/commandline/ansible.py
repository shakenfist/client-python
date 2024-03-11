import click
import json
import sys
import time

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
            n = None

        if not n:
            # Namespace doesn't exist, so just make it
            _log('Namespace did not exist')
            n = client.create_namespace(name)
            return _result(True, False, n)

        # It already exists as we expect
        return _result(False, False, n)

    if state == 'absent':
        try:
            n = client.get_namespace(name)
        except apiclient.ResourceNotFoundException:
            _log('Namespace did not exist')
            return _result(
                False, False, None, error_msg='Namespace %s did not exist' % name)

        try:
            start_time = time.time()
            while time.time() - start_time < 5:
                _log('Attempt deletion...')
                client.delete_namespace(name)
                time.sleep(1)
                n = client.get_namespace(name)
                if not n:
                    break

            if n:
                return _result(
                    True, True, n,
                    error_msg='Deletion of namespace failed')
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

        try:
            n = client.get_network(
                identifier, namespace=input.get('namespace'))
        except apiclient.ResourceNotFoundException:
            n = None

        if not n:
            # Network doesn't exist, so just make it
            _log('Network did not exist')
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

        if dirty:
            try:
                start_time = time.time()
                while time.time() - start_time < 5:
                    _log('Attempt deletion...')
                    client.delete_network(
                        n['uuid'], namespace=input.get('namespace'))
                    time.sleep(1)
                    n = client.get_network(
                        n['uuid'], namespace=input.get('namespace'))
                    if not n or n['state'] == 'deleted':
                        _log('Deleted')
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
            n = client.allocate_network(
                netblock, nat, dhcp, name, namespace=input.get('namespace'))
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
            while time.time() - start_time < 5:
                _log('Attempt deletion...')
                client.delete_network(
                    identifier, namespace=input.get('namespace'))
                time.sleep(1)
                n = client.get_network(
                    identifier, namespace=input.get('namespace'))
                if not n or n['state'] == 'deleted':
                    _log('Deleted')
                    return _result(True, False, None)

            _log('Repeated attempts at deletion failed')
            return _result(
                True, True, n,
                error_msg='Deletion of network failed, does it have instances?')

        except apiclient.ResourceNotFoundException:
            _log('Deleted')
            return _result(True, False, None)

    return _result(False, True, None, error_msg='Unknown state "%s"' % state)


ansible.add_command(namespace)
ansible.add_command(network)
