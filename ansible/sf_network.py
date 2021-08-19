#!/usr/bin/python

# A simple Shaken Fist ansible module, with thanks to
# https://blog.toast38coza.me/custom-ansible-module-hello-world/

import json

from ansible.module_utils.basic import AnsibleModule


DOCUMENTATION = """
---
module: sf_network
short_description: Create and delete Shaken Fist networks.
"""

EXAMPLES = """
- name: Create Shaken Fist network
  sf_network:
    netblock: '192.168.242.0/24'
    name: 'mynet'
  register: result

- name: Delete that network
  sf_network:
    uuid: '1ebc5020-6cfd-4641-8f3b-175596a19de0'
    state: absent
  register: result

- name: Create Shaken Fist network in different namespace
  sf_network:
    netblock: '192.168.242.0/24'
    name: 'mynet'
    namespace: 'mynamespace'
"""


def error(message):
    return True, False, {'error': message}


def present(module):
    for required in ['name', 'netblock']:
        if not module.params.get(required):
            return error('You must specify a %s when creating an instance' % required)

    params = {}
    for key in ['name', 'netblock']:
        params[key] = module.params.get(key)

    params['async_strategy'] = 'block'
    if module.params.get('async') and module.params['async']:
        params['async_strategy'] = 'continue'

    cmd = ('sf-client --json --async=%(async_strategy)s '
           'network create %(name)s %(netblock)s'
           % params)
    if 'namespace' in module.params and module.params['namespace']:
        cmd += ' --namespace ' + module.params['namespace']

    rc, stdout, stderr = module.run_command(
        cmd, check_rc=False, use_unsafe_shell=True)
    if rc != 0:
        return True, False, 'Command failed: %s' % stderr

    try:
        j = json.loads(stdout)
    except ValueError:
        rc = -1
        j = ('Failed to parse JSON:\n'
             '[[command: %s]]\n'
             '[[stdout: %s]]\n'
             '[[stderr: %s]]'
             % (cmd, stdout, stderr))

    if rc != 0:
        return True, False, j

    return False, True, j


def absent(module):
    if not module.params.get('uuid'):
        return error('You must specify a uuid when deleting a network')

    cmd = ('sf-client --json --async=block network delete %(uuid)s' %
           module.params)
    if 'namespace' in module.params and module.params['namespace']:
        cmd += ' --namespace ' + module.params['namespace']

    rc, stdout, stderr = module.run_command(
        cmd, check_rc=False, use_unsafe_shell=True)
    if rc != 0:
        return True, False, 'Command failed: %s' % stderr

    try:
        j = json.loads(stdout)
    except ValueError:
        rc = -1
        j = ('Failed to parse JSON:\n'
             '[[command: %s]]\n'
             '[[stdout: %s]]\n'
             '[[stderr: %s]]'
             % (cmd, stdout, stderr))

    if rc != 0:
        return True, False, j

    return False, True, j


def main():

    fields = {
        'uuid': {'required': False, 'type': 'str'},
        'netblock': {'required': False, 'type': 'str'},
        'name': {'required': False, 'type': 'str'},
        'namespace': {'type': 'str'},

        'async': {'required': False, 'type': 'bool'},

        'state': {
            'default': 'present',
            'choices': ['present', 'absent'],
            'type': 'str'
        },
    }

    choice_map = {
        'present': present,
        'absent': absent,
    }

    module = AnsibleModule(argument_spec=fields)
    is_error, has_changed, result = choice_map.get(
        module.params['state'])(module)

    if not is_error:
        module.exit_json(changed=has_changed, meta=result)
    else:
        module.fail_json(msg='Error manipulating network', meta=result)


if __name__ == '__main__':
    main()
