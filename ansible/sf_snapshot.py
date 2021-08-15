#!/usr/bin/python

# A simple Shaken Fist ansible module, with thanks to
# https://blog.toast38coza.me/custom-ansible-module-hello-world/

import json

from ansible.module_utils.basic import AnsibleModule


DOCUMENTATION = """
---
module: sf_snapshot
short_description: Create and delete Shaken Fist instance snapshots.
"""

EXAMPLES = """
- name: Snapshot a Shaken Fist instance
  sf_snapshot:
    instance_uuid: '9cd9ca86-0dd4-4ddd-aa28-822855ea4318'
    all: true
    state: present
  register: result
"""


def error(message):
    return True, False, {'error': message}


def present(module):
    if not module.params.get('instance_uuid'):
        return error('You must specify an instance_uuid when creating an instance')

    params = {
        'instance_uuid': module.params.get('instance_uuid')
    }

    extra = ''
    if module.params.get('all') and module.params['all']:
        extra += ' --all'
    params['extra'] = extra

    cmd = ('sf-client --json --async=block instance snapshot %(instance_uuid)s '
           '%(extra)s' % params)
    rc, stdout, stderr = module.run_command(
        cmd, check_rc=False, use_unsafe_shell=True)
    if rc != 0:
        return True, False, 'Command failed: %s' % stderr

    j = json.loads(stdout)
    if rc != 0:
        return True, False, j
    return False, True, j


def main():

    fields = {
        'uuid': {'required': False, 'type': 'str'},
        'instance_uuid': {'required': False, 'type': 'str'},
        'all': {'required': False, 'type': 'bool'},
        'state': {
            'default': 'present',
            'choices': ['present', 'absent'],
            'type': 'str'
        },
    }

    choice_map = {
        'present': present,
    }

    module = AnsibleModule(argument_spec=fields)
    is_error, has_changed, result = choice_map.get(
        module.params['state'])(module)

    if not is_error:
        module.exit_json(changed=has_changed, meta=result)
    else:
        module.fail_json(msg='Error manipulating instance',
                         params=module.params,
                         meta=result)


if __name__ == '__main__':
    main()
