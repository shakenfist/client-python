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
- name: Snapshot just the primary disk a Shaken Fist instance
  sf_snapshot:
    instance_uuid: '9cd9ca86-0dd4-4ddd-aa28-822855ea4318'
    state: present
  register: result

- name: Snapshot all disks on a Shaken Fist instance, without blocking
  sf_snapshot:
    instance_uuid: '9cd9ca86-0dd4-4ddd-aa28-822855ea4318'
    all: true
    async: true
    state: present
  register: result

- name: Snapshot and update the "ciimage" label
  sf_snapshot:
    instance_uuid: '9cd9ca86-0dd4-4ddd-aa28-822855ea4318'
    label: ciimage
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
    if module.params.get('all', False):
        extra += ' --all'
    if module.params.get('label'):
        extra += ' --label_name %s' % module.params['label']
    if module.params.get('delete_after_label', False):
        extra += ' --delete-snapshot-after-label'
    params['extra'] = extra

    params['async_strategy'] = 'block'
    if module.params.get('async'):
        params['async_strategy'] = 'continue'

    cmd = ('sf-client --json --async=%(async_strategy)s '
           'instance snapshot %(instance_uuid)s %(extra)s' % params)
    rc, stdout, stderr = module.run_command(
        cmd, check_rc=False, use_unsafe_shell=True)
    if rc != 0:
        return True, False, 'Command failed: %s' % stderr

    j = json.loads(stdout)
    if rc != 0:
        return True, False, j
    return False, True, j


def absent(module):
    if not module.params.get('uuid'):
        return error('You must specify a uuid when deleting a snapshot')

    cmd = ('sf-client --json --async=block artifact delete %(uuid)s'
           % module.params)

    rc, stdout, stderr = module.run_command(
        cmd, check_rc=False, use_unsafe_shell=True)
    if rc != 0:
        return True, False, 'Command failed: %s' % stderr

    return False, True, None


def main():

    fields = {
        'uuid': {'required': False, 'type': 'str'},
        'instance_uuid': {'required': False, 'type': 'str'},
        'all': {'required': False, 'type': 'bool'},
        'label': {'required': False, 'type': 'str'},
        'delete_after_label': {'required': False, 'type': 'bool'},

        'async': {'required': False, 'type': 'bool'},

        'state': {
            'default': 'present',
            'choices': ['present', 'absent'],
            'type': 'str'
        },
    }

    choice_map = {
        'present': present,
        'absent': absent
    }

    module = AnsibleModule(argument_spec=fields)
    is_error, has_changed, result = choice_map.get(
        module.params['state'])(module)

    if not is_error:
        module.exit_json(changed=has_changed, meta=result)
    else:
        module.fail_json(msg='Error manipulating artifact',
                         params=module.params,
                         meta=result)


if __name__ == '__main__':
    main()
