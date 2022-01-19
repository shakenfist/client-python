#!/usr/bin/python

# A simple Shaken Fist ansible module, with thanks to
# https://blog.toast38coza.me/custom-ansible-module-hello-world/

import datetime
import json
import time

from ansible.module_utils.basic import AnsibleModule


DOCUMENTATION = """
---
module: sf_instance
short_description: Create and delete Shaken Fist instances.
"""

EXAMPLES = """
- name: Create Shaken Fist instance
  sf_instance:
    name: 'myinstance'
    cpu: 1
    ram: 1024
    disks:
      - 8@cirros
    networks:
      - 1ebc5020-6cfd-4641-8f3b-175596a19de0
    ssh_key: ssh-rsa AAAAB3NzaC1yc2EAA...f5uaZaTqQa18t8s= mikal@marvin
    user_data: |
      IyEvYmluL3NoCgplY2hvICJIZWxsbyBXb3JsZC4gIFRoZSB0aW1lIGlzIG5vdyAkKGRhdGUgLVIp
      ISIgPiAvaG9tZS9jaXJyb3Mvb3V0cHV0LnR4dApjaG93biBjaXJyb3MuY2lycm9zIC9ob21lL2Np
      cnJvcy9vdXRwdXQudHh0
    state: present
  register: result

- name: Create Shaken Fist instance with fancy disks
  sf_instance:
    name: 'myinstance'
    cpu: 1
    ram: 1024
    diskspecs:
      - size=8,base=cirros,bus=ide,type=disk
      - size=16,type=disk
      - base={{'http://archive.ubuntu.com/ubuntu/dists/focal/main/installer-amd64/' +
               'current/legacy-images/netboot/mini.iso,type=cdrom'}}
    networks:
      - 1ebc5020-6cfd-4641-8f3b-175596a19de0
    ssh_key: ssh-rsa AAAAB3NzaC1yc2EAA...f5uaZaTqQa18t8s= mikal@marvin
    user_data: |
      IyEvYmluL3NoCgplY2hvICJIZWxsbyBXb3JsZC4gIFRoZSB0aW1lIGlzIG5vdyAkKGRhdGUgLVIp
      ISIgPiAvaG9tZS9jaXJyb3Mvb3V0cHV0LnR4dApjaG93biBjaXJyb3MuY2lycm9zIC9ob21lL2Np
      cnJvcy9vdXRwdXQudHh0
    state: present
  register: result

- name: Create Shaken Fist instance in namespace someuser asynchronously
  sf_instance:
    name: 'myinstance'
    cpu: 1
    ram: 1024
    disks:
      - 8@cirros
    namespace: someuser
    async: true

- name: Create Shaken Fist with affinity and other metadata
    sf_instance:
    name: 'test-metadata-affinity'
    cpu: 1
    ram: 1024
    disks:
      - 8@cirros
    networks:
      - testnet
    metadata:
      hello: world
      affinity:
        cpu:
          controller: -10
      tags:
        - controller
        - ci-test-123abc
      arbitrary:
        - key1: test1
          key2: test2
        - dict1:
          - dict1entry1
          - dict1entry2
          - dict1entry3
    state: present

- name: Delete an instance
  sf_instance:
    uuid: 'afb68328-6ff0-498f-bdaa-27d3fcc97f31'
    state: absent
  register: result
"""


def error(message):
    return True, False, {'error': message}


def log(message):
    with open('/tmp/sf_ansible.log', 'a') as logfile:
        logfile.write('%s: sf-instance %s\n'
                      % (datetime.datetime.now(), message))
        logfile.flush()


def present(module):
    log('present starting')
    params = {}

    # Required parameters
    for key in ['name', 'cpu', 'ram']:
        params[key] = module.params.get(key)
        if not params[key]:
            log.write('%s sf-instance required param %s missing\n'
                      % (datetime.datetime.now(), key))
            return error('You must specify %s when creating an instance' % key)

    if not module.params.get('disks'):
        params['disks'] = ''
    else:
        params['disks'] = '-d %s' % (' -d '.join(module.params['disks']))

    if not module.params.get('diskspecs'):
        params['diskspecs'] = ''
    else:
        params['diskspecs'] = '-D %s' % (
            ' -D '.join(module.params['diskspecs']))

    if not module.params.get('networks'):
        params['networks'] = ''
    else:
        params['networks'] = '-n %s' % (' -n '.join(module.params['networks']))

    if not module.params.get('networkspecs'):
        params['networkspecs'] = ''
    else:
        params['networkspecs'] = '-N %s' % (
            ' -N '.join(module.params['networkspecs']))

    if not module.params.get('placement'):
        params['placement'] = ''
    else:
        params['placement'] = '-p  %s' % module.params['placement']

    extra = ''
    for flag, key in [('-I', 'ssh_key'), ('-U', 'user_data'), ('-V', 'video')]:
        if module.params.get(key):
            extra += ' %s "%s"' % (flag, module.params[key])

    if module.params.get('uefi', False):
        extra += ' --uefi'
    if module.params.get('secure_boot', False):
        extra += ' --secure-boot'
    if module.params.get('nvram_template', ''):
        extra += ' --nvram-template %s' % module.params['nvram_template']

    if module.params.get('configdrive'):
        extra += ' --configdrive %s' % module.params['configdrive']

    if module.params.get('metadata'):
        for k, v in module.params.get('metadata').items():
            extra += " --metadata %s='%s'" % (k, json.dumps(v))

    params['extra'] = extra

    params['async_strategy'] = 'block'
    if module.params.get('async'):
        params['async_strategy'] = 'continue'

    log('params: %s' % params)
    cmd = ('sf-client --json --async=%(async_strategy)s instance create '
           '%(name)s %(cpu)s %(ram)s %(disks)s %(diskspecs)s '
           '%(networks)s %(networkspecs)s %(placement)s '
           '%(extra)s' % params)
    if 'namespace' in module.params and module.params['namespace']:
        cmd += ' --namespace ' + module.params['namespace']
    log('command: %s' % cmd)

    rc, stdout, stderr = module.run_command(
        cmd, check_rc=False, use_unsafe_shell=True)
    log('exit code: %d' % rc)
    if rc != 0:
        return True, False, 'Command failed: %s' % stderr

    finalized = False
    while not finalized:
        try:
            j = json.loads(stdout)
            log('%s has state %s' % (j['uuid'], j['state']))

            if params['async_strategy'] == 'continue':
                finalized = True
            if j['state'] == 'created':
                finalized = True
            elif j['state'].endswith('error'):
                return error('Instance %s failed to create' % j['uuid'])

            if not finalized:
                log('polling for finalization of uuid %s' % j['uuid'])
                rc, stdout, stderr = module.run_command(
                    'sf-client --json instance show %s' % j['uuid'],
                    check_rc=False, use_unsafe_shell=True)
                time.sleep(5)

        except ValueError as e:
            log('json parse failure: %s' % e)
            rc = -1
            j = ('Failed to parse JSON:\n'
                 '[[command: %s]]\n'
                 '[[stdout: %s]]\n'
                 '[[stderr: %s]]'
                 % (cmd, stdout, stderr))
            log('json parse failure: %s' % j)
            break
    log('finalized')

    if rc != 0:
        return True, False, j

    return False, True, j


def absent(module):
    if not module.params.get('uuid'):
        return error('You must specify a uuid when deleting an instance')

    cmd = ('sf-client --json --async=block instance delete %(uuid)s'
           % module.params)
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
        'name': {'required': False, 'type': 'str'},
        'cpu': {'required': False, 'type': 'int'},
        'ram': {'required': False, 'type': 'int'},

        'disks': {'required': False, 'type': 'list', 'elements': 'str'},
        'diskspecs': {'required': False, 'type': 'list', 'elements': 'str'},
        'namespace': {'required': False, 'type': 'str'},
        'networks': {'required': False, 'type': 'list', 'elements': 'str'},
        'networkspecs': {'required': False, 'type': 'list', 'elements': 'str'},
        'ssh_key': {'required': False, 'type': 'str'},
        'user_data': {'required': False, 'type': 'str'},
        'placement': {'required': False, 'type': 'str'},
        'video': {'required': False, 'type': 'str'},
        'configdrive': {'required': False, 'type': 'str'},

        'uefi': {'required': False, 'type': 'bool'},
        'secure_boot': {'required': False, 'type': 'bool'},
        'nvram_template': {'required': False, 'type': 'str'},

        'metadata': {'required': False, 'type': 'dict'},

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
        module.fail_json(msg='Error manipulating instance',
                         params=module.params,
                         meta=result)


if __name__ == '__main__':
    main()
