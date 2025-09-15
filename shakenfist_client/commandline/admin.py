import importlib.resources
import json
import os

import click
from prettytable import PrettyTable


@click.group(help='Admin commands')
def admin():
    pass


@click.group(help='Lock commands')
def lock():
    pass


@lock.command(name='list', help='List existing locks')
@click.pass_context
def lock_list(ctx):
    locks = ctx.obj['CLIENT'].get_existing_locks()

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['lock', 'pid', 'node', 'operation']
        for ref, meta in locks.items():
            x.add_row([ref, meta['pid'], meta['node'],
                       meta.get('operation')])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('lock,pid,node,operation')
        for ref, meta in locks.items():
            print(f"{ref},{meta['pid']},{meta['node']},"
                  f"{meta.get('operation')}")

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(locks, indent=4, sort_keys=True))


lock.add_command(lock_list)
admin.add_command(lock)


@admin.command(
    name='ansible_module_path',
    help='Returns the path ansible modules were installed to')
@click.pass_context
def ansible_module_path(ctx):
    with importlib.resources.path('shakenfist_client', 'sf_instance') as data_path:
        package_root = os.path.dirname(os.path.dirname(data_path))
        ansible_path = os.path.join(package_root, 'shakenfist_client', 'ansible')
        print(ansible_path)


admin.add_command(ansible_module_path)
