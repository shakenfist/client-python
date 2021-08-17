import click
import json
from prettytable import PrettyTable


@click.group(help='Admin commands')
def admin():
    pass


@click.group(help='Lock commands')
def lock():
    pass


@admin.command(name='list', help='List existing locks')
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
            print('%s,%s,%s,%s' % (ref, meta['pid'], meta['node'],
                                   meta.get('operation')))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(locks))


lock.add_command(lock_list)
admin.add_command(lock)
