import click
import json
from prettytable import PrettyTable
import time


@click.group(help='Node commands')
def node():
    pass


@node.command(name='list', help='List nodes')
@click.pass_context
def node_list(ctx):
    nodes = list(ctx.obj['CLIENT'].get_nodes())

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['name', 'ip', 'lastseen', 'version']
        for n in nodes:
            last_seen = '%s (%d seconds ago)' % (time.ctime(n['lastseen']),
                                                 time.time() - n['lastseen'])
            x.add_row([n['name'], n['ip'], last_seen, n['version']])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('name,ip,lastseen,version')
        for n in nodes:
            print('%s,%s,%s,%s' % (
                n['name'], n['ip'], n['lastseen'], n['version']))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(nodes, indent=4, sort_keys=True))
