import click
import json
from prettytable import PrettyTable
import sys

from shakenfist_client import util


def longest_str(d):
    if not d:
        return 0
    return max(len(k) for k in d)


@click.group(help='Namespace commands')
def namespace():
    pass


def _get_namespaces(ctx, args, incomplete):
    choices = util.get_client(ctx).get_namespaces()
    return [arg for arg in choices if arg.startswith(incomplete)]


@namespace.command(name='list', help='List namespaces')
@click.pass_context
def namespace_list(ctx):
    namespaces = list(ctx.obj['CLIENT'].get_namespaces())

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['name', 'state', 'trusted namespaces']
        for n in namespaces:
            x.add_row([n['name'], n['state'], ' '.join(n['trust']['full'])])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('name,state')
        for n in namespaces:
            print('%s,%s' % (n['name'], n['state']))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(namespaces, indent=4, sort_keys=True))


@namespace.command(name='create',
                   help=('Create a namespace.\n\n'
                         'NAMESPACE: The name of the namespace'))
@click.argument('namespace', type=click.STRING)
@click.pass_context
def namespace_create(ctx, namespace=None):
    ctx.obj['CLIENT'].create_namespace(namespace)


@namespace.command(name='delete',
                   help=('delete a namespace.\n\n'
                         'NAMESPACE: The name of the namespace'))
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.pass_context
def namespace_delete(ctx, namespace=None):
    ctx.obj['CLIENT'].delete_namespace(namespace)


@namespace.command(name='show', help='Show a namespace')
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.pass_context
def namespace_show(ctx, namespace=None):
    ns = ctx.obj['CLIENT'].get_namespace(namespace)
    if not ns:
        print('Namespace not found')
        sys.exit(1)

    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(ns, indent=4, sort_keys=True))

    elif ctx.obj['OUTPUT'] == 'pretty':
        format_string = '%-14s: %s'
        for key in ['name', 'state']:
            print(format_string % (key, ns[key]))
        print()

        if ns['keys']:
            format_string = '    %s'
            print('Key Names:')
            for key in ns['keys']:
                print(format_string % (key))
            print()

        if 'metadata' in ns and ns['metadata']:
            print('Metadata:')
            format_string = '    %-' + str(longest_str(ns['metadata'])) + 's: %s'
            for key in ns['metadata']:
                print(format_string % (key, ns['metadata'][key]))
            print()

        if 'trust' in ns and ns['trust']:
            print('Full trust:')
            format_string = '    %s'
            for key in ns['trust']['full']:
                print(format_string % key)

    else:
        format_string = '%s:%s'
        for key in ['name', 'state']:
            print(format_string % (key, ns[key]))
        print()

        print('keynames:')
        if ns['keys']:
            for key in ns['keys']:
                print('keyname,%s' % (key))
            print()

        if 'metadata' in ns and ns['metadata']:
            print('metadata,key,value')
            for key in ns['metadata']:
                print('metadata,%s,%s' % (key, ns['metadata'][key]))
            print()

        if 'trust' in ns and ns['trust']:
            for key in ns['trust']['full']:
                print('fulltrust,%s' % key)


@namespace.command(name='clean',
                   help=('Clean (delete) namespace of all instances and networks'))
@click.option('--confirm',  is_flag=True)
@click.option('--namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.pass_context
def namespace_clean(ctx, confirm=False, namespace=None):
    if not confirm:
        print('You must be sure. Use option --confirm.')
        return

    ctx.obj['CLIENT'].delete_all_instances(namespace)
    ctx.obj['CLIENT'].delete_all_networks(namespace, clean_wait=True)
    ctx.obj['CLIENT'].delete_all_artifacts(namespace)


@namespace.command(name='add-key',
                   help=('add a key to a namespace.\n\n'
                         'NAMESPACE: The name of the namespace\n'
                         'KEY_NAME:  The unique name of the key\n'
                         'KEY:       The password for the namespace'))
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('keyname', type=click.STRING)
@click.argument('key', type=click.STRING)
@click.pass_context
def namespace_add_key(ctx, namespace=None, keyname=None, key=None):
    ctx.obj['CLIENT'].add_namespace_key(namespace, keyname, key)


@namespace.command(name='delete-key',
                   help=('delete a specific key from a namespace.\n\n'
                         'NAMESPACE: The name of the namespace\n'
                         'KEYNAME:   The name of the key'))
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('keyname', type=click.STRING)
@click.pass_context
def namespace_delete_key(ctx, namespace=None, keyname=None):
    ctx.obj['CLIENT'].delete_namespace_key(namespace, keyname)


@namespace.command(name='get-metadata', help='Get metadata items')
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.pass_context
def namespace_get_metadata(ctx, namespace=None):
    metadata = ctx.obj['CLIENT'].get_namespace_metadata(namespace)

    if ctx.obj['OUTPUT'] == 'json':
        return metadata

    format_string = '%-12s: %s'
    if ctx.obj['OUTPUT'] == 'simple':
        format_string = '%s:%s'
    for key in metadata:
        print(format_string % (key, metadata[key]))


@namespace.command(name='set-metadata', help='Set a metadata item')
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('key', type=click.STRING)
@click.argument('value', type=click.STRING)
@click.pass_context
def namespace_set_metadata(ctx, namespace=None, key=None, value=None):
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, key, value)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@namespace.command(name='delete-metadata', help='Delete a metadata item')
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('key', type=click.STRING)
@click.pass_context
def namespace_delete_metadata(ctx, namespace=None, key=None):
    ctx.obj['CLIENT'].delete_namespace_metadata_item(namespace, key)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@namespace.command(name='add-trust',
                   help=('allow another namespace access to our resources.\n\n'
                         'NAMESPACE:          The name of the namespace\n'
                         'TRUSTED_NAMESPACE:  The name of the namespace to grant access to\n'))
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('trusted_namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.pass_context
def namespace_add_trust(ctx, namespace=None, trusted_namespace=None):
    out = ctx.obj['CLIENT'].add_namespace_trust(namespace, trusted_namespace)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(out, indent=4, sort_keys=True))


@namespace.command(name='remove-trust',
                   help=('remove another namespace\'s access to this namespace.\n\n'
                         'NAMESPACE:          The name of the namespace\n'
                         'TRUSTED_NAMESPACE:  The name of the namespace to remove access from\n'))
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('trusted_namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.pass_context
def namespace_remove_trust(ctx, namespace=None, trusted_namespace=None):
    out = ctx.obj['CLIENT'].remove_namespace_trust(
        namespace, trusted_namespace)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(out, indent=4, sort_keys=True))
