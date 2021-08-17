import click
import json
from prettytable import PrettyTable
import sys


def longest_str(d):
    if not d:
        return 0
    return max(len(k) for k in d)


@click.group(help='Namespace commands')
def namespace():
    pass


def _get_namespaces(ctx, args, incomplete):
    choices = ctx.obj['CLIENT'].get_namespaces()
    return [arg for arg in choices if arg.startswith(incomplete)]


@namespace.command(name='list', help='List namespaces')
@click.pass_context
def namespace_list(ctx):
    namespaces = list(ctx.obj['CLIENT'].get_namespaces())

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['namespace']
        for n in namespaces:
            x.add_row([n])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('namespace')
        for n in namespaces:
            print(n)

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(namespaces))


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
@click.argument('namespace', type=click.STRING)
@click.pass_context
def namespace_delete(ctx, namespace=None):
    ctx.obj['CLIENT'].delete_namespace(namespace)


def _show_namespace(ctx, namespace):
    if namespace not in ctx.obj['CLIENT'].get_namespaces():
        print('Namespace not found')
        sys.exit(1)

    key_names = ctx.obj['CLIENT'].get_namespace_keynames(namespace)
    metadata = ctx.obj['CLIENT'].get_namespace_metadata(namespace)

    if ctx.obj['OUTPUT'] == 'json':
        out = {'key_names': key_names,
               'metadata': metadata,
               }
        print(json.dumps(out, indent=4, sort_keys=True))
        return

    if ctx.obj['OUTPUT'] == 'pretty':
        format_string = '    %s'
        print('Key Names:')
        if key_names:
            for key in key_names:
                print(format_string % (key))

        print('Metadata:')
        if metadata:
            format_string = '    %-' + str(longest_str(metadata)) + 's: %s'
            for key in metadata:
                print(format_string % (key, metadata[key]))

    else:
        print('metadata,keyname')
        if key_names:
            for key in key_names:
                print('keyname,%s' % (key))
        print('metadata,key,value')
        if metadata:
            for key in metadata:
                print('metadata,%s,%s' % (key, metadata[key]))


@namespace.command(name='show', help='Show a namespace')
@click.argument('namespace', type=click.STRING, autocompletion=_get_namespaces)
@click.pass_context
def namespace_show(ctx, namespace=None):
    _show_namespace(ctx, namespace)


@namespace.command(name='clean',
                   help=('Clean (delete) namespace of all instances and networks'))
@click.option('--confirm',  is_flag=True)
@click.option('--namespace', type=click.STRING)
@click.pass_context
def namespace_clean(ctx, confirm=False, namespace=None):
    if not confirm:
        print('You must be sure. Use option --confirm.')
        return

    ctx.obj['CLIENT'].delete_all_instances(namespace)
    ctx.obj['CLIENT'].delete_all_networks(namespace)


@namespace.command(name='add-key',
                   help=('add a key to a namespace.\n\n'
                         'NAMESPACE: The name of the namespace\n'
                         'KEY_NAME:  The unique name of the key\n'
                         'KEY:       The password for the namespace'))
@click.argument('namespace', type=click.STRING)
@click.argument('keyname', type=click.STRING)
@click.argument('key', type=click.STRING)
@click.pass_context
def namespace_add_key(ctx, namespace=None, keyname=None, key=None):
    ctx.obj['CLIENT'].add_namespace_key(namespace, keyname, key)


@namespace.command(name='delete-key',
                   help=('delete a specific key from a namespace.\n\n'
                         'NAMESPACE: The name of the namespace\n'
                         'KEYNAME:   The name of the key'))
@click.argument('namespace', type=click.STRING)
@click.argument('keyname', type=click.STRING)
@click.pass_context
def namespace_delete_key(ctx, namespace=None, keyname=None):
    ctx.obj['CLIENT'].delete_namespace_key(namespace, keyname)


@namespace.command(name='get-metadata', help='Get metadata items')
@click.argument('namespace', type=click.STRING)
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
@click.argument('namespace', type=click.STRING)
@click.argument('key', type=click.STRING)
@click.argument('value', type=click.STRING)
@click.pass_context
def namespace_set_metadata(ctx, namespace=None, key=None, value=None):
    ctx.obj['CLIENT'].set_namespace_metadata_item(namespace, key, value)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@namespace.command(name='delete-metadata', help='Delete a metadata item')
@click.argument('namespace', type=click.STRING)
@click.argument('key', type=click.STRING)
@click.pass_context
def namespace_delete_metadata(ctx, namespace=None, key=None):
    ctx.obj['CLIENT'].delete_namespace_metadata_item(namespace, key)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')
