import datetime
import json
import sys

import click
from prettytable import PrettyTable

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
            print('{},{}'.format(n['name'], n['state']))

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
                print('metadata,{},{}'.format(key, ns['metadata'][key]))
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


@namespace.command(name='update-key',
                   help=('update a key already present in the namespace.\n\n'
                         'NAMESPACE: The name of the namespace\n'
                         'KEY_NAME:  The unique name of the key\n'
                         'KEY:       The new password for the namespace'))
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('keyname', type=click.STRING)
@click.argument('key', type=click.STRING)
@click.pass_context
def namespace_update_key(ctx, namespace=None, keyname=None, key=None):
    ctx.obj['CLIENT'].update_namespace_key(namespace, keyname, key)


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
                   help="""
Allow another namespace access to our resources.

\b
NAMESPACE:          The name of the namespace.
TRUSTED_NAMESPACE:  The name of the namespace to grant access to.
""")
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('trusted_namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.pass_context
def namespace_add_trust(ctx, namespace=None, trusted_namespace=None):
    out = ctx.obj['CLIENT'].add_namespace_trust(namespace, trusted_namespace)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(out, indent=4, sort_keys=True))


@namespace.command(name='remove-trust',
                   help="""
Remove another namespace\'s access to this namespace.

\b
NAMESPACE:          The name of the namespace.
TRUSTED_NAMESPACE:  The name of the namespace to remove access from.
""")
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('trusted_namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.pass_context
def namespace_remove_trust(ctx, namespace=None, trusted_namespace=None):
    out = ctx.obj['CLIENT'].remove_namespace_trust(
        namespace, trusted_namespace)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(out, indent=4, sort_keys=True))


@click.group(help='Namespace capacity claim commands')
def claim():
    pass


def _claim_expiry(claim):
    """Render a claim's expiry, which is a unix timestamp on the wire."""
    expires_at = claim.get('expires_at')
    if not expires_at:
        return ''
    return str(datetime.datetime.fromtimestamp(expires_at))


def _claim_rows(claims):
    for c in claims:
        yield [c['uuid'], c['state'], c['coverage_state'],
               '%s / %s' % (c['used_cpus'], c['limit_cpus']),
               '%s / %s' % (c['used_memory_mb'], c['limit_memory_mb']),
               '%s / %s' % (c['used_disk_gb'], c['limit_disk_gb']),
               _claim_expiry(c)]


# A claim's two states are two different facts and are never merged into a
# single status column: state is the object's existence (created / deleted),
# where every other object publishes it, and coverage_state is whether the
# claim currently covers placements (active / expired). An expired claim
# reads as state created, coverage_state expired, and it still has a row an
# operator has to delete by hand.
CLAIM_COLUMNS = ['uuid', 'state', 'coverage', 'cpus', 'memory mb', 'disk gb',
                 'expires']


@claim.command(name='list',
               help=('List the capacity claims held by a namespace.\n\n'
                     'NAMESPACE: The name of the namespace'))
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.pass_context
def claim_list(ctx, namespace=None):
    claims = ctx.obj['CLIENT'].get_namespace_claims(namespace)

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = CLAIM_COLUMNS
        for row in _claim_rows(claims):
            x.add_row(row)
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print(','.join(CLAIM_COLUMNS))
        for row in _claim_rows(claims):
            print(','.join(str(f) for f in row))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(claims, indent=4, sort_keys=True))


@claim.command(name='show',
               help=('Show a capacity claim.\n\n'
                     'NAMESPACE:  The name of the namespace\n'
                     'CLAIM_UUID: The UUID of the claim'))
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('claim_uuid', type=click.STRING)
@click.pass_context
def claim_show(ctx, namespace=None, claim_uuid=None):
    c = ctx.obj['CLIENT'].get_namespace_claim(namespace, claim_uuid)

    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(c, indent=4, sort_keys=True))
        return

    format_string = '%-16s: %s'
    if ctx.obj['OUTPUT'] == 'simple':
        format_string = '%s:%s'

    for key in ['uuid', 'namespace', 'state', 'coverage_state',
                'limit_cpus', 'used_cpus', 'limit_memory_mb',
                'used_memory_mb', 'limit_disk_gb', 'used_disk_gb']:
        print(format_string % (key, c[key]))
    print(format_string % ('expires_at', _claim_expiry(c)))


@claim.command(name='create',
               help="""
Claim aggregate cluster capacity for a namespace.

A claim declares how much capacity a namespace expects to hold at once.
The cluster accounts placements in the namespace against it, and will not
promise capacity it does not have -- so creating a claim can be refused
when the cluster is full. Note that in this release exceeding a claim is
recorded as an audit event rather than refused.

\b
NAMESPACE: The name of the namespace
""")
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.option('--cpus', type=click.INT, required=True,
              help='The number of vCPUs this namespace may hold at once')
@click.option('--memory-mb', type=click.INT, required=True,
              help='Instance memory in megabytes')
@click.option('--disk-gb', type=click.INT, required=True,
              help='Instance disk in gigabytes')
@click.option('--expires-in', type=click.INT, required=True,
              help=('How long the claim lasts, in seconds. This is a '
                    'duration and not a time, because the expiry is '
                    'computed from the cluster\'s clock'))
@click.pass_context
def claim_create(ctx, namespace=None, cpus=None, memory_mb=None,
                 disk_gb=None, expires_in=None):
    c = ctx.obj['CLIENT'].create_namespace_claim(
        namespace, cpus, memory_mb, disk_gb, expires_in)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(c, indent=4, sort_keys=True))
    else:
        print(c['uuid'])


@claim.command(name='update',
               help="""
Grow, shrink or re-date a capacity claim.

Only the dimensions you name are changed, so re-dating a claim does not
disturb its limits. A claim cannot be shrunk below what it is already
using, and growing one can be refused if the cluster does not have the
capacity to promise.

\b
NAMESPACE:  The name of the namespace
CLAIM_UUID: The UUID of the claim
""")
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('claim_uuid', type=click.STRING)
@click.option('--cpus', type=click.INT, default=None,
              help='The new vCPU limit')
@click.option('--memory-mb', type=click.INT, default=None,
              help='The new memory limit, in megabytes')
@click.option('--disk-gb', type=click.INT, default=None,
              help='The new disk limit, in gigabytes')
@click.option('--expires-in', type=click.INT, default=None,
              help='A new lifetime, in seconds from now')
@click.pass_context
def claim_update(ctx, namespace=None, claim_uuid=None, cpus=None,
                 memory_mb=None, disk_gb=None, expires_in=None):
    # Deliberately passes through the options which were not supplied as
    # None, so the client sends a body naming only what changed. Filling
    # them in from a previous read would turn a re-date into a resize.
    c = ctx.obj['CLIENT'].update_namespace_claim(
        namespace, claim_uuid, limit_cpus=cpus, limit_memory_mb=memory_mb,
        limit_disk_gb=disk_gb, expires_in_seconds=expires_in)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(c, indent=4, sort_keys=True))


@claim.command(name='delete',
               help=('Delete a capacity claim, returning its capacity to '
                     'the cluster.\n\n'
                     'NAMESPACE:  The name of the namespace\n'
                     'CLAIM_UUID: The UUID of the claim'))
@click.argument('namespace', type=click.STRING, shell_complete=_get_namespaces)
@click.argument('claim_uuid', type=click.STRING)
@click.pass_context
def claim_delete(ctx, namespace=None, claim_uuid=None):
    c = ctx.obj['CLIENT'].delete_namespace_claim(namespace, claim_uuid)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(c, indent=4, sort_keys=True))


namespace.add_command(claim)
