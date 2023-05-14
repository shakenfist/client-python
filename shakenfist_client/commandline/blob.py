import click
from collections import defaultdict
import json
from prettytable import PrettyTable
import sys


GiB = 1024 * 1024 * 1024


@click.group(help='Blob commands')
def blob():
    pass


def _get_blobs(ctx, args, incomplete):
    choices = [b['uuid'] for b in ctx.obj['CLIENT'].get_blobs()]
    return [arg for arg in choices if arg.startswith(incomplete)]


@blob.command(name='list', help='List blobs.')
@click.option('--audit/--no-audit', default=False)
@click.pass_context
def blob_list(ctx, node=None, audit=False):
    discovered_blob_references = defaultdict(int)
    if audit:
        for instance in ctx.obj['CLIENT'].get_instances():
            for d in instance['disk_spec']:
                blob_uuid = d.get('blob_uuid')
                if blob_uuid:
                    discovered_blob_references[blob_uuid] += 1

        for blob in ctx.obj['CLIENT'].get_blobs():
            for index in blob['blobs']:
                discovered_blob_references[blob['blobs']
                                           [index]['uuid']] += 1

        for b in ctx.obj['CLIENT'].get_blobs():
            if b['depends_on']:
                discovered_blob_references[b['depends_on']] += 1

            for t in b.get('transcodes'):
                discovered_blob_references[b['transcodes'][t]] += 1

    blobs = ctx.obj['CLIENT'].get_blobs(node)

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['uuid', 'state', 'size', 'virtual size', 'file format',
                         'mime type', 'modified', 'fetched at', 'locations',
                         'reference count', 'instances']
        for meta in blobs:
            if audit:
                difference = (meta.get('reference_count', 0) !=
                              discovered_blob_references[meta['uuid']])
                difference_highlight = ''
                if difference:
                    difference_highlight = ' !!!'
                reference_count = ('%d (audit %d) %s'
                                   % (meta.get('reference_count', 0),
                                      discovered_blob_references[meta['uuid']],
                                      difference_highlight))
            else:
                reference_count = meta.get('reference_count', 0)

            x.add_row([
                meta.get('uuid', ''),
                meta.get('state', ''),
                '%.02f' % (int(meta.get('size', 0)) / GiB),
                '%.02f' % (int(meta.get('virtual size', 0)) / GiB),
                meta.get('file format', ''),
                meta.get('mime type', ''),
                meta.get('modified', 0),
                meta.get('fetched_at', 0),
                ' '.join(sorted(meta.get('locations', []))),
                reference_count,
                '\n'.join(meta.get('instances', []))
            ])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('uuid,state,size,virtual size,file format,mime type,modified'
              'fetched at,locations,reference count,instances')
        for meta in blobs:
            if audit:
                difference = (meta.get('reference_count', 0) !=
                              discovered_blob_references[meta['uuid']])
                difference_highlight = ''
                if difference:
                    difference_highlight = ' !!!'
                reference_count = ('%d (audit %d) %s'
                                   % (meta.get('reference_count', 0),
                                      discovered_blob_references[meta['uuid']],
                                      difference_highlight))
            else:
                reference_count = meta.get('reference_count', 0)

            print('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
                meta.get('uuid', ''),
                meta.get('state', ''),
                '%.02f' % (int(meta.get('size', 0)) / GiB),
                '%.02f' % (int(meta.get('virtual size', 0)) / GiB),
                meta.get('file format', ''),
                meta.get('mime type', ''),
                meta.get('modified', 0),
                meta.get('fetched_at', 0),
                ' '.join(sorted(meta.get('locations', []))),
                reference_count,
                ' '.join(meta.get('instances', []))
            ))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(blobs, indent=4, sort_keys=True))


def _blob_show(ctx, b):
    if ctx.obj['OUTPUT'] == 'simple':
        format_string = '%s:%s'
    else:
        format_string = '%-16s: %s'

    if not ctx.obj['CLIENT'].check_capability('blob-metadata'):
        metadata = {}
    else:
        metadata = ctx.obj['CLIENT'].get_blob_metadata(b['uuid'])

    print(format_string % ('uuid', b['uuid']))
    print(format_string % ('state', b['state']))
    print(format_string % ('actual size', b['size']))
    print(format_string % ('virtual size', b.get('virtual size', '0')))
    print(format_string % ('sha512', b.get('sha512')))
    print(format_string % ('format', b.get('file format')))
    print(format_string % ('fetched at', b['fetched_at']))
    print(format_string % ('last used', b['last_used']))
    print(format_string % ('reference count', b['reference_count']))
    print(format_string % ('locations', ' '.join(b['locations'])))

    print()
    if ctx.obj['OUTPUT'] == 'pretty':
        format_string = '    %-8s: %s'
        print('Metadata:')
        for key in metadata:
            print(format_string % (key, metadata[key]))
    else:
        print('metadata,key,value')
        for key in metadata:
            print('metadata,%s,%s' % (key, metadata[key]))

    print()
    for t in b.get('transcodes'):
        print('Transcoded as %s at %s' % (t, b['transcodes'][t]))

    print()
    for i in b.get('instances'):
        print('Used by instance %s' % i)


@blob.command(name='show', help='Show details for a blob.')
@click.argument('uuid', type=click.STRING)
@click.pass_context
def blob_show(ctx, uuid=None):
    blob = ctx.obj['CLIENT'].get_blob(uuid)

    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(blob, indent=4, sort_keys=True))
        return

    if not blob:
        print('No blob found')
        return

    _blob_show(ctx, blob)


@blob.command(name='sha512', help='Find a blob with a matching checksum.')
@click.argument('hash', type=click.STRING)
@click.pass_context
def blob_sha512(ctx, hash=None):
    blob = ctx.obj['CLIENT'].get_blob_by_sha512(hash)

    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(blob, indent=4, sort_keys=True))
        return

    if not blob:
        print('No blob found')
        return

    _blob_show(ctx, blob)


@blob.command(name='set-metadata', help='Set a metadata item')
@click.argument('uuid', type=click.STRING, shell_complete=_get_blobs)
@click.argument('key', type=click.STRING)
@click.argument('value', type=click.STRING)
@click.pass_context
def instance_set_metadata(ctx, uuid=None, key=None, value=None):
    if not ctx.obj['CLIENT'].check_capability('blob-metadata'):
        sys.stderr.write(
            'Unfortunately this server does not implement blob metadata.\n')
        sys.exit(1)
    ctx.obj['CLIENT'].set_instance_metadata_item(uuid, key, value)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@blob.command(name='delete-metadata', help='Delete a metadata item')
@click.argument('uuid', type=click.STRING, shell_complete=_get_blobs)
@click.argument('key', type=click.STRING)
@click.pass_context
def instance_delete_metadata(ctx, uuid=None, key=None):
    if not ctx.obj['CLIENT'].check_capability('blob-metadata'):
        sys.stderr.write(
            'Unfortunately this server does not implement blob metadata.\n')
        sys.exit(1)
    ctx.obj['CLIENT'].delete_instance_metadata_item(uuid, key)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')
