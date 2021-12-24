import click
import json
from prettytable import PrettyTable


GiB = 1024 * 1024 * 1024


@click.group(help='Blob commands')
def blob():
    pass


def _get_blobs(ctx, args, incomplete):
    choices = [b['uuid'] for b in ctx.obj['CLIENT'].get_blobs()]
    return [arg for arg in choices if arg.startswith(incomplete)]


@blob.command(name='list', help='List blobs.')
@click.pass_context
def blob_list(ctx, node=None):
    blobs = ctx.obj['CLIENT'].get_blobs(node)

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['uuid', 'state', 'size', 'virtual size', 'file format',
                         'mime type', 'modified', 'fetched at', 'locations',
                         'reference count', 'instances']
        for meta in blobs:
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
                meta.get('reference_count', 0),
                '\n'.join(meta.get('instances', []))
            ])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('uuid,state,size,virtual size,file format,mime type,modified'
              'fetched at,locations,reference count,instances')
        for meta in blobs:
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
                meta.get('reference_count', 0),
                ' '.join(meta.get('instances', []))
            ))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(blobs))
