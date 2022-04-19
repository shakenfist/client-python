import click
from collections import defaultdict
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

        for artifact in ctx.obj['CLIENT'].get_artifacts():
            for index in artifact['blobs']:
                discovered_blob_references[artifact['blobs']
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
