import datetime
import http
import json
import sys

import click
import requests
import urllib3
from prettytable import PrettyTable
from tqdm import tqdm

from shakenfist_client import util


@click.group(help='Artifact commands')
def artifact():
    pass


def _get_artifacts(ctx, args, incomplete):
    choices = [a['uuid'] for a in util.get_client(ctx).get_artifacts()]
    return [arg for arg in choices if arg.startswith(incomplete)]


@artifact.command(name='cache',
                  help=('Cache an image.\n\n'
                        'IMAGE_URL: The URL of the image to cache'))
@click.argument('image_url', type=click.STRING)
@click.option('--not-shared/--shared', is_flag=True, default=True,
              help=('If you are an admin, you can pass --shared to share an '
                    'artifact with others.'))
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this object in a '
                    'different namespace.'))
@click.pass_context
def artifact_cache(ctx, image_url=None, not_shared=True, namespace=None):
    s = not not_shared
    ctx.obj['CLIENT'].cache_artifact(image_url, shared=s, namespace=namespace)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@artifact.command(name='upload', help='Upload an artifact.')
@click.argument('name', type=click.STRING)
@click.argument('source', type=click.Path(exists=True))
@click.option('--source_url', default=None,
              help='A URL to act as if this artifact was downloaded from.')
@click.option('--not-shared/--shared', is_flag=True, default=True,
              help=('If you are an admin, you can pass --shared to share an '
                    'artifact with others.'))
@click.option('--checksum/--no-checksum', is_flag=True, default=True,
              help='Generate a checksum and reuse an existing blob if one exists.')
@click.option('--namespace', type=click.STRING,
              help=('If you are an admin, you can create this object in a '
                    'different namespace.'))
@click.pass_context
def artifact_upload(ctx, name=None, source=None, source_url=None, not_shared=True,
                    namespace=None, checksum=True):
    if not checksum:
        blob = None
    elif not ctx.obj['CLIENT'].check_capability('blob-search-by-hash'):
        blob = None
    else:
        # We can cheat here -- if we already have a blob in the cluster with the
        # checksum of the file we're uploading, we can skip the upload entirely and
        # just reuse that blob.
        blob = util.checksum_with_progress(ctx.obj['CLIENT'], source)

    if not blob:
        print('Uploading new blob')
        artifact = util.upload_artifact_with_progress(
            ctx.obj['CLIENT'], name, source, source_url,
            namespace=namespace, shared=(not not_shared))
    else:
        print('Recycling existing blob')
        s = not not_shared
        artifact = ctx.obj['CLIENT'].blob_artifact(
            name, blob['uuid'], source_url=source_url, shared=s, namespace=namespace)
    print('Created artifact %s' % artifact['uuid'])


@artifact.command(name='download', help='Download an artifact.')
@click.argument('artifact_ref', type=click.STRING, shell_complete=_get_artifacts)
@click.argument('destination', type=click.Path(exists=False))
@click.pass_context
def artifact_download(ctx, artifact_ref=None, destination=None):
    a = ctx.obj['CLIENT'].get_artifact(artifact_ref)

    if not a:
        print('Artifact not found')
        sys.exit(1)

    blob_uuid = a.get('blob_uuid')
    blob_index = a.get('index')
    if not blob_uuid:
        print('Artifact has no versions')

    size = a['blobs'][blob_index]['size']
    print('%s -> %s of %d bytes' % (artifact_ref, blob_uuid, size))

    total = 0
    connection_failures = 0
    done = False

    with tqdm(total=size, unit='B', unit_scale=True,
              desc=f'Downloading {artifact_ref} to {destination}') as pbar:
        with open(destination, 'wb') as f:
            while not done:
                bytes_in_attempt = 0

                try:
                    for chunk in ctx.obj['CLIENT'].get_blob_data(blob_uuid, offset=total):
                        received = len(chunk)
                        f.write(chunk)
                        pbar.update(received)
                        bytes_in_attempt += received
                        total += received

                    done = True

                except urllib3.exceptions.NewConnectionError as e:
                    connection_failures += 1
                    if connection_failures > 2:
                        print('HTTP connection repeatedly failed: %s' % e)
                        sys.exit(1)

                except (ConnectionResetError, http.client.IncompleteRead,
                        urllib3.exceptions.ProtocolError,
                        requests.exceptions.ChunkedEncodingError) as e:
                    # An API error (or timeout) occurred. Retry unless we got nothing.
                    if bytes_in_attempt == 0:
                        print('HTTP connection dropped without '
                              'transferring data: %s' % e)
                        sys.exit(1)

    if total != size:
        print('Remote side has %d, we have received %d!' % (size, total))
        sys.exit(1)

    print('Download complete')


@artifact.command(name='list', help='List artifacts.')
@click.pass_context
def artifact_list(ctx, node=None):
    artifacts = ctx.obj['CLIENT'].get_artifacts(node)

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['uuid', 'namespace', 'type',
                         'source url', 'versions', 'state', 'shared']
        for meta in artifacts:
            versions = '%d of %d' % (len(meta.get('blobs', [])),
                                     meta.get('index', 'unknown'))
            x.add_row([meta.get('uuid', ''), meta.get('namespace', ''),
                       meta.get('artifact_type', ''),
                       meta.get('source_url', ''), versions,
                       meta.get('state', ''), meta.get('shared', False)])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('uuid,namespace,type,source_url,versions,state,shared')
        for meta in artifacts:
            versions = '%d of %d' % (len(meta.get('blobs', [])),
                                     meta.get('index', 'unknown'))
            print('{},{},{},{},{},{},{}'.format(
                meta.get('uuid', ''), meta.get('namespace', ''),
                meta.get('artifact_type', ''),
                meta.get('source_url', ''), versions,
                meta.get('state', ''), meta.get('shared', False)))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(artifacts, indent=4, sort_keys=True))


@artifact.command(name='show', help='Show an artifact')
@click.argument('artifact_ref', type=click.STRING, shell_complete=_get_artifacts)
@click.pass_context
def artifact_show(ctx, artifact_ref=None):
    a = ctx.obj['CLIENT'].get_artifact(artifact_ref)
    if not a:
        print('Artifact not found')
        sys.exit(1)

    if not ctx.obj['CLIENT'].check_capability('artifact-metadata'):
        metadata = {}
    else:
        metadata = ctx.obj['CLIENT'].get_artifact_metadata(a['uuid'])

    if ctx.obj['OUTPUT'] == 'json':
        out = util.filter_dict(a, ['uuid', 'namespace', 'artifact_type', 'state',
                                   'source_url', 'blob_uuid', 'index', 'blobs',
                                   'max_versions', 'shared'])
        out['metadata'] = metadata
        print(json.dumps(out, indent=4, sort_keys=True))
        return

    if ctx.obj['OUTPUT'] == 'simple':
        format_string = '%s:%s'
    else:
        format_string = '%-25s: %s'

    print(format_string % ('uuid', a['uuid']))
    print(format_string % ('namespace', a.get('namespace', '')))
    print(format_string % ('type', a['artifact_type']))
    print(format_string % ('state', a['state']))
    print(format_string % ('source url', a['source_url']))
    print(format_string % ('current version blob uuid', a.get('blob_uuid', 'None')))
    print(format_string % ('number of versions', len(a.get('blobs'))))
    print(format_string % ('maximum versions', a['max_versions']))
    print(format_string % ('shared', a.get('shared', False)))

    print()
    if ctx.obj['OUTPUT'] == 'pretty':
        format_string = '    %-8s: %s'
        print('Metadata:')
        for key in metadata:
            print(format_string % (key, metadata[key]))
    else:
        print('metadata,key,value')
        for key in metadata:
            print(f'metadata,{key},{metadata[key]}')

    if ctx.obj['OUTPUT'] == 'simple':
        print('version,size,instance')
        format_string = '%s:%0.1fMB,%s'
        for ver, info in a.get('blobs', {}).items():
            print(format_string % (ver,
                                   int(info['size'])/1024/1024,
                                   ','.join(info['instances'])))

    else:
        print('\nVersions:')
        format_string = '    %-2s : blob %s is %0.1fMB %s %s'
        for ver, info in a.get('blobs', {}).items():
            if info['instances']:
                in_use_by = 'in use by instances'
            else:
                in_use_by = ''
            print(format_string % (ver, info['uuid'],
                                   int(info['size'])/1024/1024,
                                   in_use_by,
                                   ', '.join(info['instances'])))


@artifact.command(name='versions', help='Show versions of an artifact')
@click.argument('artifact_ref', type=click.STRING, shell_complete=_get_artifacts)
@click.pass_context
def artifact_versions(ctx, artifact_ref=None):
    vers = ctx.obj['CLIENT'].get_artifact_versions(artifact_ref)
    print(json.dumps(vers, indent=4, sort_keys=True))


@artifact.command(name='delete', help='Delete an artifact')
@click.argument('artifact_ref', type=click.STRING, shell_complete=_get_artifacts)
@click.pass_context
def artifact_delete(ctx, artifact_ref=None):
    out = ctx.obj['CLIENT'].delete_artifact(artifact_ref)
    if ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(out, indent=4, sort_keys=True))


@artifact.command(name='delete-version', help='Delete an artifact version')
@click.argument('artifact_ref', type=click.STRING, shell_complete=_get_artifacts)
@click.argument('version_id', type=click.INT)
@click.pass_context
def artifact_delete_version(ctx, artifact_ref=None, version_id=0):
    ctx.obj['CLIENT'].delete_artifact_version(artifact_ref, str(version_id))


@artifact.command(name='max-versions',
                  help='Set the maximum number of versions of an artifact')
@click.argument('artifact_ref', type=click.STRING, shell_complete=_get_artifacts)
@click.argument('max_versions', type=click.INT)
@click.pass_context
def max_versions(ctx, artifact_ref, max_versions):
    ctx.obj['CLIENT'].set_artifact_max_versions(artifact_ref, max_versions)


@artifact.command(name='share', help='Share an artifact')
@click.argument('artifact_ref', type=click.STRING, shell_complete=_get_artifacts)
@click.pass_context
def artifact_share(ctx, artifact_ref=None):
    ctx.obj['CLIENT'].share_artifact(artifact_ref)


@artifact.command(name='unshare', help='Unshare an artifact')
@click.argument('artifact_ref', type=click.STRING, shell_complete=_get_artifacts)
@click.pass_context
def artifact_unshare(ctx, artifact_ref=None):
    ctx.obj['CLIENT'].unshare_artifact(artifact_ref)


@artifact.command(name='events', help='Display events for an artifact')
@click.argument('artifact_ref', type=click.STRING, shell_complete=_get_artifacts)
@click.option('-t', '--type', help='The event type to return')
@click.option('-l', '--limit', help='The maximum number of events to return')
@click.pass_context
def artifact_events(ctx, artifact_ref=None, type=None, limit=None):
    events = ctx.obj['CLIENT'].get_artifact_events(artifact_ref, event_type=type, limit=limit)
    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['timestamp', 'node', 'duration', 'message', 'extra']
        for e in events:
            e['timestamp'] = datetime.datetime.fromtimestamp(e['timestamp'])
            x.add_row([e['timestamp'], e['fqdn'], e['duration'], e['message'],
                       e.get('extra', '')])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('timestamp,node,duration,message,extra')
        for e in events:
            e['timestamp'] = datetime.datetime.fromtimestamp(e['timestamp'])
            print('%s,%s,%s,%s,%s'
                  % (e['timestamp'], e['fqdn'], e['duration'], e['message'],
                     e.get('extra', '')))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(events, indent=4, sort_keys=True))


@artifact.command(name='set-metadata', help='Set a metadata item')
@click.argument('artifact_ref', type=click.STRING, shell_complete=_get_artifacts)
@click.argument('key', type=click.STRING)
@click.argument('value', type=click.STRING)
@click.pass_context
def instance_set_metadata(ctx, artifact_ref=None, key=None, value=None):
    if not ctx.obj['CLIENT'].check_capability('artifact-metadata'):
        sys.stderr.write(
            'Unfortunately this server does not implement artifact metadata.\n')
        sys.exit(1)
    ctx.obj['CLIENT'].set_instance_metadata_item(artifact_ref, key, value)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@artifact.command(name='delete-metadata', help='Delete a metadata item')
@click.argument('artifact_ref', type=click.STRING, shell_complete=_get_artifacts)
@click.argument('key', type=click.STRING)
@click.pass_context
def instance_delete_metadata(ctx, artifact_ref=None, key=None):
    if not ctx.obj['CLIENT'].check_capability('artifact-metadata'):
        sys.stderr.write(
            'Unfortunately this server does not implement artifact metadata.\n')
        sys.exit(1)
    ctx.obj['CLIENT'].delete_instance_metadata_item(artifact_ref, key)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')
