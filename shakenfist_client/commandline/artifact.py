import click
import json
import os
from prettytable import PrettyTable
from tqdm import tqdm
import sys
import time

from shakenfist_client import apiclient
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
@click.pass_context
def artifact_cache(ctx, image_url=None):
    ctx.obj['CLIENT'].cache_artifact(image_url)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@artifact.command(name='upload', help='Upload an artifact.')
@click.argument('name', type=click.STRING)
@click.argument('source', type=click.Path(exists=True))
@click.option('--source_url', default=None,
              help='A URL to act as if this artifact was downloaded from.')
@click.pass_context
def artifact_upload(ctx, name=None, source=None, source_url=None):
    st = os.stat(source)
    buffer_size = 4096

    upload = ctx.obj['CLIENT'].create_upload()
    total = 0
    retries = 0
    with tqdm(total=st.st_size, unit='B', unit_scale=True,
              desc='Uploading %s to %s' % (upload['uuid'], upload['node'])) as pbar:
        with open(source, 'rb') as f:
            d = f.read(buffer_size)
            while d:
                start_time = time.time()
                try:
                    remote_total = ctx.obj['CLIENT'].send_upload(
                        upload['uuid'], d)
                    retries = 0
                except apiclient.APIException as e:
                    retries += 1

                    if retries > 5:
                        print('Repeated failures, aborting')
                        raise e

                    print('Upload error, retrying...')
                    ctx.obj['CLIENT'].truncate_upload(upload['uuid'], total)
                    f.seek(total)
                    buffer_size = 4096
                    d = f.read(buffer_size)
                    continue

                # We aim for each chunk to take three seconds to transfer. This is
                # partially because of the API timeout on the other end, but also
                # so that uploads don't appear to stall over very slow networks.
                # However, the buffer size must also always be between 4kb and 4mb.
                elapsed = time.time() - start_time
                buffer_size = int(buffer_size * 3.0 / elapsed)
                buffer_size = max(4 * 1024, buffer_size)
                buffer_size = min(2 * 1024 * 1024, buffer_size)

                sent = len(d)
                total += sent
                pbar.update(sent)

                if total != remote_total:
                    print('Remote side has %d, we have sent %d!'
                          % (remote_total, total))
                    sys.exit(1)

                d = f.read(buffer_size)

    artifact = ctx.obj['CLIENT'].upload_artifact(name, upload['uuid'],
                                                 source_url=source_url)
    print('Created artifact %s' % artifact['uuid'])


@artifact.command(name='download', help='Download an artifact.')
@click.argument('artifact_uuid', type=click.STRING, autocompletion=_get_artifacts)
@click.argument('destination', type=click.Path(exists=False))
@click.pass_context
def artifact_download(ctx, artifact_uuid=None, destination=None):
    a = ctx.obj['CLIENT'].get_artifact(artifact_uuid)

    if not a:
        print('Artifact not found')
        sys.exit(1)

    blob_uuid = a.get('blob_uuid')
    blob_index = a.get('index')
    if not blob_uuid:
        print('Artifact has no verions')

    size = a['blobs'][blob_index]['size']
    print('%s -> %s of %d bytes' % (artifact_uuid, blob_uuid, size))

    total = 0
    with tqdm(total=size, unit='B', unit_scale=True,
              desc='Downloading %s to %s' % (artifact_uuid, destination)) as pbar:
        with open(destination, 'wb') as f:
            for chunk in ctx.obj['CLIENT'].get_blob(blob_uuid):
                received = len(chunk)
                f.write(chunk)
                pbar.update(received)
                total += received

    if total != size:
        print('Remote side has %d, we have sent %d!' % (size, total))
        sys.exit(1)

    print('Download complete')


@artifact.command(name='list', help='List artifacts.')
@click.pass_context
def artifact_list(ctx, node=None):
    artifacts = ctx.obj['CLIENT'].get_artifacts(node)

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['uuid', 'type', 'source url', 'versions', 'state']
        for meta in artifacts:
            versions = '%d of %d' % (len(meta.get('blobs', [])),
                                     meta.get('index', 'unknown'))
            x.add_row([meta.get('uuid', ''), meta.get('artifact_type', ''),
                       meta.get('source_url', ''), versions,
                       meta.get('state', '')])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('uuid,type,source_url,versions,state')
        for meta in artifacts:
            versions = '%d of %d' % (len(meta.get('blobs', [])),
                                     meta.get('index', 'unknown'))
            print('%s,%s,%s,%s,%s' % (
                meta.get('uuid', ''), meta.get('artifact_type', ''),
                meta.get('source_url', ''), versions,
                meta.get('state', '')))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(artifacts))


@artifact.command(name='show', help='Show an artifact')
@click.argument('artifact_uuid', type=click.STRING, autocompletion=_get_artifacts)
@click.pass_context
def artifact_show(ctx, artifact_uuid=None):
    a = ctx.obj['CLIENT'].get_artifact(artifact_uuid)

    if not a:
        print('Artifact not found')
        sys.exit(1)

    if ctx.obj['OUTPUT'] == 'json':
        out = util.filter_dict(a, ['uuid', 'artifact_type', 'state', 'source_url',
                                   'blob_uuid', 'index', 'blobs', 'max_versions'])
        print(json.dumps(out, indent=4, sort_keys=True))
        return

    if ctx.obj['OUTPUT'] == 'simple':
        format_string = '%s:%s'
    else:
        format_string = '%-25s: %s'

    print(format_string % ('uuid', a['uuid']))
    print(format_string % ('type', a['artifact_type']))
    print(format_string % ('state', a['state']))
    print(format_string % ('source url', a['source_url']))
    print(format_string
          % ('current version blob uuid', a.get('blob_uuid', 'None')))
    print(format_string % ('number of versions', len(a.get('blobs'))))
    print(format_string % ('maximum versions', a['max_versions']))

    if ctx.obj['OUTPUT'] == 'simple':
        print('version,size,instance')
        format_string = '%s:%0.1fMB,%s'
        for ver, info in a.get('blobs', {}).items():
            print(format_string % (ver,
                                   int(info['size'])/1024/1024,
                                   ','.join(info['instances'])))

    else:
        print('\nVersions used by Instances:')
        format_string = '    %-2s : %0.1fMB  %s'
        for ver, info in a.get('blobs', {}).items():
            print(format_string % (ver,
                                   int(info['size'])/1024/1024,
                                   ', '.join(info['instances'])))


@artifact.command(name='versions', help='Show versions of an artifact')
@click.argument('artifact_uuid', type=click.STRING, autocompletion=_get_artifacts)
@click.pass_context
def artifact_versions(ctx, artifact_uuid=None):
    vers = ctx.obj['CLIENT'].get_artifact_versions(artifact_uuid)
    print(json.dumps(vers, indent=4, sort_keys=True))


@artifact.command(name='delete', help='Delete an artifact')
@click.argument('artifact_uuid', type=click.STRING, autocompletion=_get_artifacts)
@click.pass_context
def artifact_delete(ctx, artifact_uuid=None):
    ctx.obj['CLIENT'].delete_artifact(artifact_uuid)


@artifact.command(name='delete-version', help='Delete an artifact version')
@click.argument('artifact_uuid', type=click.STRING, autocompletion=_get_artifacts)
@click.argument('version_id', type=click.INT)
@click.pass_context
def artifact_delete_version(ctx, artifact_uuid=None, version_id=0):
    ctx.obj['CLIENT'].delete_artifact_version(artifact_uuid, str(version_id))


@artifact.command(name='max-versions',
                  help='Set the maximum number of versions of an artifact')
@click.argument('artifact_uuid', type=click.STRING, autocompletion=_get_artifacts)
@click.argument('max_versions', type=click.INT)
@click.pass_context
def max_versions(ctx, artifact_uuid, max_versions):
    ctx.obj['CLIENT'].set_artifact_max_versions(artifact_uuid, max_versions)
