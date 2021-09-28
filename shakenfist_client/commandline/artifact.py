import click
import json
import os
from prettytable import PrettyTable
from tqdm import tqdm
import sys


@click.group(help='Artifact commands')
def artifact():
    pass


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
@click.pass_context
def artifact_upload(ctx, name=None, source=None):
    st = os.stat(source)
    buffer_size = 102400

    upload = ctx.obj['CLIENT'].create_upload()
    total = 0
    with tqdm(total=st.st_size, unit='B', unit_scale=True,
              desc='Uploading %s to %s' % (upload['uuid'], upload['node'])) as pbar:
        with open(source, 'rb') as f:
            d = f.read(buffer_size)
            while d:
                remote_total = ctx.obj['CLIENT'].send_upload(upload['uuid'], d)
                sent = len(d)
                total += sent
                pbar.update(sent)

                if total != remote_total:
                    print('Remote side has %d, we have sent %d!'
                          % (remote_total, total))
                    sys.exit(1)

                d = f.read(buffer_size)

    artifact = ctx.obj['CLIENT'].upload_artifact(name, f)
    print('Created artifact %s' % artifact['uuid'])


@artifact.command(name='list', help='List artifacts.')
@click.pass_context
def artifact_list(ctx, node=None):
    artifacts = ctx.obj['CLIENT'].get_artifacts(node)

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['uuid', 'type', 'source url', 'versions', 'state']
        for meta in artifacts:
            x.add_row([meta.get('uuid', ''), meta.get('artifact_type', ''),
                       meta.get('source_url', ''), meta.get('index', ''),
                       meta.get('state', '')])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('uuid,type,source_url,versions,state')
        for meta in artifacts:
            print('%s,%s,%s,%s,%s' % (
                meta.get('uuid', ''), meta.get('artifact_type', ''),
                meta.get('source_url', ''), meta.get('index', ''),
                meta.get('state', '')))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(artifacts))
