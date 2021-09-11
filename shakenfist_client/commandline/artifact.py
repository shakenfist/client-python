import click
import json
from prettytable import PrettyTable


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
