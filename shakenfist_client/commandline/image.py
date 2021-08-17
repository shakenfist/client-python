import click
import json
from prettytable import PrettyTable


@click.group(help='Image commands')
def image():
    pass


@image.command(name='cache',
               help=('Cache an image.\n\n'
                     'IMAGE_URL: The URL of the image to cache'))
@click.argument('image_url', type=click.STRING)
@click.pass_context
def image_cache(ctx, image_url=None):
    ctx.obj['CLIENT'].cache_image(image_url)
    if ctx.obj['OUTPUT'] == 'json':
        print('{}')


@image.command(name='list',
               help='List cached images.\n\n'
                    'NODE: Only list images cached on NODE')
@click.option('--node', type=click.STRING)
@click.pass_context
def image_list(ctx, node=None):
    images = ctx.obj['CLIENT'].get_images(node)

    if ctx.obj['OUTPUT'] == 'pretty':
        x = PrettyTable()
        x.field_names = ['ref', 'node', 'url', 'size', 'modified', 'fetched',
                         'file_version', 'checksum']
        x.align['url'] = 'l'
        x.sortby = 'url'
        for meta in images:
            x.add_row([meta.get('ref', ''), meta.get('node', ''),
                       meta.get('url', ''), meta.get('size', ''),
                       meta.get('modified', ''), meta.get('fetched', ''),
                       meta.get('file_version', ''), meta.get('checksum', '')])
        print(x)

    elif ctx.obj['OUTPUT'] == 'simple':
        print('ref,node,url,size,modified,fetched,file_version,checksum')
        for meta in images:
            print('%s,%s,%s,%s,%s,%s,%s,%s' % (
                meta.get('ref', ''), meta.get('node', ''),
                meta.get('url', ''), meta.get('size', ''),
                meta.get('modified', ''), meta.get('fetched', ''),
                meta.get('file_version', ''), meta.get('checksum', '')))

    elif ctx.obj['OUTPUT'] == 'json':
        print(json.dumps(images))
