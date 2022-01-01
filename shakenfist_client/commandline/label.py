import click


@click.group(help='Label commands')
def label():
    pass


@label.command(name='update',
               help=('Update a label to use a new blob.\n\n'
                     'LABEL:     The name of the label to update\n\n'
                     'BLOB_UUID: The UUID of the blob to use.'))
@click.argument('label', type=click.STRING)
@click.argument('blob_uuid', type=click.STRING)
@click.pass_context
def label_update(ctx, label, blob_uuid=None):
    ctx.obj['CLIENT'].update_label(label, blob_uuid)
