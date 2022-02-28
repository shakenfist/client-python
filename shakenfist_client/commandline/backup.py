import click
import http
import json
import os
import requests
import sys
from tqdm import tqdm
import urllib3


@click.group(help='Backup commands')
def backup():
    pass


@backup.command(name='create', help='Create a new backup.')
@click.argument('destination', type=click.Path(exists=False))
@click.pass_context
def artifact_list(ctx, destination=None):
    summary = {
        'artifacts': {},
        'blobs': {}
    }

    artifacts = ctx.obj['CLIENT'].get_artifacts()
    for artifact in artifacts:
        summary['artifacts'][artifact['uuid']] = artifact
        for blob_index in artifact['blobs']:
            blob_ref = artifact['blobs'][blob_index]
            summary['blobs'][blob_ref['uuid']] = None

    blobs = ctx.obj['CLIENT'].get_blobs()
    for blob in blobs:
        summary['blobs'][blob['uuid']] = blob

    with open(destination, 'w') as f:
        f.write(json.dumps(summary, indent=4, sort_keys=True))
    print('Created summary at %s' % destination)

    if not os.path.exists('blobs'):
        os.makedirs('blobs')

    for blob in summary['blobs']:
        if os.path.exists('blobs/%s' % blob):
            print('Already have %s' % blob)
        else:
            blob_path = 'blobs/%s.partial' % blob
            size = summary['blobs'][blob]['size']
            total = 0

            with tqdm(total=size, unit='B', unit_scale=True,
                      desc='Downloading %s to %s' % (blob, blob_path)) as pbar:
                with open(blob_path, 'wb') as f:
                    while size != total:
                        this_attempt = 0

                        try:

                            for chunk in ctx.obj['CLIENT'].get_blob_data(
                                    blob, offset=total):
                                received = len(chunk)
                                f.write(chunk)
                                pbar.update(received)
                                total += received
                                this_attempt += received

                        except (http.client.IncompleteRead,
                                urllib3.exceptions.ProtocolError,
                                requests.exceptions.ChunkedEncodingError) as e:
                            if this_attempt == 0:
                                raise e

            if total != size:
                print('Remote side has %d, we have sent %d!' % (size, total))
                sys.exit(1)

            os.rename(blob_path, 'blobs/%s' % blob)
