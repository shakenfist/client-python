import hashlib
import os
import sys
import time

from tqdm import tqdm

from shakenfist_client import apiclient


def filter_dict(d, allowed_keys):
    out = {}
    for key in allowed_keys:
        if key in d:
            out[key] = d[key]
    return out


def show_interface(ctx, interface, out=[]):
    if not interface:
        print('Interface not found')
        sys.exit(1)

    if not ctx.obj['CLIENT'].check_capability('interface-metadata'):
        metadata = {}
    else:
        metadata = ctx.obj['CLIENT'].get_interface_metadata(interface['uuid'])

    if ctx.obj['OUTPUT'] == 'json':
        if 'network_interfaces' not in out:
            out['network_interfaces'] = []

        out['network_interfaces'].append(
            filter_dict(
                interface, ['uuid', 'network_uuid', 'macaddr', 'order',
                            'ipv4', 'floating', 'model']))
        return

    if ctx.obj['OUTPUT'] == 'pretty':
        format_string = '    %-8s: %s'
        print(format_string % ('uuid', interface['uuid']))
        print(format_string % ('network', interface['network_uuid']))
        print(format_string % ('macaddr', interface['macaddr']))
        print(format_string % ('order', interface['order']))
        print(format_string % ('ipv4', interface.get('ipv4', '')))
        print(format_string % ('floating', interface.get('floating', '')))
        print(format_string % ('model', interface['model']))
    else:
        print('iface,%s,%s,%s,%s,%s,%s,%s'
              % (interface['uuid'], interface['network_uuid'],
                 interface['macaddr'], interface['order'], interface.get(
                     'ipv4', ''),
                 interface.get('floating', ''), interface['model']))

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


def get_client(ctx):
    client = None
    if hasattr(ctx, 'obj') and ctx.obj:
        client = ctx.obj.get('CLIENT')
    if not client:
        client = apiclient.Client(
            namespace=os.environ.get('SHAKENFIST_NAMESPACE'),
            key=os.environ.get('SHAKENFIST_KEY'),
            base_url=os.environ.get('SHAKENFIST_API_URL', 'http://localhost:13000'),
            async_strategy=os.environ.get('SHAKENFIST_ASYNC', 'pause')
        )
    return client


def get_networks(ctx, args, incomplete):
    networks = get_client(ctx).get_networks()
    choices = [n[key] for key in ['uuid', 'name'] for n in networks]
    return [arg for arg in choices if arg.startswith(incomplete)]


def checksum_with_progress(client, source):
    st = os.stat(source)
    with open(source, 'rb') as f:
        return checksum_with_progress_from_file_like_object(
            client, f, st.st_size)


def checksum_with_progress_from_file_like_object(client, source_file_object, size):
    sha512_hash = hashlib.sha512()
    with tqdm(total=size, unit='B', unit_scale=True,
              desc='Calculate checksum') as pbar:
        while d := source_file_object.read(4096):
            sha512_hash.update(d)
            pbar.update(len(d))

    print('Searching for a pre-existing blob with this hash...')
    return client.get_blob_by_sha512(sha512_hash.hexdigest())


def upload_artifact_with_progress(client, name, source, source_url,
                                  namespace=None, shared=False):
    st = os.stat(source)
    with open(source, 'rb') as f:
        return upload_artifact_with_progress_file_like_object(
            client, name, f, st.st_size, source_url, namespace=namespace,
            shared=shared)


def upload_artifact_with_progress_file_like_object(
        client, name, source_file_object, size, source_url, namespace=None,
        shared=False):
    # We do not use send_upload_file because we want to hook in our own
    # progress bar.
    buffer_size = 4096
    upload = client.create_upload()
    total = 0
    retries = 0
    with tqdm(total=size, unit='B', unit_scale=True,
              desc='Uploading {} to {}'.format(upload['uuid'], upload['node'])) as pbar:
        while d := source_file_object.read(buffer_size):
            start_time = time.time()
            try:
                remote_total = client.send_upload(upload['uuid'], d)
                retries = 0
            except apiclient.APIException as e:
                retries += 1

                if retries > 5:
                    print('Repeated failures, aborting')
                    raise e

                print('Upload error, retrying...')
                client.truncate_upload(upload['uuid'], total)
                source_file_object.seek(total)
                buffer_size = 4096
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
                print('Remote side has %d, we have sent %d!' % (remote_total, total))
                sys.exit(1)

    print('Creating artifact')
    artifact = client.upload_artifact(
        name, upload['uuid'], source_url=source_url, shared=shared, namespace=namespace)
    return artifact
