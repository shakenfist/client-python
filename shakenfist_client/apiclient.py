import copy
import errno
import json
import logging
import os
from pbr.version import VersionInfo
import requests
import time


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


# Async strategies
ASYNC_CONTINUE = 'continue'
ASYNC_PAUSE = 'pause'
ASYNC_BLOCK = 'block'


class UnconfiguredException(Exception):
    pass


class IncapableException(Exception):
    pass


class InvalidException(Exception):
    pass


class APIException(Exception):
    def __init__(self, message, method, url, status_code, text):
        self.message = message
        self.method = method
        self.url = url
        self.status_code = status_code
        self.text = text


class RequestMalformedException(APIException):
    pass


class UnauthenticatedException(APIException):
    pass


class UnauthorizedException(APIException):
    pass


class ResourceNotFoundException(APIException):
    pass


class DependenciesNotReadyException(APIException):
    pass


class ResourceStateConflictException(APIException):
    pass


class InternalServerError(APIException):
    pass


class InsufficientResourcesException(APIException):
    pass


class UnknownAsyncStrategy(APIException):
    pass


STATUS_CODES_TO_ERRORS = {
    400: RequestMalformedException,
    401: UnauthenticatedException,
    403: UnauthorizedException,
    404: ResourceNotFoundException,
    406: DependenciesNotReadyException,
    409: ResourceStateConflictException,
    500: InternalServerError,
    507: InsufficientResourcesException,
}


def _calculate_async_deadline(strategy):
    if strategy == ASYNC_CONTINUE:
        return -1
    if strategy == ASYNC_PAUSE:
        return 60
    if strategy == ASYNC_BLOCK:
        return 3600
    raise UnknownAsyncStrategy('Async strategy %s is unknown' % strategy)


def _correct_blob_indexes(d):
    # JSON requires dictionary keys to be strings. Reverse that for the blobs
    # element here to reduce confusion.
    new_blobs = {}
    for k in d.get('blobs', []):
        new_blobs[int(k)] = d['blobs'][k]
    d['blobs'] = new_blobs
    LOG.debug('Rewrote blob keys to ints')
    return d


class Client(object):
    def __init__(self, base_url=None, verbose=False,
                 namespace=None, key=None, sync_request_timeout=300,
                 suppress_configuration_lookup=False, logger=None,
                 async_strategy=ASYNC_BLOCK):
        global LOG
        if verbose:
            LOG.setLevel(logging.DEBUG)
        if logger:
            LOG = logger

        self.sync_request_timeout = sync_request_timeout

        LOG.debug('Client initially configured with apiurl of %s for namespace %s '
                  'and async strategy %s'
                  % (base_url, namespace, async_strategy))

        if not suppress_configuration_lookup:
            # Where do we find authentication details? First off, we try command line
            # flags; then environment variables (thanks for doing this for free click);
            # ~/.shakenfist (which is a JSON file); and finally /etc/sf/shakenfist.json.
            if not base_url:
                LOG.debug('Testing for ~/.shakenfist')
                user_conf = os.path.expanduser('~/.shakenfist')
                if os.path.exists(user_conf):
                    LOG.debug('Loading configuration from ~/.shakenfist')
                    with open(user_conf) as f:
                        d = json.loads(f.read())
                        if not namespace:
                            namespace = d['namespace']
                        if not key:
                            key = d['key']
                        if not base_url:
                            base_url = d['apiurl']

            if not base_url:
                LOG.debug('Testing for /etc/sf/shakenfist.json')
                try:
                    if os.path.exists('/etc/sf/shakenfist.json'):
                        LOG.debug(
                            'Loading configuration from /etc/sf/shakenfist.json')
                        with open('/etc/sf/shakenfist.json') as f:
                            d = json.loads(f.read())
                            if not namespace:
                                namespace = d['namespace']
                            if not key:
                                key = d['key']
                            if not base_url:
                                base_url = d['apiurl']
                except IOError as e:
                    if e.errno != errno.EACCES:
                        raise

        if not base_url:
            raise UnconfiguredException(
                'You have not specified the server to communicate with')

        self.base_url = base_url
        self.namespace = namespace
        self.key = key
        self.async_strategy = async_strategy
        LOG.debug('Client configured with apiurl of %s for namespace %s '
                  'and async strategy %s'
                  % (self.base_url, self.namespace, self.async_strategy))

        self.cached_auth = None

        # Request capabilities information
        self._collect_capabilities()

    def _collect_capabilities(self):
        r = requests.request('GET', self.base_url, allow_redirects=True)
        self.root_html = r.text

    def check_capability(self, capability_string):
        # NOTE(mikal): this likely needs to be fancier
        return capability_string in self.root_html

    def _actual_request_url(self, method, url, data=None,
                            request_body_is_binary=False,
                            response_body_is_binary=False,
                            allow_redirects=True, stream=False):
        url = self.base_url + url

        h = {'Authorization': self.cached_auth,
             'User-Agent': get_user_agent()}
        if data:
            if request_body_is_binary:
                h['Content-Type'] = 'application/octet-stream'
            else:
                h['Content-Type'] = 'application/json'
                data = json.dumps(data, indent=4, sort_keys=True)

        start_time = time.time()
        r = requests.request(method, url, data=data, headers=h,
                             allow_redirects=allow_redirects, stream=stream)
        end_time = time.time()

        LOG.debug('-------------------------------------------------------')
        LOG.debug('API client requested: %s %s' % (method, url))
        if data:
            if request_body_is_binary:
                LOG.debug('Data: ...%d bytes of binary omitted...' % len(data))
            else:
                LOG.debug('Data:\n    %s' % '\n    '.join(data.split('\n')))
        for h in r.history:
            LOG.debug('URL request history: %s --> %s %s'
                      % (h.url, h.status_code, h.headers.get('Location')))
        LOG.debug('API client response: code = %s (took %.02f seconds)'
                  % (r.status_code, (end_time - start_time)))

        if not stream and r.text:
            if response_body_is_binary:
                LOG.debug('Data: ...%d bytes of binary omitted...'
                          % len(r.text))
            else:
                try:
                    LOG.debug('Data:\n    %s'
                              % ('\n    '.join(json.dumps(json.loads(r.text),
                                                          indent=4,
                                                          sort_keys=True).split('\n'))))
                except Exception:
                    LOG.debug('Text:\n    %s'
                              % ('\n    '.join(r.text.split('\n'))))
        LOG.debug('-------------------------------------------------------')

        if r.status_code in STATUS_CODES_TO_ERRORS:
            raise STATUS_CODES_TO_ERRORS[r.status_code](
                'API request failed', method, url, r.status_code, r.text)

        acceptable = [200]
        if not allow_redirects:
            acceptable.append(301)
        if r.status_code not in acceptable:
            raise APIException(
                'API request failed', method, url, r.status_code, r.text)
        return r

    def _authenticate(self):
        LOG.debug('Authentication request made, contents not logged')
        auth_url = self.base_url + '/auth'
        r = requests.request('POST', auth_url,
                             data=json.dumps(
                                 {'namespace': self.namespace,
                                  'key': self.key}),
                             headers={'Content-Type': 'application/json',
                                      'User-Agent': get_user_agent()})
        if r.status_code != 200:
            raise UnauthenticatedException('API unauthenticated', 'POST', auth_url,
                                           r.status_code, r.text)
        return 'Bearer %s' % r.json()['access_token']

    def _request_url(self, method, url, data=None, request_body_is_binary=False,
                     response_body_is_binary=False, stream=False):
        # NOTE(mikal): if we are not authenticated, probe the base_url looking
        # for redirections. If we are redirected, rewrite our base_url to the
        # redirection target.
        if not self.cached_auth:
            probe = self._actual_request_url('GET', '', allow_redirects=False)
            if probe.status_code == 301:
                LOG.debug('API server redirects to %s'
                          % probe.headers['Location'])
                self.base_url = probe.headers['Location']
            self.cached_auth = self._authenticate()

        deadline = time.time() + _calculate_async_deadline(self.async_strategy)
        while True:
            try:
                try:
                    return self._actual_request_url(
                        method, url, data=data,
                        request_body_is_binary=request_body_is_binary,
                        response_body_is_binary=response_body_is_binary,
                        stream=stream)
                except UnauthenticatedException:
                    self.cached_auth = self._authenticate()
                    return self._actual_request_url(
                        method, url, data=data,
                        request_body_is_binary=request_body_is_binary,
                        response_body_is_binary=response_body_is_binary,
                        stream=stream)

            except DependenciesNotReadyException as e:
                # The API server will return a 406 exception when we have
                # specified an operation which depends on a resource and
                # that resource is not in the created state.
                if time.time() > deadline:
                    LOG.debug('Deadline exceeded waiting for dependancies')
                    raise e

                LOG.debug('Dependencies not ready, retrying')
                time.sleep(1)

    # The metadata calls are repetitive and handled here as a group
    def _get_metadata(self, object_plural, object_reference):
        r = self._request_url(
            'GET', '/' + object_plural + '/' + object_reference + '/metadata')
        return r.json()

    def get_artifact_metadata(self, artifact_ref):
        return self._get_metadata('artifacts', artifact_ref)

    def get_blob_metadata(self, blob_uuid):
        return self._get_metadata('blobs', blob_uuid)

    def get_interface_metadata(self, interface_uuid):
        return self._get_metadata('interfaces', interface_uuid)

    def get_instance_metadata(self, instance_ref):
        return self._get_metadata('instances', instance_ref)

    def get_namespace_metadata(self, namespace):
        return self._get_metadata('auth/namespaces', namespace)

    def get_network_metadata(self, network_ref):
        return self._get_metadata('networks', network_ref)

    def get_node_metadata(self, node):
        return self._get_metadata('nodes', node)

    def _set_metadata(self, object_plural, object_reference, key, value):
        r = self._request_url(
            'PUT', '/' + object_plural + '/' + object_reference +
            '/metadata/' + key, data={'value': value})
        return r.json()

    def set_artifact_metadata_item(self, artifact_ref, key, value):
        return self._set_metadata('artifacts', artifact_ref, key, value)

    def set_blob_metadata_item(self, blob_uuid, key, value):
        return self._set_metadata('blobs', blob_uuid, key, value)

    def set_interface_metadata_item(self, interface_uuid, key, value):
        return self._set_metadata('interfaces', interface_uuid, key, value)

    def set_instance_metadata_item(self, instance_ref, key, value):
        return self._set_metadata('instances', instance_ref, key, value)

    def set_namespace_metadata_item(self, namespace, key, value):
        return self._set_metadata('auth/namespaces', namespace, key, value)

    def set_network_metadata_item(self, network_ref, key, value):
        return self._set_metadata('networks', network_ref, key, value)

    def set_node_metadata_item(self, node, key, value):
        return self._set_metadata('nodes', node, key, value)

    def _delete_metadata(self, object_plural, object_reference, key):
        r = self._request_url(
            'DELETE', '/' + object_plural + '/' + object_reference +
            '/metadata/' + key)
        return r.json()

    def delete_artifact_metadata_item(self, artifact_ref, key):
        return self._delete_metadata('artifacts', artifact_ref, key)

    def delete_blob_metadata_item(self, blob_uuid, key):
        return self._delete_metadata('blobs', blob_uuid, key)

    def delete_interface_metadata_item(self, interface_uuid, key):
        return self._delete_metadata('interfaces', interface_uuid, key)

    def delete_instance_metadata_item(self, instance_ref, key):
        return self._delete_metadata('instances', instance_ref, key)

    def delete_namespace_metadata_item(self, namespace, key):
        return self._delete_metadata('auth/namespaces', namespace, key)

    def delete_network_metadata_item(self, network_ref, key):
        return self._delete_metadata('networks', network_ref, key)

    def delete_node_metadata_item(self, node, key):
        return self._delete_metadata('nodes', node, key)

    # Similarly the event calls are repetitive and handled as a group
    def _get_events(self, object_plural, object_reference):
        r = self._request_url('GET', '/' + object_plural + '/' +
                              object_reference + '/events')
        return r.json()

    def get_artifact_events(self, artifact_ref):
        return self._get_events('artifacts', artifact_ref)

    def get_instance_events(self, instance_ref):
        return self._get_events('instances', instance_ref)

    def get_network_events(self, network_ref):
        return self._get_events('networks', network_ref)

    def get_node_events(self, node):
        return self._get_events('nodes', node)

    # Other calls
    def get_instances(self, all=False):
        r = self._request_url('GET', '/instances', data={'all': all})
        return r.json()

    def delete_all_instances(self, namespace):
        r = self._request_url('DELETE', '/instances',
                              data={'confirm': True,
                                    'namespace': namespace})
        deleted = r.json()
        waiting_for = set(deleted)

        deadline = time.time() + _calculate_async_deadline(self.async_strategy)
        while waiting_for:
            LOG.debug('Waiting for instances to deleted: %s'
                      % ', '.join(waiting_for))
            if time.time() > deadline:
                LOG.debug('Deadline exceeded waiting for instances to delete')
                break

            time.sleep(1)
            for uuid in copy.copy(waiting_for):
                inst = self.get_instance(uuid)
                if not inst or inst['state'] == 'deleted':
                    LOG.debug('Instance %s is now deleted' % uuid)
                    waiting_for.remove(uuid)

        return deleted

    def get_instance(self, instance_ref):
        r = self._request_url('GET', '/instances/' + instance_ref)
        return r.json()

    def get_instance_interfaces(self, instance_ref):
        r = self._request_url('GET', '/instances/' + instance_ref +
                              '/interfaces')
        return r.json()

    def create_instance(self, name, cpus, memory, network, disk, sshkey, userdata,
                        namespace=None, force_placement=None, video=None, uefi=False,
                        configdrive=None, nvram_template=None, secure_boot=False,
                        metadata=None, side_channels=None):
        body = {
            # Values all instances care about
            'name': name,
            'cpus': cpus,
            'memory': memory,
            'network': network,
            'ssh_key': sshkey,
            'user_data': userdata,
            'namespace': namespace,
            'video': video,
            'configdrive': configdrive,
            'metadata': metadata,
            'side_channels': side_channels,

            # UEFI values: secure boot implies UEFI and NVRAM templates are not
            # used for BIOS boot (the default).
            'uefi': uefi,
            'secure_boot': secure_boot,
            'nvram_template': nvram_template
        }

        if force_placement:
            body['placed_on'] = force_placement

        # Ensure size is always an int if specified
        clean_disks = []
        for d in disk:
            if 'size' in d and d['size']:
                d['size'] = int(d['size'])
            clean_disks.append(d)
        body['disk'] = clean_disks

        r = self._request_url('POST', '/instances',
                              data=body)
        i = r.json()

        deadline = time.time() + _calculate_async_deadline(self.async_strategy)
        while True:
            if i['state'] not in ['initial', 'creating']:
                return i

            LOG.debug('Waiting for instance to be created')
            if time.time() > deadline:
                LOG.debug('Deadline exceeded waiting for instance to be created')
                return i

            time.sleep(1)
            i = self.get_instance(i['uuid'])

    def snapshot_instance(self, instance_ref, all=False, device=None, label_name=None,
                          delete_snapshot_after_label=False, thin=False):
        r = self._request_url(
            'POST', '/instances/' + instance_ref + '/snapshot',
            data={'all': all, 'device': device, 'thin': thin})
        out = r.json()

        waiting_for = []
        for s in out:
            waiting_for.append(out[s]['blob_uuid'])

        # If we are going to apply a label, then we must block for the snapshot
        # to complete before we can apply the label.
        async_strategy = self.async_strategy
        if label_name:
            async_strategy = ASYNC_BLOCK

        deadline = time.time() + _calculate_async_deadline(async_strategy)
        while waiting_for:
            LOG.debug('Waiting for snapshots: %s' % ', '.join(waiting_for))
            if time.time() > deadline:
                LOG.debug('Deadline exceeded waiting for snapshots')
                break

            time.sleep(1)
            snaps = self.get_instance_snapshots(instance_ref)
            for s in snaps:
                if s.get('blob_uuid') in waiting_for:
                    if s.get('state') == 'created':
                        LOG.debug('Blob %s now present' % s['blob_uuid'])
                        waiting_for.remove(s['blob_uuid'])
                    else:
                        LOG.debug('Blob %s not yet created' % s['blob_uuid'])

        if not all and label_name:
            # It only makes sense to update a label if we've snapshotted a single
            # disk. Otherwise we'd immediately clobber the label with the last
            # disk in the snapshot series.
            if not device:
                device = list(out.keys())[0]
            out['label'] = self.update_label(
                label_name, out[device]['blob_uuid'])

            if delete_snapshot_after_label:
                self.delete_artifact(out[device]['artifact_uuid'])

        return out

    def get_instance_snapshots(self, instance_ref):
        r = self._request_url('GET', '/instances/' + instance_ref +
                              '/snapshot')
        return r.json()

    def update_label(self, label_name, blob_uuid):
        r = self._request_url(
            'POST', '/label/%s' % label_name, data={'blob_uuid': blob_uuid})
        return _correct_blob_indexes(r.json())

    def reboot_instance(self, instance_ref, hard=False):
        style = 'soft'
        if hard:
            style = 'hard'
        r = self._request_url('POST', '/instances/' + instance_ref +
                              '/reboot' + style)
        return r.json()

    def power_off_instance(self, instance_ref):
        r = self._request_url('POST', '/instances/' + instance_ref +
                              '/poweroff')
        return r.json()

    def power_on_instance(self, instance_ref):
        r = self._request_url('POST', '/instances/' + instance_ref +
                              '/poweron')
        return r.json()

    def pause_instance(self, instance_ref):
        r = self._request_url('POST', '/instances/' + instance_ref +
                              '/pause')
        return r.json()

    def unpause_instance(self, instance_ref):
        r = self._request_url('POST', '/instances/' + instance_ref +
                              '/unpause')
        return r.json()

    def delete_instance(self, instance_ref, namespace=None, async_request=False):
        # Why pass a namespace when you're passing an exact UUID? The idea here
        # is that it provides a consistent interface, but also a safety check
        # against overly zealous loops deleting things.
        data = None
        if namespace:
            data = {'namespace': namespace}
        r = self._request_url('DELETE', '/instances/' +
                              instance_ref, data=data)

        if async_request:
            return {}

        obj_uuid = r.json().get('uuid')
        if not obj_uuid:
            print('ERROR: No instance UUID returned by API')
            return {}

        deadline = time.time() + _calculate_async_deadline(self.async_strategy)
        while True:
            i = self.get_instance(obj_uuid)
            if i['state'] == 'deleted':
                return i

            LOG.debug('Waiting for instance to be deleted')
            if time.time() > deadline:
                LOG.debug('Deadline exceeded waiting for instance to delete')
                return i

            time.sleep(1)

    def cache_artifact(self, image_url, shared=False, namespace=None):
        r = self._request_url('POST', '/artifacts',
                              data={
                                  'url': image_url,
                                  'shared': shared,
                                  'namespace': namespace
                              })
        return r.json()

    def upload_artifact(self, name, upload_uuid, source_url=None, shared=False,
                        namespace=None, artifact_type='image'):
        if '/' in name:
            raise InvalidException('Names must not contain /')

        if artifact_type != 'image':
            if not self.check_capability('artifact-upload-types'):
                raise IncapableException(
                    'The API server version you are talking to does not support '
                    'specifying upload artifact types other than image.')

        r = self._request_url('POST', '/artifacts/upload/%s' % name,
                              data={
                                  'upload_uuid': upload_uuid,
                                  'source_url': source_url,
                                  'shared': shared,
                                  'namespace': namespace
                              })
        return r.json()

    def blob_artifact(self, name, blob_uuid, source_url=None, shared=False,
                      namespace=None):
        if '/' in name:
            raise InvalidException('Names must not contain /')

        r = self._request_url('POST', '/artifacts/upload/%s' % name,
                              data={
                                  'blob_uuid': blob_uuid,
                                  'source_url': source_url,
                                  'shared': shared,
                                  'namespace': namespace
                              })
        return r.json()

    def get_artifact(self, artifact_ref):
        r = self._request_url('GET', '/artifacts/' + artifact_ref)
        return _correct_blob_indexes(r.json())

    def get_artifacts(self, node=None):
        r = self._request_url('GET', '/artifacts', data={'node': node})

        out = []
        for a in r.json():
            out.append(_correct_blob_indexes(a))
        return out

    def get_artifact_versions(self, artifact_ref):
        r = self._request_url(
            'GET', '/artifacts/' + artifact_ref + '/versions')
        return r.json()

    def set_artifact_max_versions(self, artifact_ref, max_versions):
        r = self._request_url('POST',
                              '/artifacts/' + artifact_ref + '/versions',
                              data={'max_versions': max_versions})
        return r.json()

    def delete_artifact(self, artifact_ref):
        r = self._request_url('DELETE', '/artifacts/' + artifact_ref)
        return r.json()

    def delete_artifact_version(self, artifact_ref, version_id):
        r = self._request_url('DELETE', '/artifacts/' + artifact_ref +
                              '/versions/' + str(version_id))
        return r.json()

    def delete_all_artifacts(self, namespace):
        # Unlike instances and networks, artifact deletion isn't a task in the
        # backend, so we don't need to poll here.
        r = self._request_url('DELETE', '/artifacts',
                              data={'confirm': True,
                                    'namespace': namespace})
        return r.json()

    def share_artifact(self, artifact_ref):
        r = self._request_url('POST', '/artifacts/' + artifact_ref + '/share')
        return r.json()

    def unshare_artifact(self, artifact_ref):
        r = self._request_url(
            'POST', '/artifacts/' + artifact_ref + '/unshare')
        return r.json()

    def get_blob(self, blob_uuid):
        r = self._request_url('GET', '/blobs/' + blob_uuid)
        return r.json()

    def get_blob_by_sha512(self, sha512):
        r = self._request_url('GET', '/blob_checksums/sha512/' + sha512)
        return r.json()

    def get_blob_data(self, blob_uuid, offset=0):
        r = self._request_url(
            'GET', '/blobs/' + blob_uuid + '/data?offset=' + str(offset),
            stream=True)
        for chunk in r.iter_content(chunk_size=8192):
            yield chunk

    def get_blobs(self, node=None):
        r = self._request_url('GET', '/blobs', data={'node': node})
        return r.json()

    def get_networks(self, all=False):
        r = self._request_url('GET', '/networks', data={'all': all})
        return r.json()

    def get_network(self, network_ref):
        r = self._request_url('GET', '/networks/' + network_ref)
        return r.json()

    def delete_network(self, network_ref, namespace=None):
        # Why pass a namespace when you're passing an exact UUID? The idea here
        # is that it provides a consistent interface, but also a safety check
        # against overly zealous loops deleting things.
        data = None
        if namespace:
            data = {'namespace': namespace}
        r = self._request_url('DELETE', '/networks/' + network_ref, data=data)
        return r.json()

    def delete_all_networks(self, namespace, clean_wait=False):
        r = self._request_url('DELETE', '/networks',
                              data={'confirm': True,
                                    'namespace': namespace,
                                    'clean_wait': clean_wait,
                                    })
        return r.json()

    def allocate_network(self, netblock, provide_dhcp, provide_nat, name, namespace=None):
        r = self._request_url('POST', '/networks',
                              data={
                                  'netblock': netblock,
                                  'provide_dhcp': provide_dhcp,
                                  'provide_nat': provide_nat,
                                  'name': name,
                                  'namespace': namespace
                              })
        n = r.json()

        deadline = time.time() + _calculate_async_deadline(self.async_strategy)
        while True:
            if n['state'] not in ['initial', 'creating']:
                return n

            LOG.debug('Waiting for network to be created')
            if time.time() > deadline:
                LOG.debug('Deadline exceeded waiting for network to be created')
                return n

            time.sleep(1)
            n = self.get_network(n['uuid'])

    def get_network_interfaces(self, network_ref):
        r = self._request_url('GET', '/networks/' +
                              network_ref + '/interfaces')
        return r.json()

    def get_node(self, node):
        if not self.check_capability('node-get'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'showing a single node, try "node list" instead.')
        r = self._request_url('GET', '/nodes/' + node)
        return r.json()

    def get_nodes(self):
        r = self._request_url('GET', '/nodes')
        return r.json()

    def delete_node(self, node):
        r = self._request_url('DELETE', '/nodes/' + node)
        return r.json()

    def get_interface(self, interface_uuid):
        r = self._request_url('GET', '/interfaces/' + interface_uuid)
        return r.json()

    def float_interface(self, interface_uuid):
        r = self._request_url('POST', '/interfaces/' + interface_uuid +
                              '/float')
        return r.json()

    def defloat_interface(self, interface_uuid):
        r = self._request_url('POST', '/interfaces/' + interface_uuid +
                              '/defloat')
        return r.json()

    def get_console_data(self, instance_ref, length=None, decode='utf-8'):
        url = '/instances/' + instance_ref + '/consoledata'
        if length:
            d = {'length': length}
        else:
            d = {}
        r = self._request_url('GET', url, data=d)

        out = r.text
        if decode:
            try:
                out = out.decode(decode)
            except Exception:
                pass
        return out

    def delete_console_data(self, instance_ref):
        url = '/instances/' + instance_ref + '/consoledata'
        self._request_url('DELETE', url)

    def get_vdi_console_helper(self, instance_ref):
        r = self._request_url('GET', '/instances/' + instance_ref + '/vdiconsolehelper')
        return r.text

    def put_instance_blob(self, instance_ref, blob_uuid, path, mode):
        r = self._request_url('POST', '/instances/' + instance_ref + '/agent/put',
                              data={'blob_uuid': blob_uuid, 'path': path, 'mode': mode})
        return r.json()

    def get_namespaces(self):
        r = self._request_url('GET', '/auth/namespaces')
        return r.json()

    def get_namespace(self, namespace):
        r = self._request_url('GET', '/auth/namespaces/' + namespace)
        return r.json()

    def create_namespace(self, namespace):
        r = self._request_url('POST', '/auth/namespaces',
                              data={'namespace': namespace})
        return r.json()

    def delete_namespace(self, namespace):
        if not namespace:
            namespace = self.namespace
        self._request_url('DELETE', '/auth/namespaces/' + namespace)

    def get_namespace_keynames(self, namespace):
        r = self._request_url('GET', '/auth/namespaces/' + namespace + '/keys')
        return r.json()

    def add_namespace_key(self, namespace, key_name, key):
        r = self._request_url('POST', '/auth/namespaces/' + namespace + '/keys',
                              data={'key_name': key_name, 'key': key})
        return r.json()

    def update_namespace_key(self, namespace, key_name, key):
        r = self._request_url('PUT', '/auth/namespaces/' + namespace + '/keys',
                              data={'key_name': key_name, 'key': key})
        return r.json()

    def delete_namespace_key(self, namespace, key_name):
        self._request_url(
            'DELETE', '/auth/namespaces/' + namespace + '/keys/' + key_name)

    def add_namespace_trust(self, namespace, trusted_namespace):
        r = self._request_url('POST', '/auth/namespaces/' + namespace + '/trust',
                              data={'external_namespace': trusted_namespace})
        return r.json()

    def remove_namespace_trust(self, namespace, trusted_namespace):
        r = self._request_url(
            'DELETE', '/auth/namespaces/' + namespace + '/trust/' + trusted_namespace)
        return r.json()

    def get_existing_locks(self):
        r = self._request_url('GET', '/admin/locks')
        return r.json()

    def ping(self, network_ref, address):
        r = self._request_url(
            'GET', '/networks/' + network_ref + '/ping/' + address)
        return r.json()

    def create_upload(self):
        r = self._request_url('POST', '/upload')
        return r.json()

    def send_upload(self, upload_uuid, data):
        r = self._request_url('POST', '/upload/' + upload_uuid,
                              data=data, request_body_is_binary=True)
        return r.json()

    def send_upload_file(self, upload_uuid, flo):
        buffer_size = 4096
        total = 0

        d = flo.read(buffer_size)
        while d:
            start_time = time.time()
            try:
                self.send_upload(upload_uuid, d)
                retries = 0
            except APIException as e:
                retries += 1

                if retries > 5:
                    raise e

                self.truncate_upload(upload_uuid, total)
                flo.seek(total)
                buffer_size = 4096
                d = flo.read(buffer_size)
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

            d = flo.read(buffer_size)

    def truncate_upload(self, upload_uuid, offset):
        r = self._request_url(
            'POST', '/upload/' + upload_uuid + '/truncate/' + str(offset))
        return r.json()

    def get_agent_operation(self, operation_uuid):
        r = self._request_url('GET', '/agentoperation/' + operation_uuid)
        return r.json()

    def delete_agent_operation(self, operation_uuid):
        self._request_url('DELETE', '/agentoperation/' + operation_uuid)


def get_user_agent():
    sf_version = VersionInfo('shakenfist_client').version_string()
    return 'Mozilla/5.0 (Ubuntu; Linux x86_64) Shaken Fist/%s' % sf_version
