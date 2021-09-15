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


class APIException(Exception):
    def __init__(self, message, method, url, status_code, text):
        self.message = message
        self.method = method
        self.url = url
        self.status_code = status_code
        self.text = text


class RequestMalformedException(APIException):
    pass


class UnauthorizedException(APIException):
    pass


class ResourceCannotBeDeletedException(APIException):
    pass


class ResourceNotFoundException(APIException):
    pass


class DependenciesNotReadyException(APIException):
    pass


class ResourceInUseException(APIException):
    pass


class InternalServerError(APIException):
    pass


class InsufficientResourcesException(APIException):
    pass


class UnknownAsyncStrategy(APIException):
    pass


STATUS_CODES_TO_ERRORS = {
    400: RequestMalformedException,
    401: UnauthorizedException,
    403: ResourceCannotBeDeletedException,
    404: ResourceNotFoundException,
    406: DependenciesNotReadyException,
    409: ResourceInUseException,
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

        if not suppress_configuration_lookup:
            # Where do we find authentication details? First off, we try command line
            # flags; then environment variables (thanks for doing this for free click);
            # ~/.shakenfist (which is a JSON file); and finally /etc/sf/shakenfist.json.
            if not base_url:
                user_conf = os.path.expanduser('~/.shakenfist')
                if os.path.exists(user_conf):
                    with open(user_conf) as f:
                        d = json.loads(f.read())
                        if not namespace:
                            namespace = d['namespace']
                        if not key:
                            key = d['key']
                        if not base_url:
                            base_url = d['apiurl']

            if not base_url:
                try:
                    if os.path.exists('/etc/sf/shakenfist.json'):
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

    def _actual_request_url(self, method, url, data=None, allow_redirects=True):
        url = self.base_url + url

        h = {'Authorization': self.cached_auth,
             'User-Agent': get_user_agent()}
        if data:
            h['Content-Type'] = 'application/json'

        start_time = time.time()
        r = requests.request(method, url, data=json.dumps(data), headers=h,
                             allow_redirects=allow_redirects)
        end_time = time.time()

        LOG.debug('-------------------------------------------------------')
        LOG.debug('API client requested: %s %s' % (method, url))
        if data:
            LOG.debug('Data:\n    %s'
                      % ('\n    '.join(json.dumps(data,
                                                  indent=4,
                                                  sort_keys=True).split('\n'))))
        for h in r.history:
            LOG.debug('URL request history: %s --> %s %s'
                      % (h.url, h.status_code, h.headers.get('Location')))
        LOG.debug('API client response: code = %s (took %.02f seconds)'
                  % (r.status_code, (end_time - start_time)))

        if r.text:
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
            raise UnauthorizedException('API unauthorized', 'POST', auth_url,
                                        r.status_code, r.text)
        return 'Bearer %s' % r.json()['access_token']

    def _request_url(self, method, url, data=None):
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
                    return self._actual_request_url(method, url, data=data)
                except UnauthorizedException:
                    self.cached_auth = self._authenticate()
                    return self._actual_request_url(method, url, data=data)

            except DependenciesNotReadyException as e:
                # The API server will return a 406 exception when we have
                # specified an operation which depends on a resource and
                # that resource is not in the created state.
                if time.time() > deadline:
                    LOG.debug('Deadline exceeded waiting for dependancies')
                    raise e

                LOG.debug('Dependencies not ready, retrying')
                time.sleep(1)

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

    def get_instance(self, instance_uuid):
        r = self._request_url('GET', '/instances/' + instance_uuid)
        return r.json()

    def get_instance_interfaces(self, instance_uuid):
        r = self._request_url('GET', '/instances/' + instance_uuid +
                              '/interfaces')
        return r.json()

    def get_instance_metadata(self, instance_uuid):
        r = self._request_url('GET', '/instances/' + instance_uuid +
                              '/metadata')
        return r.json()

    def set_instance_metadata_item(self, instance_uuid, key, value):
        r = self._request_url('PUT', '/instances/' + instance_uuid +
                              '/metadata/' + key, data={'value': value})
        return r.json()

    def delete_instance_metadata_item(self, instance_uuid, key):
        r = self._request_url('DELETE', '/instances/' + instance_uuid +
                              '/metadata/' + key)
        return r.json()

    def create_instance(self, name, cpus, memory, network, disk, sshkey, userdata,
                        namespace=None, force_placement=None, video=None, uefi=False):
        body = {
            'name': name,
            'cpus': cpus,
            'memory': memory,
            'network': network,
            'ssh_key': sshkey,
            'user_data': userdata,
            'namespace': namespace,
            'video': video,
            'uefi': uefi
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

    def snapshot_instance(self, instance_uuid, all=False, label_name=None):
        r = self._request_url(
            'POST', '/instances/' + instance_uuid + '/snapshot', data={'all': all})
        out = r.json()

        waiting_for = []
        for s in out:
            waiting_for.append(out[s]['blob_uuid'])

        deadline = time.time() + _calculate_async_deadline(self.async_strategy)
        while waiting_for:
            LOG.debug('Waiting for snapshots: %s' % ', '.join(waiting_for))
            if time.time() > deadline:
                LOG.debug('Deadline exceeded waiting for snapshots')
                break

            time.sleep(1)
            snaps = self.get_instance_snapshots(instance_uuid)
            for s in snaps:
                if s.get('blob_uuid') in waiting_for:
                    LOG.debug('Blob %s now present' % s['blob_uuid'])
                    waiting_for.remove(s['blob_uuid'])

        if not all and label_name:
            # It only makes sense to update a label if we've snapshotted a single
            # disk. Otherwise we'd immediately clobber the label with the last
            # disk in the snapshot series.
            out['label'] = self.update_label(
                label_name, out['vda']['blob_uuid'])

        return out

    def get_instance_snapshots(self, instance_uuid):
        r = self._request_url('GET', '/instances/' + instance_uuid +
                              '/snapshot')
        return r.json()

    def update_label(self, label_name, blob_uuid):
        r = self._request_url(
            'POST', '/label/%s' % label_name, data={'blob_uuid': blob_uuid})
        return r.json()

    def reboot_instance(self, instance_uuid, hard=False):
        style = 'soft'
        if hard:
            style = 'hard'
        r = self._request_url('POST', '/instances/' + instance_uuid +
                              '/reboot' + style)
        return r.json()

    def power_off_instance(self, instance_uuid):
        r = self._request_url('POST', '/instances/' + instance_uuid +
                              '/poweroff')
        return r.json()

    def power_on_instance(self, instance_uuid):
        r = self._request_url('POST', '/instances/' + instance_uuid +
                              '/poweron')
        return r.json()

    def pause_instance(self, instance_uuid):
        r = self._request_url('POST', '/instances/' + instance_uuid +
                              '/pause')
        return r.json()

    def unpause_instance(self, instance_uuid):
        r = self._request_url('POST', '/instances/' + instance_uuid +
                              '/unpause')
        return r.json()

    def delete_instance(self, instance_uuid, namespace=None, async_request=False):
        # Why pass a namespace when you're passing an exact UUID? The idea here
        # is that it provides a consistent interface, but also a safety check
        # against overly zealous loops deleting things.
        data = None
        if namespace:
            data = {'namespace': namespace}
        self._request_url('DELETE', '/instances/' + instance_uuid, data=data)

        if async_request:
            return

        i = self.get_instance(instance_uuid)
        deadline = time.time() + _calculate_async_deadline(self.async_strategy)
        while True:
            if i['state'] == 'deleted':
                return

            LOG.debug('Waiting for instance to be deleted')
            if time.time() > deadline:
                LOG.debug('Deadline exceeded waiting for instance to delete')
                return

            time.sleep(1)
            i = self.get_instance(instance_uuid)

    def get_instance_events(self, instance_uuid):
        r = self._request_url('GET', '/instances/' + instance_uuid + '/events')
        return r.json()

    def cache_artifact(self, image_url):
        r = self._request_url('POST', '/artifacts', data={'url': image_url})
        return r.json()

    def get_artifact(self, artifact_uuid):
        r = self._request_url('GET', '/artifacts/' + artifact_uuid)
        return r.json()

    def get_artifacts(self, node=None):
        r = self._request_url('GET', '/artifacts', data={'node': node})
        return r.json()

    def get_artifact_events(self, artifact_uuid):
        r = self._request_url('GET', '/artifacts/events/' + artifact_uuid)
        return r.json()

    def get_networks(self, all=False):
        r = self._request_url('GET', '/networks', data={'all': all})
        return r.json()

    def get_network(self, network_uuid):
        r = self._request_url('GET', '/networks/' + network_uuid)
        return r.json()

    def delete_network(self, network_uuid):
        r = self._request_url('DELETE', '/networks/' + network_uuid)
        return r.json()

    def delete_all_networks(self, namespace, clean_wait=False):
        r = self._request_url('DELETE', '/networks',
                              data={'confirm': True,
                                    'namespace': namespace,
                                    'clean_wait': clean_wait,
                                    })
        return r.json()

    def get_network_events(self, instance_uuid):
        r = self._request_url('GET', '/networks/' + instance_uuid + '/events')
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

    def get_network_interfaces(self, network_uuid):
        r = self._request_url('GET', '/networks/' +
                              network_uuid + '/interfaces')
        return r.json()

    def get_network_metadata(self, network_uuid):
        r = self._request_url('GET', '/networks/' + network_uuid +
                              '/metadata')
        return r.json()

    def set_network_metadata_item(self, network_uuid, key, value):
        r = self._request_url('PUT', '/networks/' + network_uuid +
                              '/metadata/' + key, data={'value': value})
        return r.json()

    def delete_network_metadata_item(self, network_uuid, key):
        r = self._request_url('DELETE', '/networks/' + network_uuid +
                              '/metadata/' + key)
        return r.json()

    def get_nodes(self):
        r = self._request_url('GET', '/nodes')
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

    def get_console_data(self, instance_uuid, length=None):
        url = '/instances/' + instance_uuid + '/consoledata'
        if length:
            d = {'length': length}
        else:
            d = {}
        r = self._request_url('GET', url, data=d)
        return r.text

    def get_namespaces(self):
        r = self._request_url('GET', '/auth/namespaces')
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

    def delete_namespace_key(self, namespace, key_name):
        self._request_url(
            'DELETE', '/auth/namespaces/' + namespace + '/keys/' + key_name)

    def get_namespace_metadata(self, namespace):
        r = self._request_url('GET', '/auth/namespaces/' + namespace +
                              '/metadata')
        return r.json()

    def set_namespace_metadata_item(self, namespace, key, value):
        r = self._request_url('PUT', '/auth/namespaces/' + namespace +
                              '/metadata/' + key, data={'value': value})
        return r.json()

    def delete_namespace_metadata_item(self, namespace, key):
        r = self._request_url(
            'DELETE', '/auth/namespaces/' + namespace + '/metadata/' + key)
        return r.json()

    def get_existing_locks(self):
        r = self._request_url('GET', '/admin/locks')
        return r.json()

    def ping(self, network_uuid, address):
        r = self._request_url('GET', '/networks/' +
                              network_uuid + '/ping/' + address)
        return r.json()


def get_user_agent():
    sf_version = VersionInfo('shakenfist_client').version_string()
    return 'Mozilla/5.0 (Ubuntu; Linux x86_64) Shaken Fist/%s' % sf_version
