import errno
import json
import logging
import os
from pbr.version import VersionInfo
import requests
import time
import sys


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


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


class Client(object):
    def __init__(self, base_url='http://localhost:13000', verbose=False,
                 namespace=None, key=None, sync_request_timeout=300,
                 suppress_configuration_lookup=False):
        self.sync_request_timeout = sync_request_timeout

        if not suppress_configuration_lookup:
            # Where do we find authentication details? First off, we try command line
            # flags; then environment variables (thanks for doing this for free click);
            # and finally ~/.shakenfist (which is a JSON file).
            if not namespace:
                user_conf = os.path.expanduser('~/.shakenfist')
                if os.path.exists(user_conf):
                    with open(user_conf) as f:
                        d = json.loads(f.read())
                        namespace = d['namespace']
                        key = d['key']
                        base_url = d['apiurl']

            if not namespace:
                try:
                    if os.path.exists('/etc/sf/shakenfist.json'):
                        with open('/etc/sf/shakenfist.json') as f:
                            d = json.loads(f.read())
                            namespace = d['namespace']
                            key = d['key']
                            base_url = d['apiurl']
                except IOError as e:
                    if e.errno != errno.EACCES:
                        raise

        self.base_url = base_url
        self.namespace = namespace
        self.key = key

        self.cached_auth = None
        if verbose:
            LOG.setLevel(logging.DEBUG)

    def _actual_request_url(self, method, url, data=None):
        h = {'Authorization': self.cached_auth,
             'User-Agent': get_user_agent()}
        if data:
            h['Content-Type'] = 'application/json'
        r = requests.request(method, url, data=json.dumps(data), headers=h)

        LOG.debug('-------------------------------------------------------')
        LOG.debug('API client requested: %s %s' % (method, url))
        if data:
            LOG.debug('Data:\n    %s'
                      % ('\n    '.join(json.dumps(data,
                                                  indent=4,
                                                  sort_keys=True).split('\n'))))
        LOG.debug('API client response: code = %s' % r.status_code)
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

        if r.status_code != 200:
            raise APIException(
                'API request failed', method, url, r.status_code, r.text)
        return r

    def _authenticate(self):
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
        if not self.cached_auth:
            self.cached_auth = self._authenticate()

        start_time = time.time()
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
                # that resource is not in the created state. We retry
                # for a while before we give up.
                if time.time() - start_time > 60:
                    raise e
                time.sleep(1)

    def get_instances(self, all=False):
        r = self._request_url('GET', self.base_url +
                              '/instances', data={'all': all})
        return r.json()

    def delete_all_instances(self, namespace):
        r = self._request_url('DELETE', self.base_url + '/instances',
                              data={'confirm': True,
                                    'namespace': namespace})
        return r.json()

    def get_instance(self, instance_uuid):
        r = self._request_url('GET', self.base_url +
                              '/instances/' + instance_uuid)
        return r.json()

    def get_instance_interfaces(self, instance_uuid):
        r = self._request_url('GET', self.base_url + '/instances/' + instance_uuid +
                              '/interfaces')
        return r.json()

    def get_instance_metadata(self, instance_uuid):
        r = self._request_url('GET', self.base_url + '/instances/' + instance_uuid +
                              '/metadata')
        return r.json()

    def set_instance_metadata_item(self, instance_uuid, key, value):
        r = self._request_url('PUT', self.base_url + '/instances/' + instance_uuid +
                              '/metadata/' + key, data={'value': value})
        return r.json()

    def delete_instance_metadata_item(self, instance_uuid, key):
        r = self._request_url('DELETE', self.base_url + '/instances/' + instance_uuid +
                              '/metadata/' + key)
        return r.json()

    def create_instance(self, name, cpus, memory, network, disk, sshkey, userdata,
                        namespace=None, force_placement=None, video=None, async_request=False):
        body = {
            'name': name,
            'cpus': cpus,
            'memory': memory,
            'network': network,
            'ssh_key': sshkey,
            'user_data': userdata,
            'namespace': namespace,
            'video': video
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

        r = self._request_url('POST', self.base_url + '/instances',
                              data=body)
        i = r.json()
        if not async_request:
            start_time = time.time()
            rounds = 1
            while (time.time() - start_time < self.sync_request_timeout and
                   i.get('state') in ['initial', 'creating']):
                time.sleep(min(rounds, 10))
                rounds += 1
                i = self.get_instance(i['uuid'])
        return i

    def snapshot_instance(self, instance_uuid, all=False):
        r = self._request_url('POST', self.base_url + '/instances/' + instance_uuid +
                              '/snapshot', data={'all': all})
        return r.json()

    def get_instance_snapshots(self, instance_uuid):
        r = self._request_url('GET', self.base_url + '/instances/' + instance_uuid +
                              '/snapshot')
        return r.json()

    def reboot_instance(self, instance_uuid, hard=False):
        style = 'soft'
        if hard:
            style = 'hard'
        r = self._request_url('POST', self.base_url + '/instances/' + instance_uuid +
                              '/reboot' + style)
        return r.json()

    def power_off_instance(self, instance_uuid):
        r = self._request_url('POST', self.base_url + '/instances/' + instance_uuid +
                              '/poweroff')
        return r.json()

    def power_on_instance(self, instance_uuid):
        r = self._request_url('POST', self.base_url + '/instances/' + instance_uuid +
                              '/poweron')
        return r.json()

    def pause_instance(self, instance_uuid):
        r = self._request_url('POST', self.base_url + '/instances/' + instance_uuid +
                              '/pause')
        return r.json()

    def unpause_instance(self, instance_uuid):
        r = self._request_url('POST', self.base_url + '/instances/' + instance_uuid +
                              '/unpause')
        return r.json()

    def delete_instance(self, instance_uuid, async_request=False):
        self._request_url('DELETE', self.base_url +
                          '/instances/' + instance_uuid)
        if async_request:
            return

        i = self.get_instance(instance_uuid)
        start_time = time.time()
        rounds = 1
        while (time.time() - start_time < self.sync_request_timeout and
               i and i.get('state') not in ['deleted', 'error']):
            time.sleep(min(rounds, 10))
            rounds += 1
            i = self.get_instance(instance_uuid)
        return

    def get_instance_events(self, instance_uuid):
        r = self._request_url('GET', self.base_url +
                              '/instances/' + instance_uuid + '/events')
        return r.json()

    def cache_image(self, image_url):
        r = self._request_url('POST', self.base_url + '/images',
                              data={'url': image_url})
        return r.json()

    def get_images(self, node=None):
        r = self._request_url('GET', self.base_url + '/images',
                              data={'node': node})
        return r.json()

    def get_image_meta(self, node=None):
        sys.stderr.write('get_image_meta() is deprecated and will be removed in '
                         'Shaken Fist 0.5. Please convert to get_images().')
        sys.stderr.flush()
        return self.get_images(node=node)

    def get_image_events(self, image_url):
        r = self._request_url('GET', self.base_url + '/images/events',
                              data={'url': image_url})
        return r.json()

    def get_networks(self, all=False):
        r = self._request_url('GET', self.base_url +
                              '/networks', data={'all': all})
        return r.json()

    def get_network(self, network_uuid):
        r = self._request_url('GET', self.base_url +
                              '/networks/' + network_uuid)
        return r.json()

    def delete_network(self, network_uuid):
        r = self._request_url('DELETE', self.base_url +
                              '/networks/' + network_uuid)
        return r.json()

    def delete_all_networks(self, namespace):
        r = self._request_url('DELETE', self.base_url + '/networks',
                              data={'confirm': True,
                                    'namespace': namespace})
        return r.json()

    def get_network_events(self, instance_uuid):
        r = self._request_url('GET', self.base_url +
                              '/networks/' + instance_uuid + '/events')
        return r.json()

    def allocate_network(self, netblock, provide_dhcp, provide_nat, name, namespace=None):
        r = self._request_url('POST', self.base_url + '/networks',
                              data={
                                  'netblock': netblock,
                                  'provide_dhcp': provide_dhcp,
                                  'provide_nat': provide_nat,
                                  'name': name,
                                  'namespace': namespace
                              })
        return r.json()

    def get_network_interfaces(self, network_uuid):
        r = self._request_url('GET', self.base_url + '/networks/'
                              + network_uuid + '/interfaces')
        return r.json()

    def get_network_metadata(self, network_uuid):
        r = self._request_url('GET', self.base_url + '/networks/' + network_uuid +
                              '/metadata')
        return r.json()

    def set_network_metadata_item(self, network_uuid, key, value):
        r = self._request_url('PUT', self.base_url + '/networks/' + network_uuid +
                              '/metadata/' + key, data={'value': value})
        return r.json()

    def delete_network_metadata_item(self, network_uuid, key):
        r = self._request_url('DELETE', self.base_url + '/networks/' + network_uuid +
                              '/metadata/' + key)
        return r.json()

    def get_nodes(self):
        r = self._request_url('GET', self.base_url + '/nodes')
        return r.json()

    def get_interface(self, interface_uuid):
        r = self._request_url('GET', self.base_url +
                              '/interfaces/' + interface_uuid)
        return r.json()

    def float_interface(self, interface_uuid):
        r = self._request_url('POST', self.base_url + '/interfaces/' + interface_uuid +
                              '/float')
        return r.json()

    def defloat_interface(self, interface_uuid):
        r = self._request_url('POST', self.base_url + '/interfaces/' + interface_uuid +
                              '/defloat')
        return r.json()

    def get_console_data(self, instance_uuid, length=None):
        url = self.base_url + '/instances/' + instance_uuid + '/consoledata'
        if length:
            d = {'length': length}
        else:
            d = {}
        r = self._request_url('GET', url, data=d)
        return r.text

    def get_namespaces(self):
        r = self._request_url('GET', self.base_url + '/auth/namespaces')
        return r.json()

    def create_namespace(self, namespace):
        r = self._request_url('POST', self.base_url + '/auth/namespaces',
                              data={'namespace': namespace})
        return r.json()

    def delete_namespace(self, namespace):
        if not namespace:
            namespace = self.namespace
        self._request_url(
            'DELETE', self.base_url + '/auth/namespaces/' + namespace)

    def get_namespace_keynames(self, namespace):
        r = self._request_url('GET', self.base_url + '/auth/namespaces/' +
                              namespace + '/keys')
        return r.json()

    def add_namespace_key(self, namespace, key_name, key):
        r = self._request_url('POST', self.base_url + '/auth/namespaces/' +
                              namespace + '/keys',
                              data={'key_name': key_name, 'key': key})
        return r.json()

    def delete_namespace_key(self, namespace, key_name):
        self._request_url(
            'DELETE', self.base_url + '/auth/namespaces/' + namespace + '/keys/' + key_name)

    def get_namespace_metadata(self, namespace):
        r = self._request_url('GET', self.base_url + '/auth/namespaces/' + namespace +
                              '/metadata')
        return r.json()

    def set_namespace_metadata_item(self, namespace, key, value):
        r = self._request_url('PUT', self.base_url + '/auth/namespaces/' + namespace +
                              '/metadata/' + key, data={'value': value})
        return r.json()

    def delete_namespace_metadata_item(self, namespace, key):
        r = self._request_url('DELETE', self.base_url + '/auth/namespaces/' + namespace +
                              '/metadata/' + key)
        return r.json()

    def get_existing_locks(self):
        r = self._request_url('GET', self.base_url + '/admin/locks')
        return r.json()

    def ping(self, network_uuid, address):
        r = self._request_url('GET', self.base_url +
                              '/networks/' + network_uuid + '/ping/' + address)
        return r.json()


def get_user_agent():
    sf_version = VersionInfo('shakenfist').version_string()
    return 'Mozilla/5.0 (Ubuntu; Linux x86_64) Shaken Fist/%s' % sf_version
