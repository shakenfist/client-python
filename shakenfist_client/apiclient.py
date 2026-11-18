import copy
import errno
import json
import logging
import os
import re
import time
# The class rather than the module, because a loop variable elsewhere in
# this file is named uuid and would shadow a module imported as such.
from uuid import UUID

import requests


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)


# A JWT in compact serialisation: three dot separated base64url
# segments, the first of which always begins "eyJ" because a JWT header
# is the base64url encoding of a JSON object and so always starts '{"'.
# Anchoring on that rather than on "three dotted runs of base64url" is
# what keeps hostnames like host.example.internal out of the match.
JWT_RE = re.compile(r'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*')


def redact_tokens(text):
    """Replace any JWT in text with a placeholder.

    A Kerbside VDI console URL carries its own capability as a
    short-lived, single-use JWT, so the URL is a credential and
    anything that renders it is a way for that credential to escape.
    This module redacts the two it creates itself -- the exceptions
    raised out of get_vdi_console_proxy_file() -- and sf-client
    installs it as a logging filter for the ones libraries create,
    such as urllib3 printing the request target of every connection.
    """
    return JWT_RE.sub('*****', text)


# Async strategies
ASYNC_CONTINUE = 'continue'
ASYNC_PAUSE = 'pause'
ASYNC_BLOCK = 'block'


class UnconfiguredException(Exception):
    ...


class IncapableException(Exception):
    ...


class InvalidException(Exception):
    ...


class InstanceWillNeverBeReady(Exception):
    ...


class TimeoutException(Exception):
    ...


class APIException(Exception):
    def __init__(self, message, method, url, status_code, text, headers=None):
        self.message = message
        self.method = method
        self.url = url
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


class RequestMalformedException(APIException):
    ...


class UnauthenticatedException(APIException):
    ...


class UnauthorizedException(APIException):
    ...


class ResourceNotFoundException(APIException):
    ...


class DependenciesNotReadyException(APIException):
    ...


class ResourceStateConflictException(APIException):
    ...


class InternalServerError(APIException):
    ...


class InsufficientResourcesException(APIException):
    ...


class ServiceUnavailableException(APIException):
    # The request was well formed and the server understood it, but could not
    # answer it just now. The correct behaviour for a caller is to retry --
    # this client does not retry automatically, only _request_url's handling
    # of 406 does that -- which is why this is worth telling apart from a
    # durable refusal: the namespace claims API answers 503 both while the
    # cluster capacity accounting is still being built and when a claim row
    # kept moving under a concurrent writer until the optimistic retry budget
    # ran out.
    ...


class UnknownAsyncStrategy(APIException):
    ...


class AgentAwaitTimeout(Exception):
    ...


class AgentCommandError(Exception):
    ...


class AgentOperationFailed(Exception):
    """Raised when a polled agent operation ends in a non-complete terminal state.

    Carries the operation uuid and the operation's last known view so
    callers can inspect ``state`` and any results the server attached
    before the operation gave up.
    """

    def __init__(self, message, op_uuid, op_view=None):
        super().__init__(message)
        self.op_uuid = op_uuid
        self.op_view = op_view or {}


class ClusterOperationFailed(Exception):
    """Raised when a polled cluster operation ends in a terminal failure.

    Carries the op handle and the operation's final external_view so
    callers can inspect ``state``, ``tasks``, and any error payload the
    server attached to the op record.
    """

    def __init__(self, message, op_type, op_uuid, op_view=None):
        super().__init__(message)
        self.op_type = op_type
        self.op_uuid = op_uuid
        self.op_view = op_view or {}


class ClusterOperationTimeout(Exception):
    """Raised when polling a cluster operation exceeds the timeout."""

    def __init__(self, message, op_type, op_uuid, op_view=None):
        super().__init__(message)
        self.op_type = op_type
        self.op_uuid = op_uuid
        self.op_view = op_view or {}


STATUS_CODES_TO_ERRORS = {
    400: RequestMalformedException,
    401: UnauthenticatedException,
    403: UnauthorizedException,
    404: ResourceNotFoundException,
    406: DependenciesNotReadyException,
    409: ResourceStateConflictException,
    500: InternalServerError,
    503: ServiceUnavailableException,
    507: InsufficientResourcesException,
}


# How long to wait before retrying a 507 which the server marked as
# transient but which carried no usable Retry-After header. The server
# sends a fixed 15 (see shakenfist's
# docs/plans/PLAN-transient-capacity-refusals-phase-04-retry-after.md,
# decision D31), so this only matters when a proxy has stripped the
# header.
TRANSIENT_RETRY_DEFAULT = 15

# A Retry-After we are willing to honour is clamped into this range. The
# lower bound stops a "Retry-After: 0" from turning the retry into a busy
# loop against a server which is already out of capacity. The upper bound
# means a single sleep can never exceed the entire 60 second budget
# _calculate_async_deadline() gives ASYNC_PAUSE, so a hostile or broken
# header cannot park a caller for longer than it asked to wait in total.
TRANSIENT_RETRY_MINIMUM = 1
TRANSIENT_RETRY_MAXIMUM = 60

# How many times a single call will send the same refused request. A
# deadline alone is not a bound a caller can reason about: ASYNC_BLOCK's
# is an hour, which at the server's fixed 15 seconds is around 240
# replays, and the server creates an instance object and allocates its
# addresses before the scheduler runs, so each of those replays leaves a
# record behind for the cluster to reap. Five is what a 60 second
# ASYNC_PAUSE budget could already spend -- four waits and a final
# attempt -- so no waiting strategy loses an attempt it used to have,
# and the story this retry exists for (a neighbouring instance is being
# torn down right now; phase 3 measured capacity returning 5.3-5.6
# seconds after a domain is destroyed) does not need more.
TRANSIENT_RETRY_MAXIMUM_ATTEMPTS = 5


def _calculate_async_deadline(strategy):
    if strategy == ASYNC_CONTINUE:
        return -1
    if strategy == ASYNC_PAUSE:
        return 60
    if strategy == ASYNC_BLOCK:
        return 3600
    raise UnknownAsyncStrategy('Async strategy %s is unknown' % strategy)


def _is_error_state(state):
    # Instances error to transitional states like 'creating-error' before
    # settling in the terminal 'error' state. Both mean the instance will
    # never be ready.
    return state == 'error' or state.endswith('-error')


# These five are exactly AgentOperation.TERMINAL_STATES in the server
# repository (shakenfist/operations/agentoperation.py). The client cannot
# import that module, so this duplicates it deliberately -- keep the two
# in sync by hand.
TERMINAL_AGENT_OPERATION_STATES = frozenset({'complete', 'error', 'expired', 'abort', 'deleted'})


# The only keys an agent operation dependency edge may carry. This mirrors
# the server's edge model, which forbids an unknown key on the parameter it
# publishes rather than dropping it.
AGENT_DEPENDENCY_KEYS = frozenset({'op_uuid', 'settle_seconds'})


# The states a dependency can be observed in which mean it had not
# finished yet. These matter to a chain walk because the cascade records
# one of them when a dependent was given up on rather than when a step
# failed: a dependent waits only so long for a dependency to go terminal,
# and when that budget is spent it is aborted with whatever state the
# dependency was still in. The server's own copy, with the reasoning, is
# DEPENDENCY_PENDING_STATES in shakenfist/daemons/sidechannel/main.py.
AGENT_DEPENDENCY_PENDING_STATES = frozenset({'initial', 'preflight', 'queued', 'executing'})

# The two sentinels the dispatcher records in place of a dependency's
# state, neither of which is a state any operation can hold. They say
# opposite things, which is why the server keeps them apart and why a
# chain walk has to as well: missing means the dependency was looked for
# and is not there, while unreadable means nothing at all was learned
# about it.
AGENT_DEPENDENCY_MISSING = 'missing'
AGENT_DEPENDENCY_UNREADABLE = 'unreadable'


def _canonical_op_uuid(where, value):
    # The canonical form of the UUID, not whatever the caller typed. The
    # server parses an edge's op_uuid and hands it back in canonical lower
    # case, and a creating call compares the edges it sent against the ones
    # the response carries -- so an upper case UUID sent through as typed
    # would come back looking like an edge the cluster had dropped.
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError):
        raise ValueError(f'{where}: {value!r} is not a valid UUID') from None


def agent_dependency(operation, settle_seconds=None):
    """Build a dependency edge which names an agent operation.

    operation is either an agent operation as the API returns it (a
    dictionary with a 'uuid') or an operation UUID as a string, so that a
    caller chaining operations writes
    depends_on=[agent_dependency(first_op)] instead of assembling
    depends_on=[{'op_uuid': first_op['uuid']}] by hand.

    settle_seconds delays the edge being satisfied for that long after the
    named operation finished, for a caller whose next step needs the guest
    to settle after whatever the dependency changed.

    A caller who names something which is not an agent operation UUID is
    refused here, rather than by the cluster a request later.
    """
    if isinstance(operation, dict):
        op_uuid = operation.get('uuid')
        if op_uuid is None:
            raise ValueError(
                'agent_dependency(): an edge must name an agent operation, and this one has no uuid')
    else:
        op_uuid = operation

    # op_type is deliberately not emitted. The server stores it as a
    # constant it does not publish, on the grounds that a caller forced to
    # supply a constant can only get it wrong, and the parameter it
    # validates against refuses any key it does not publish -- so an edge
    # carrying op_type would be rejected rather than tolerated.
    edge = {'op_uuid': _canonical_op_uuid('agent_dependency()', op_uuid)}

    # Only sent when the caller asked for a settle. An absent key is what
    # means "satisfied as soon as the named operation finishes", and that
    # is the server's decision to make, not a value to send on the caller's
    # behalf.
    if settle_seconds is not None:
        edge['settle_seconds'] = settle_seconds

    return edge


def _agentop_dependency_edges(name, edges):
    """Normalise a list of agent operation dependency edges.

    Each member is either an agent operation UUID as a string or an edge
    dictionary of the shape agent_dependency() builds. The returned list
    holds an edge dictionary per member, in the order they were given.

    name is the parameter the list arrived in ('depends_on' or
    'runs_after'), and every refusal names it and the index of the member
    at fault, because a caller submitting a graph needs to know which edge
    the complaint is about.

    What is checked here is shape, and only shape. How many edges an
    operation may have, and how long a settle may be, are deployment
    policy which the server reads from its own configuration -- the settle
    ceiling moved from 86400 to 300 seconds in a single release -- so a
    copy of either bound in this client would refuse a value the cluster
    being talked to accepts, or tell a caller a value is fine which that
    cluster then refuses. The server's own edge model declines to enforce
    the settle ceiling for the same reason, and checks it where the
    configuration can be read. Nothing is missing here; both bounds belong
    to the server alone.
    """
    normalised = []

    for index, edge in enumerate(edges):
        if isinstance(edge, str):
            edge = {'op_uuid': edge}

        elif isinstance(edge, dict):
            unknown = set(edge) - AGENT_DEPENDENCY_KEYS
            if unknown:
                raise ValueError(
                    f'{name}[{index}] has keys which are not part of a dependency edge: '
                    f'{", ".join(sorted(unknown))}. An edge may only name '
                    f'{", ".join(sorted(AGENT_DEPENDENCY_KEYS))}')

            if edge.get('op_uuid') is None:
                raise ValueError(
                    f'{name}[{index}] does not name an agent operation, a dependency '
                    'edge must have an op_uuid')

            edge = dict(edge)

        else:
            raise ValueError(
                f'{name}[{index}] must be an agent operation UUID or a dependency edge '
                f'dictionary, not a {type(edge).__name__}')

        edge['op_uuid'] = _canonical_op_uuid(f'{name}[{index}] op_uuid', edge['op_uuid'])
        normalised.append(edge)

    return normalised


def _agentop_chain_position(hops):
    # Where in a submitted chain the walk below ended up, phrased for a
    # failure message. Zero hops is the operation the caller awaited, and
    # saying so is worth a branch: "0 operations back" reads like an
    # arithmetic bug in the client rather than a chain whose last link is
    # the one that broke.
    if hops == 0:
        return 'the operation which was awaited'
    if hops == 1:
        return '1 operation back in the chain'
    return f'{hops} operations back in the chain'


def _correct_blob_indexes(d):
    # JSON requires dictionary keys to be strings. Reverse that for the blobs
    # element here to reduce confusion.
    new_blobs = {}
    for k in d.get('blobs', []):
        new_blobs[int(k)] = d['blobs'][k]
    d['blobs'] = new_blobs
    LOG.debug('Rewrote blob keys to ints')
    return d


class Client:
    def __init__(self, base_url=None, verbose=False,
                 namespace=None, key=None, sync_request_timeout=300,
                 suppress_configuration_lookup=False, logger=None,
                 async_strategy=ASYNC_BLOCK, retry_transient_capacity=False):
        global LOG
        if verbose:
            LOG.setLevel(logging.DEBUG)
        if logger:
            LOG = logger

        self.most_recent_request_id = None
        # Passed as the requests timeout on every HTTP call (connect and
        # read). Without it a server that accepts the socket but never
        # responds would block the caller forever.
        self.sync_request_timeout = sync_request_timeout

        LOG.debug('Client initially configured with apiurl of %s for namespace %s '
                  'and async strategy %s'
                  % (base_url, namespace, async_strategy))

        if not suppress_configuration_lookup:
            # Where do we find authentication details? First off, we try command line
            # flags; then environment variables; ~/.shakenfist (which is a JSON file);
            # and finally /etc/sf/shakenfist.json. click reads the environment
            # variables for free in the CLI, but we must also do it here so that
            # other library callers (the ansible collection for example) behave the
            # same way as the CLI.
            if not base_url:
                LOG.debug('Testing environment variables')
                base_url = os.environ.get('SHAKENFIST_API_URL')
                if base_url:
                    LOG.debug('Loading configuration from environment variables')
                    if not namespace:
                        namespace = os.environ.get('SHAKENFIST_NAMESPACE')
                    if not key:
                        key = os.environ.get('SHAKENFIST_KEY')

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
                except OSError as e:
                    if e.errno != errno.EACCES:
                        raise

        if not base_url:
            raise UnconfiguredException(
                'You have not specified the server to communicate with')

        self.base_url = base_url
        self.namespace = namespace
        self.key = key
        self.async_strategy = async_strategy

        # Opt in to waiting out a capacity refusal which the server has
        # marked as transient. Off by default: a 507 is a refusal, and a
        # caller which has not asked to wait should be told immediately
        # rather than silently spending its deadline inside one call. See
        # docs/transient-capacity-retry.md.
        self.retry_transient_capacity = retry_transient_capacity

        LOG.debug('Client configured with apiurl of %s for namespace %s '
                  'and async strategy %s'
                  % (self.base_url, self.namespace, self.async_strategy))

        self.cached_auth = None

        self.session = requests.Session()

        # Request capabilities information
        self._collect_capabilities()

    def _collect_capabilities(self):
        r = self.session.request('GET', self.base_url, allow_redirects=True,
                                 timeout=self.sync_request_timeout)
        self.root_html = r.text

    def check_capability(self, capability_string):
        # NOTE(mikal): this likely needs to be fancier
        return capability_string in self.root_html

    def _actual_request_url(self, method, url, data=None,
                            request_body_is_binary=False,
                            response_body_is_binary=False,
                            allow_redirects=True, stream=False):
        url = self.base_url + url

        h = {
                'Authorization': self.cached_auth,
                'User-Agent': get_user_agent()
            }
        if data:
            if request_body_is_binary:
                h['Content-Type'] = 'application/octet-stream'
            else:
                h['Content-Type'] = 'application/json'
                data = json.dumps(data, indent=4, sort_keys=True)

        start_time = time.time()
        try:
            r = self.session.request(method, url, data=data, headers=h,
                                     allow_redirects=allow_redirects,
                                     stream=stream,
                                     timeout=self.sync_request_timeout)

        except requests.exceptions.ConnectionError:
            # Session was terminated gracelessly, rebuild it
            self.session = requests.Session()
            r = self.session.request(method, url, data=data, headers=h,
                                     allow_redirects=allow_redirects,
                                     stream=stream,
                                     timeout=self.sync_request_timeout)

        end_time = time.time()

        LOG.debug('-------------------------------------------------------')
        LOG.debug(f'API client requested: {method} {url}')
        for hkey in h:
            if hkey == 'Authorization' and h[hkey]:
                LOG.debug('Header: Authorization = Bearer *****')
            else:
                LOG.debug(f'Header: {hkey} = {h[hkey]}')
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

        self.most_recent_request_id = r.headers.get('X-Request-ID')
        for hkey in r.headers:
            LOG.debug(f'Header: {hkey} = {r.headers[hkey]}')

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
                'API request failed', method, url, r.status_code, r.text,
                headers=r.headers)

        acceptable = [200, 202]
        if not allow_redirects:
            acceptable.append(301)
        if r.status_code not in acceptable:
            raise APIException(
                'API request failed', method, url, r.status_code, r.text,
                headers=r.headers)
        return r

    def _authenticate(self):
        LOG.debug('Authentication request made, contents not logged')
        auth_url = self.base_url + '/auth'
        r = requests.request('POST', auth_url,
                             data=json.dumps(
                                 {'namespace': self.namespace,
                                  'key': self.key}),
                             headers={'Content-Type': 'application/json',
                                      'User-Agent': get_user_agent()},
                             timeout=self.sync_request_timeout)
        if r.status_code != 200:
            raise UnauthenticatedException('API unauthenticated', 'POST', auth_url,
                                           r.status_code, r.text, headers=r.headers)
        return 'Bearer %s' % r.json()['access_token']

    def _request_url(self, method, url, data=None, request_body_is_binary=False,
                     response_body_is_binary=False, stream=False,
                     deadline=None):
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

        # A caller with a budget of its own passes a deadline, so the
        # dependency retry below cannot outlast it. Without one this falls
        # back to the async strategy's deadline, which for ASYNC_BLOCK is an
        # hour -- spent inside a single call, invisible to anything above.
        if deadline is None:
            deadline = time.time() + _calculate_async_deadline(
                self.async_strategy)

        transient_attempts = 0
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

            except InsufficientResourcesException as e:
                # The API server returns a 507 when it could not find a
                # home for the request. Some of those refusals are worth
                # waiting out and some are not, so retry only when the
                # caller opted in and the server said so itself. Never
                # retry on the status code alone: an older server's 507
                # carries no marker at all, and an unmarked 507 is not a
                # promise that waiting will help.
                if not self.retry_transient_capacity:
                    raise e

                try:
                    body = json.loads(e.text)
                except (TypeError, ValueError):
                    # A proxy's HTML 507, or a truncated body. Not a
                    # marker, so not a retry.
                    raise e
                if not isinstance(body, dict) or not body.get('transient'):
                    raise e

                # Two bounds, not one, and they are not redundant: the
                # deadline is what the caller asked to wait, the attempt
                # cap is what replaying costs the cluster. Whichever is
                # reached first raises the refusal the server actually
                # sent, so a caller sees the same exception it would have
                # seen with the flag off.
                transient_attempts += 1
                if transient_attempts >= TRANSIENT_RETRY_MAXIMUM_ATTEMPTS:
                    LOG.debug('Transient capacity refusal, %d attempts spent'
                              % transient_attempts)
                    raise e

                # RFC 9110 allows Retry-After to be either delta-seconds or
                # an HTTP-date. Only delta-seconds is implemented here, and
                # that is deliberate rather than an oversight: our server
                # only ever sends the integer 15, and anything we cannot
                # parse -- including a date -- falls back to a default which
                # is that same number.
                try:
                    retry_after = int(e.headers.get('Retry-After', ''))
                except (TypeError, ValueError):
                    retry_after = TRANSIENT_RETRY_DEFAULT
                retry_after = max(TRANSIENT_RETRY_MINIMUM,
                                  min(retry_after, TRANSIENT_RETRY_MAXIMUM))

                # Unlike the 406 clause above, which checks the deadline and
                # then sleeps a whole second regardless, this clamps its
                # sleep to what is left of the budget. That difference is on
                # purpose and should not be "fixed": a 406 overshoots the
                # deadline by at most a second, whereas a 15 second sleep
                # would overshoot by enough for a caller to notice.
                #
                # Note that _calculate_async_deadline(ASYNC_CONTINUE)
                # returns -1, so a client using that strategy and not
                # passing an explicit deadline has a deadline already in the
                # past and never retries here. That is correct -- that
                # strategy means the caller is not waiting for anything --
                # and so it is not special cased.
                remaining = deadline - time.time()
                if remaining <= 0:
                    LOG.debug('Deadline exceeded waiting for capacity')
                    raise e

                wait = min(retry_after, remaining)
                LOG.debug('Transient capacity refusal, retrying in %s seconds'
                          % wait)
                time.sleep(wait)

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
    def _get_events(self, object_plural, object_reference, event_type, limit):
        if event_type or limit:
            if not self.check_capability('events-by-type'):
                raise IncapableException(
                    'The API server version you are talking to does not support '
                    'filtering by event type or count.')

        body = {}
        if event_type:
            body['event_type'] = event_type
        if limit:
            body['limit'] = limit

        r = self._request_url(
            'GET', '/' + object_plural + '/' + object_reference + '/events',
            body)
        return r.json()

    def get_artifact_events(self, artifact_ref, event_type=None, limit=None):
        return self._get_events('artifacts', artifact_ref, event_type, limit)

    def get_blob_events(self, blob_uuid, event_type=None, limit=None):
        if not self.check_capability('blob-events'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'lookup of events for a blob.')
        return self._get_events('blobs', blob_uuid, event_type, limit)

    def get_instance_events(self, instance_ref, event_type=None, limit=None):
        return self._get_events('instances', instance_ref, event_type, limit)

    def get_network_events(self, network_ref, event_type=None, limit=None):
        return self._get_events('networks', network_ref, event_type, limit)

    def get_node_events(self, node, event_type=None, limit=None):
        return self._get_events('nodes', node, event_type, limit)

    def get_namespace_events(self, namespace, event_type=None, limit=None):
        if not self.check_capability('namespace-events'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'lookup of events for a namespace.')
        return self._get_events('auth/namespaces', namespace, event_type, limit)

    def get_namespace_claim_events(self, namespace, claim_uuid, event_type=None, limit=None):
        if not self.check_capability('namespace-claim-events'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'lookup of events for a namespace claim.')
        return self._get_events('auth/namespaces/' + namespace + '/claims',
                                claim_uuid, event_type, limit)

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

    def get_instance(self, instance_ref, namespace=None):
        if namespace and not self.check_capability('get-instance-namespace'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'lookup of instances with a specific namespace.')

        data = None
        if namespace:
            data = {'namespace': namespace}
        r = self._request_url('GET', '/instances/' + instance_ref, data=data)
        return r.json()

    def get_instance_interfaces(self, instance_ref):
        r = self._request_url('GET', '/instances/' + instance_ref +
                              '/interfaces')
        return r.json()

    def create_instance(self, name, cpus, memory, network, disk, sshkey, userdata,
                        namespace=None, force_placement=None, video=None, uefi=False,
                        configdrive=None, nvram_template=None, secure_boot=False,
                        metadata=None, side_channels=None, timeout=None):
        """Create an instance, optionally waiting for it to be created.

        ``timeout`` bounds the whole call -- the dependency retries on the
        POST as well as the wait for the instance to leave its transitional
        states. Pass 0 to return as soon as the POST is accepted, which is
        what a caller that is about to call
        :meth:`await_instance_create` wants: otherwise the create and the
        await are two independent budgets on the same condition, run back
        to back.

        The default of None keeps the historical behaviour, where the wait
        is bounded by the client's async strategy.

        A client built with ``retry_transient_capacity`` spends this same
        budget waiting out a capacity refusal, before the instance
        exists. If that exhausts it, the wait below returns an instance
        still in a transitional state -- see the NOTE there and
        ``docs/transient-capacity-retry.md``.
        """
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

        if timeout is None:
            deadline = time.time() + _calculate_async_deadline(
                self.async_strategy)
        else:
            deadline = time.time() + timeout

        r = self._request_url('POST', '/instances', data=body,
                              deadline=deadline)
        i = r.json()

        while True:
            if i['state'] not in ['initial', 'creating']:
                return i

            LOG.debug('Waiting for instance to be created')
            if time.time() > deadline:
                # NOTE: this returns an instance still in a transitional
                # state rather than raising, and callers cannot tell that
                # apart from success. Changing it would break every caller
                # that treats a transitional instance as normal, so instead
                # give callers a way to make the wait short -- see the
                # timeout argument.
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

    def get_instance_agentoperations(self, instance_ref, all=False):
        if not self.check_capability('instance-agentoperations'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'looking up the agent operations for an instance.')
        if all and not self.check_capability('instance-agentoperations-all'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'looking up all agent operations for an instance.')

        r = self._request_url('GET', '/instances/' + instance_ref +
                              '/agentoperations', data={'all': all})
        return r.json()

    def get_instance_clusteroperations(self, instance_ref, all=False, namespace=None):
        """List the cluster operations targeting an instance, newest first.

        Each operation carries an ``observational`` flag. By default only
        operations which are still queued, in preflight or executing are
        returned; ``all`` adds finished operations the server has not yet
        removed.
        """
        if not self.check_capability('instance-clusteroperations'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'looking up the cluster operations for an instance.')

        data = {'all': all}
        if namespace:
            data['namespace'] = namespace
        r = self._request_url('GET', '/instances/' + instance_ref +
                              '/clusteroperations', data=data)
        return r.json()

    def get_network_clusteroperations(self, network_ref, all=False, namespace=None):
        """List the cluster operations targeting a network, newest first.

        Each operation carries an ``observational`` flag. By default only
        operations which are still queued, in preflight or executing are
        returned; ``all`` adds finished operations the server has not yet
        removed.
        """
        if not self.check_capability('network-clusteroperations'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'looking up the cluster operations for a network.')

        data = {'all': all}
        if namespace:
            data['namespace'] = namespace
        r = self._request_url('GET', '/networks/' + network_ref +
                              '/clusteroperations', data=data)
        return r.json()

    def get_artifact_clusteroperations(self, artifact_ref, all=False, namespace=None):
        """List the cluster operations targeting an artifact, newest first.

        Each operation carries an ``observational`` flag. By default only
        operations which are still queued, in preflight or executing are
        returned; ``all`` adds finished operations the server has not yet
        removed.
        """
        if not self.check_capability('artifact-clusteroperations'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'looking up the cluster operations for an artifact.')

        data = {'all': all}
        if namespace:
            data['namespace'] = namespace
        r = self._request_url('GET', '/artifacts/' + artifact_ref +
                              '/clusteroperations', data=data)
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

    def add_instance_interface(self, instance_ref, netdesc):
        if not self.check_capability('hot-plug-interface'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'hot plugging an interface into an instance.')

        r = self._request_url('POST', '/instances/' + instance_ref +
                              '/interfaces', data={'network': netdesc})
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

    def get_artifact(self, artifact_ref, namespace=None):
        # Unlike instances and networks, the server advertises no capability
        # for a namespace on an artifact lookup, so there is nothing to check.
        data = None
        if namespace:
            data = {'namespace': namespace}
        r = self._request_url('GET', '/artifacts/' + artifact_ref, data=data)
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

    def get_blob_hash(self, blob_uuid, algorithm):
        if not self.check_capability('blob-single-checksum'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'fetching the hash of a blob.')

        r = self._request_url(
            'GET', f'/blobs/{blob_uuid}/checksum/{algorithm}')
        return r.json()

    def get_blob_by_sha512(self, sha512):
        r = self._request_url('GET', '/blob_checksums/sha512/' + sha512)
        return r.json()

    def get_blob_data(self, blob_uuid, offset=0, limit=0):
        supports_limits = self.check_capability('blob-data-limit')
        if limit != 0 and not supports_limits:
            raise IncapableException(
                'The API server version you are talking to does not support '
                'fetching a subsection of a blob.')

        if not supports_limits:
            # If we don't support limits, then we just do a simple single read.
            r = self._request_url(
                'GET', '/blobs/' + blob_uuid + '/data?offset=' + str(offset),
                stream=True)
            for chunk in r.iter_content(chunk_size=8192):
                yield chunk
            return

        # If we do support limits, then we use them to do a series of smaller,
        # probably slightly slower, but more reliable reads.
        limit = 512 * 1024 * 1024
        while True:
            r = self._request_url(
                'GET',
                ('/blobs/' + blob_uuid + '/data?offset=' + str(offset) +
                 '&limit=' + str(limit)),
                stream=True)
            fetched = 0
            for chunk in r.iter_content(chunk_size=8192):
                fetched += len(chunk)
                yield chunk

            offset += fetched
            if fetched < limit:
                return

    def get_blobs(self, node=None):
        r = self._request_url('GET', '/blobs', data={'node': node})
        return r.json()

    def get_networks(self, all=False):
        r = self._request_url('GET', '/networks', data={'all': all})
        return r.json()

    def get_network(self, network_ref, namespace=None):
        if namespace and not self.check_capability('get-network-namespace'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'lookup of networks with a specific namespace.')

        data = None
        if namespace:
            data = {'namespace': namespace}
        r = self._request_url('GET', '/networks/' + network_ref, data=data)
        return r.json()

    def delete_network(self, network_ref, namespace=None, wait=True):
        """Delete a network.

        Phase 7 of the network facade flipped this endpoint to a
        202+poll contract: the server enqueues the delete work and
        returns ``{'op_type': 'net_op', 'op_uuid': '<uuid>'}`` with
        HTTP 202.

        By default (``wait=True``) this method transparently polls
        the returned cluster operation until it reaches a terminal
        state, raising :class:`ClusterOperationFailed` if the op ended
        in ``error`` / ``abort``. The op's final ``external_view``
        dict is returned on success.

        Pass ``wait=False`` to receive the raw op handle (the
        ``{'op_type', 'op_uuid'}`` dict) without polling.
        """
        # Why pass a namespace when you're passing an exact UUID? The idea here
        # is that it provides a consistent interface, but also a safety check
        # against overly zealous loops deleting things.
        data = None
        if namespace:
            data = {'namespace': namespace}
        r = self._request_url('DELETE', '/networks/' + network_ref, data=data)
        # A pre-phase-7 server returns 200 with the network's external
        # view; a phase-7+ server advertises ``network-delete-async`` and
        # returns 202 with a {op_type, op_uuid} handle. Pick the path
        # from the advertised capability so we never try to poll a
        # response that doesn't carry an op handle.
        if not self.check_capability('network-delete-async'):
            return r.json()
        handle = r.json()
        if not wait:
            return handle
        return self._poll_cluster_operation(handle['op_type'], handle['op_uuid'])

    def delete_all_networks(self, namespace, clean_wait=False, wait=True):
        """Delete every network in a namespace.

        Phase 7 of the network facade flipped this endpoint to a
        202+poll contract: the server enqueues one delete op per
        network and returns a list of
        ``{'network_uuid', 'op_type', 'op_uuid'}`` entries with HTTP
        202.

        By default (``wait=True``) this method polls each returned op
        sequentially until terminal, raising
        :class:`ClusterOperationFailed` on the first failure. The
        return value mirrors the input list but each entry gains an
        ``op_view`` key with the op's final ``external_view`` dict.

        Pass ``wait=False`` to receive the raw list of handles
        without polling.
        """
        r = self._request_url('DELETE', '/networks',
                              data={'confirm': True,
                                    'namespace': namespace,
                                    'clean_wait': clean_wait,
                                    })
        # Same capability gate as ``delete_network``: pre-phase-7
        # servers return the old shape, phase-7+ servers advertise
        # ``network-delete-async`` and return the op-handle list.
        if not self.check_capability('network-delete-async'):
            return r.json()
        handles = r.json()
        if not wait:
            return handles

        results = []
        for handle in handles:
            view = self._poll_cluster_operation(
                handle['op_type'], handle['op_uuid'])
            entry = dict(handle)
            entry['op_view'] = view
            results.append(entry)
        return results

    def allocate_network(self, netblock, provide_dhcp, provide_nat, name,
                         namespace=None, provide_dns=False):
        data = {
            'netblock': netblock,
            'provide_dhcp': provide_dhcp,
            'provide_nat': provide_nat,
            'name': name,
            'namespace': namespace
        }

        if provide_dns and not self.check_capability('provide-dns'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'virtual network DNS services.')
        if provide_dns:
            data['provide_dns'] = provide_dns

        r = self._request_url('POST', '/networks', data=data)
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
        r = self._request_url('GET', '/networks/' + network_ref + '/interfaces')
        return r.json()

    def get_network_addresses(self, network_ref):
        if not self.check_capability('list-addresses'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'listing network addresses.')

        r = self._request_url('GET', '/networks/' + network_ref + '/addresses')
        return r.json()

    def reserve_network_address(self, network_ref, address, comment=None):
        if not self.check_capability('reserve-addresses'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'reserving addresses.')

        body = {}
        if comment:
            body['comment'] = comment

        r = self._request_url(
            'POST', '/networks/' + network_ref + '/addresses/' + address,
            data=body)
        return r.json()

    def release_network_address(self, network_ref, address):
        if not self.check_capability('reserve-addresses'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'reserving addresses.')

        self._request_url(
            'DELETE', '/networks/' + network_ref + '/addresses/' + address)

    def route_network_address(self, network_ref):
        if not self.check_capability('route-addresses'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'routing addresses.')

        r = self._request_url('POST', '/networks/' + network_ref + '/route')
        return r.json()

    def unroute_network_address(self, network_ref, address):
        if not self.check_capability('route-addresses'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'routing addresses.')

        self._request_url(
            'DELETE', '/networks/' + network_ref + '/route' + '/' + address)

    def update_network_dns_entry(self, network_ref, name, value):
        if not self.check_capability('extra-dns-entries'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'managing extra DNS entries.')

        self._request_url(
            'POST', '/networks/' + network_ref + '/dns',
            data={
                'name': name,
                'value': value
            })

    def delete_network_dns_entry(self, network_ref, name):
        if not self.check_capability('extra-dns-entries'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'managing extra DNS entries.')

        self._request_url(
            'DELETE', '/networks/' + network_ref + '/dns',
            data={
                'name': name
            })

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

    def get_node_process_metrics(self, node):
        if not self.check_capability('node-process-metrics'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'fetching process metrics for a node.')
        r = self._request_url('GET', '/nodes/' + node + '/processmetrics')
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
        r = self._request_url('GET', url, data=d, response_body_is_binary=True)

        if decode is None:
            return r.content
        return r.content.decode(decode, errors='replace')

    def delete_console_data(self, instance_ref):
        url = '/instances/' + instance_ref + '/consoledata'
        self._request_url('DELETE', url)

    def get_vdi_console_helper(self, instance_ref):
        r = self._request_url('GET', '/instances/' + instance_ref + '/vdiconsolehelper')
        return r.text

    def get_vdi_console_proxy(self, instance_ref):
        r = self._request_url(
            'GET', '/instances/' + instance_ref + '/vdiconsoleproxy')
        return r.json()

    def get_vdi_console_proxy_file(self, instance_ref):
        # The kerbside URL embeds its own capability (a JWT), so this is a
        # plain requests.get() and must NOT go through _request_url --
        # that would incorrectly attach the SF bearer token.
        result = self.get_vdi_console_proxy(instance_ref)
        try:
            r = requests.get(result['url'], timeout=self.sync_request_timeout)
            r.raise_for_status()
            return r.text

        except requests.exceptions.RequestException as e:
            # requests builds its messages out of the URL it was handed,
            # so every one of them carries the live token: HTTPError's
            # "for url: ...", the connection errors' "Max retries
            # exceeded with url: ...". Only some of those are caught in
            # main.py, and an uncaught one is rendered by the interpreter
            # as a traceback, which no logging filter ever sees. Redact
            # here, where we know the URL is a credential, rather than
            # depending on where the exception happens to land.
            #
            # type(e) keeps the class so an existing caller's except
            # clause still matches; every requests exception takes its
            # message positionally. "from None" is what stops the
            # interpreter printing the original, unredacted exception as
            # the context of this one.
            raise type(e)(redact_tokens(str(e))) from None

    def get_vdi_token_public_keys(self):
        r = self._request_url('GET', '/admin/vditokenpubkey')
        return r.json()

    def get_screenshot(self, instance_ref):
        if not self.check_capability('instance-screenshot'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'fetching a screenshot of an instance.')

        r = self._request_url('GET', '/instances/' + instance_ref + '/screenshot')
        return self.get_blob_data(r.json())

    def _await_agentop(self, r, await_seconds=None):
        """Poll an agent operation until it settles, or until we stop waiting.

        ``await_seconds`` is how long *this client* will wait, which is a
        different number from the ``deadline_seconds`` the operation was
        created with -- that one bounds the operation's life on the server.
        A caller which has a budget of its own (``await_agent_command``,
        ``await_agent_fetch``) passes it here so the wait it asked for is
        the wait it gets; every other caller waits for as long as the async
        strategy says, which is this client's general policy for how long a
        call may block.

        A terminal failure state raises for *every* caller, including
        ASYNC_CONTINUE. Fire and forget means "do not wait", not "do not
        notice a failure the server has already reported": an operation
        which is dead in the POST response is not something a caller can
        usefully be handed and told to poll later. Only the still in flight
        case returns silently.
        """
        if await_seconds is not None:
            deadline = time.time() + await_seconds
        else:
            deadline = time.time() + _calculate_async_deadline(self.async_strategy)

        while True:
            if r['state'] in TERMINAL_AGENT_OPERATION_STATES:
                if r['state'] != 'complete':
                    raise AgentOperationFailed(
                        f'Agent operation {r["uuid"]} entered terminal state '
                        f'"{r["state"]}"', r['uuid'], r)
                return r

            LOG.debug('Waiting for agent operation to be complete')
            if time.time() > deadline:
                # Returning the still in flight operation instead of raising
                # is deliberate, and it covers two cases which are not the
                # same. With ASYNC_CONTINUE the deadline is already in the
                # past when the loop starts, so this is the fire and forget
                # path for a caller which never meant to wait. Under
                # ASYNC_PAUSE or ASYNC_BLOCK the caller *was* waiting and
                # still gets an in flight operation back with no exception:
                # that is long standing behaviour the CLI depends on, since
                # it prints whatever results have arrived and lets the user
                # poll later. A caller which needs a hard failure passes
                # await_seconds and inspects the state itself.
                LOG.debug('Deadline exceeded waiting for agent operation to complete')
                return r

            time.sleep(1)
            r = self.get_agent_operation(r['uuid'])

    def await_agent_chain(self, operations, timeout=None):
        """Await a chain of ordered agent operations and report what broke it.

        ``operations`` is the operation views a caller submitted, in
        submission order -- each one created with ``await_seconds=0`` so
        that submitting the chain does not wait for it, which is the whole
        point of declaring the order on the server instead of enforcing it
        from here. Only the last operation is awaited, because every other
        link has to finish before it can run.

        Returns the last operation once it completes. An operation which is
        still in flight when the wait runs out is returned as
        ``_await_agentop`` returns it, with no exception, which is this
        client's long standing behaviour for a wait that expired rather
        than a thing that failed.

        A chain fails backwards: the operation the caller awaited is
        aborted because the one before it did not succeed, and that one may
        have been aborted for the same reason in turn. So the exception
        raised from here names the operation which *actually* failed rather
        than the last link in the chain, and when it cannot name one it says
        which of the several reasons it cannot -- each has a different
        remedy, and guessing between them is worse than reporting the
        uncertainty.
        """
        if not operations:
            raise ValueError(
                'await_agent_chain() needs the operations of a chain to await, and was given none')

        try:
            return self._await_agentop(operations[-1], await_seconds=timeout)
        except AgentOperationFailed as e:
            if e.op_view.get('state') != 'abort':
                # Not a chain question. abort is the only state the
                # dependency cascade writes, so any other failure is this
                # operation's own and is already reported as well as it can
                # be -- re-describing it in terms of a chain would bury the
                # real failure under a walk which has nothing to walk.
                raise

            raise self._agentop_chain_failure(operations, e) from e

    def _agentop_chain_failure(self, operations, failure):
        """Walk an aborted operation back to the one which actually failed.

        Returns the AgentOperationFailed to raise in place of the one the
        awaited operation produced, which is a return rather than a raise so
        that the caller above can chain the two exceptions together.

        Each hop reads a dependency_failure, which the cascade writes as
        {op_type, op_uuid, state} before it assigns the abort state. The
        recorded state is not always a failure: the dispatcher aborts a
        dependent both for a dependency which failed and for one which never
        finished inside the dependency wait budget, and it records two
        sentinels which are not states at all. Those are branches below
        rather than assumptions, because they are remedied differently.
        """
        # Only the operations the caller submitted, which is what bounds
        # what this walk is willing to look at.
        submitted = {op['uuid']: op for op in operations if op.get('uuid')}

        awaited = failure.op_uuid
        current_uuid = awaited
        current_view = failure.op_view
        hops = 0

        # Bounded by the chain which was submitted. A client cannot build a
        # cycle -- an edge may only name an operation which already exists --
        # but this walk reads state a cluster wrote, and a reporting helper
        # which a surprising cluster could hang is a worse answer than one
        # which stops and says it stopped.
        for _ in range(len(operations)):
            dependency_failure = current_view.get('dependency_failure')
            if not dependency_failure:
                return AgentOperationFailed(
                    f'Agent operation {current_uuid} ({_agentop_chain_position(hops)}) was aborted, but it '
                    'carries no dependency_failure, so which dependency aborted it cannot be named. Only '
                    'the dependency cascade writes an abort, and it records the cause before it writes the '
                    'state, so an abort without one is a cluster to look at rather than a chain to read.',
                    current_uuid, current_view)

            state = dependency_failure.get('state')
            named = dependency_failure.get('op_uuid')

            if state in AGENT_DEPENDENCY_PENDING_STATES:
                # Not a failed step at all: the dependency was still working
                # when the dependent ran out of patience. Said separately
                # because the remedy is a longer budget or a quicker
                # dependency, and nothing is wrong with either operation.
                return AgentOperationFailed(
                    f'Agent operation {awaited} was aborted because the chain ran out of its dependency '
                    f'wait budget, not because a step failed. {current_uuid} '
                    f'({_agentop_chain_position(hops)}) was waiting on {named}, which was still in state '
                    f'"{state}" when the wait expired. A dependent waits at most '
                    'AGENT_OPERATION_DEPENDENCY_WAIT seconds for a dependency to finish, 300 by default, '
                    'so the remedy is a longer budget on the cluster or a dependency which finishes '
                    'sooner.',
                    named, submitted.get(named, {}))

            if state == AGENT_DEPENDENCY_MISSING:
                # The dependency is gone, so success and failure cannot be
                # told apart. Not a guess in either direction: an errored
                # operation is never swept while every other terminal one is
                # swept an hour after it finished, so "gone" is weak evidence
                # it did not error and no evidence at all about the rest.
                return AgentOperationFailed(
                    f'Agent operation {awaited} was aborted because its dependency {named} had been swept '
                    f'or deleted by the time {current_uuid} ({_agentop_chain_position(hops)}) came to run, '
                    'so whether that dependency succeeded or failed can no longer be established. A '
                    'completed, aborted, expired or deleted operation is swept an hour after it finished '
                    'while an errored one is never swept, so the state it ended in is not recorded '
                    'anywhere left to read.',
                    named, submitted.get(named, {}))

            if state == AGENT_DEPENDENCY_UNREADABLE:
                # Nothing was learned about the dependency, which is a
                # different thing from learning that it is gone, and it is a
                # cluster problem rather than a chain problem.
                return AgentOperationFailed(
                    f'Agent operation {awaited} was aborted because the dispatcher could not read the '
                    f'state of its dependency {named} at all, which is a problem with the cluster rather '
                    f'than with the chain that was submitted. Nothing was learned about {named}, so it may '
                    'well have succeeded. Look at the sidechannel daemon on the node holding the instance, '
                    'and at that node\'s database connectivity, before resubmitting the chain.',
                    named, submitted.get(named, {}))

            if named not in submitted:
                # Deliberately not fetched. A chain may cross namespaces, so
                # reading an operation the caller did not hand us turns a
                # reporting helper into a question about authorisation --
                # and an answer the caller may not be entitled to.
                return AgentOperationFailed(
                    f'Agent operation {awaited} was aborted because its dependency {named} ended in state '
                    f'"{state}", but that operation was not submitted with this chain, so the cause cannot '
                    'be resolved any further here. This client does not read an operation it was not '
                    'given: a chain may cross namespaces, and resolving a uuid from somewhere else would '
                    'ask a question about authorisation rather than about the chain.',
                    named, {})

            hops += 1
            current_uuid = named
            current_view = self._refresh_chain_operation(named, submitted[named], state)

            if current_view.get('state') != 'abort':
                # The first link whose own failure was not a dependency
                # failure, which is the operation the operator has to go and
                # look at.
                failed_state = current_view.get('state', state)
                return AgentOperationFailed(
                    f'Agent operation {awaited} was aborted because a dependency failed. The operation '
                    f'which actually failed is {current_uuid}, {_agentop_chain_position(hops)}, which '
                    f'ended in state "{failed_state}".',
                    current_uuid, current_view)

        return AgentOperationFailed(
            f'Agent operation {awaited} was aborted, and the chain of aborts behind it is longer than the '
            f'{len(operations)} operations submitted, so the walk was stopped rather than followed any '
            f'further. The last operation it reached was {current_uuid}.',
            current_uuid, current_view)

    def _refresh_chain_operation(self, op_uuid, submitted_view, observed_state):
        """Fetch the current view of one operation in a submitted chain.

        Only ever called with a uuid which is in the list handed to
        ``await_agent_chain()``, which is the whole of this walk's reading
        policy. The fetch is needed because a chain's views are as the
        creating POSTs returned them -- the state in one is the state the
        operation started in, not the one it ended in -- and the walk needs
        the ending state to tell a link which was cascaded in turn from the
        one which actually failed.

        Best effort, for the reason ``_agent_failure_context`` gives: a read
        which fails while *decorating* a failure must not replace it, so a
        failed read falls back to the submitted view carrying the state the
        dispatcher recorded having observed.
        """
        try:
            return self.get_agent_operation(op_uuid)
        except Exception:
            return dict(submitted_view, state=observed_state)

    def _add_agentop_timing(self, data, deadline_seconds, progress_timeout_seconds):
        """Add the timing parameters the caller actually asked for to a request body.

        Only values the caller passed are sent, and an omitted value means
        the server's own default applies. Nothing is derived here, because
        the async strategy is a statement about how long this client will
        block and the deadline is a statement about how long the operation
        may live -- deriving one from the other would give every CLI
        invocation a 60 second server side kill (ASYNC_PAUSE is the CLI
        default) in place of the server's far more generous default, and an
        upload into a guest would start dying at a minute.

        The keys are built here, in the creating helpers, rather than in
        ``_await_agentop``, because by the time that is polling the POST
        which created the operation has already gone out.

        Zero is a real value and is passed through: the server reads it as
        "no such budget at all", so the ``is not None`` tests below must not
        be tidied into truthiness tests.
        """
        if not self.check_capability('agentoperation-deadlines'):
            # Decision 7: against a server which cannot accept these, the
            # client behaves exactly as it did before they existed.
            return

        if deadline_seconds is not None:
            data['deadline_seconds'] = deadline_seconds
        if progress_timeout_seconds is not None:
            data['progress_timeout_seconds'] = progress_timeout_seconds

    def _add_agentop_dependencies(self, data, depends_on, runs_after):
        """Add the dependency edges the caller declared to a request body.

        Only a list the caller passed sends a key. Neither None -- a
        wrapper forwarding a keyword argument it does not use -- nor an
        empty list -- a caller who built no edges -- is a declaration, so
        neither consults the capability nor sends anything, because
        refusing either would be a refusal with no cause. That is the
        opposite of the timing parameters above, where zero is a real
        value and so only None may be treated as absent.

        Unlike ``_add_agentop_timing``, a server which cannot accept
        these is refused rather than silently sent nothing. An omitted
        deadline means "the server's own default applies", which is what
        an older server would have given anyway and costs the caller a
        budget. A dropped ordering edge means "run now", which is the
        precise outcome the caller declared the edge to prevent -- an
        upload and the command which reads the file it placed would both
        be dispatched with no guarantee the upload had finished -- and
        the caller would be told it had succeeded. Silence there is worse
        than a refusal.
        """
        if not (depends_on or runs_after):
            return

        if not self.check_capability('agentoperation-dependencies'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'agent operation dependencies, which require the '
                'agentoperation-dependencies capability.')

        if depends_on:
            data['depends_on'] = _agentop_dependency_edges('depends_on', depends_on)
        if runs_after:
            data['runs_after'] = _agentop_dependency_edges('runs_after', runs_after)

    def _verify_agentop_dependencies(self, data, op):
        """Check that the edges a creating call sent came back on the operation.

        The agentoperation-dependencies capability says the API server
        understands the parameters. It does not say the tier behind it
        stores them, and the server records the consequence itself in the
        comment which defines that token
        (shakenfist/external_api/app.py): a newer sf-api against an older
        sf-database accepts the parameters, answers 200, and stores empty
        edge lists.

        That is the one way this family of calls fails silently -- an
        operation which will run with no ordering at all, reported to the
        caller as a success -- and no capability token can report it. The
        POST response is the created operation's own view, which carries
        both edge lists, so this costs no extra request.

        Only the op_uuid sets are compared. Not settle_seconds, because a
        tier which does not understand an edge drops the whole edge rather
        than part of one, and not order, which the server does not promise
        to preserve.
        """
        for name in ('depends_on', 'runs_after'):
            sent = data.get(name)
            if not sent:
                # A caller who declared nothing on this list has nothing to
                # verify, so whatever the response carries is not this
                # client's business.
                continue

            # Neither side needs normalising here. The edges were
            # canonicalised when they were built, and the server's op_uuid is
            # a pydantic UUID4 which its view renders in that same canonical
            # lower case form -- so the two sides are already the same
            # strings. Normalising again would be a second place for the two
            # forms to disagree, and could only hide the mismatch this check
            # exists to find.
            sent_uuids = {edge['op_uuid'] for edge in sent}
            stored_uuids = {edge.get('op_uuid') for edge in op.get(name) or []}
            if sent_uuids == stored_uuids:
                continue

            # Counts, and the edges which did not come back rather than
            # both sets in full: an operation may name as many edges as the
            # cluster allows, and a sentence carrying a hundred and
            # twenty eight UUIDs is one nobody reads to the end of.
            detail = f'{len(sent_uuids)} sent, {len(stored_uuids)} came back'
            missing = sorted(sent_uuids - stored_uuids)
            if missing:
                detail += ', missing ' + ', '.join(missing[:3])
                if len(missing) > 3:
                    detail += f' and {len(missing) - 3} more'

            raise IncapableException(
                f'The API server accepted the {name} edges for agent operation '
                f'{op["uuid"]} and answered success, but the database tier did not store '
                f'them: {detail}. This is a cluster part way through an upgrade -- sf-api '
                'advertises the agentoperation-dependencies capability, but the '
                'sf-database behind it predates the dependency fields and drops them. '
                'Upgrade sf-database to a version which stores dependency edges. The '
                'call which was made is correct, and is refused here only because the '
                'operation would otherwise run unordered while reporting that it had '
                'been ordered.')

    @staticmethod
    def _remaining_agentop_budget(start_time, timeout):
        """How much of the caller's timeout is left, as a server side budget.

        Never zero, and never omitted, which are the two other things an
        exhausted budget could plausibly be spelt as. Zero is wrong because
        the server reads a deadline of 0 as "no wall clock deadline at all",
        which is the opposite of what an exhausted caller means. Omitting
        the key is wrong for a subtler reason: this client is about to stop
        waiting, and an operation nobody is waiting for should be reaped
        rather than left running under the server's default budget. One
        second says "start it, then kill it", which is the honest request.
        """
        return max(1, round(timeout - (time.time() - start_time)))

    def _agent_failure_context(self, instance_uuid):
        """Gather what an operator wants to see when an agent operation fails.

        Best effort, deliberately. Both lookups can fail for the same
        reason the operation being reported failed -- an instance which
        went away underneath a long running operation is the common case,
        and it is exactly what a "deleted" operation usually means -- and
        letting a ResourceNotFoundException raised while *decorating* a
        failure replace that failure would report the wrong thing
        entirely, losing the operation uuid and state the caller needs.

        Returns the agent state and console data as strings, already
        phrased for a failure message, so both callers below read the same
        way whether or not the lookups worked.
        """
        try:
            i = self.get_instance(instance_uuid)
            return i['agent_state'], self.get_console_data(instance_uuid)
        except Exception as e:
            return f'could not be gathered: {e}', f'could not be gathered: {e}'

    def _enriched_agent_failure(self, verb, op, instance_uuid):
        """Build an AgentOperationFailed carrying what an operator will want.

        A failed agent operation is exactly when someone wants the console,
        so gather it here. ``_await_agentop`` cannot: it knows the operation
        but not the instance it ran against, and it raises before its caller
        has a chance to add anything.
        """
        agent_state, cd = self._agent_failure_context(instance_uuid)
        return AgentOperationFailed(
            f'Agent {verb} operation {op["uuid"]} on instance {instance_uuid} '
            f'entered terminal state "{op["state"]}"\n'
            f'    Agent state: {agent_state}\n\n'
            f'    Console data: {cd}',
            op['uuid'], op)

    def instance_put_blob(self, instance_ref, blob_uuid, path, mode,
                          deadline_seconds=None, progress_timeout_seconds=None,
                          await_seconds=None, depends_on=None, runs_after=None):
        if not self.check_capability('instance-put-blob'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'placing a blob on an instance.')

        data = {'blob_uuid': blob_uuid, 'path': path, 'mode': mode}
        self._add_agentop_timing(data, deadline_seconds, progress_timeout_seconds)
        self._add_agentop_dependencies(data, depends_on, runs_after)

        r = self._request_url('POST', '/instances/' + instance_ref + '/agent/put',
                              data=data)
        op = r.json()
        self._verify_agentop_dependencies(data, op)
        return self._await_agentop(op, await_seconds=await_seconds)

    def instance_execute(self, instance_ref, command_line, deadline_seconds=None,
                         await_seconds=None, depends_on=None, runs_after=None):
        if not self.check_capability('instance-execute'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'executing a command within an instance.')

        data = {'command_line': command_line}
        # The server refuses a progress timeout on execute (decision 4;
        # shakenfist/external_api/instance.py), so execute never gains a
        # progress_timeout_seconds kwarg and always passes None here. The
        # dependency parameters have no such asymmetry: all three endpoints
        # accept both edge lists.
        self._add_agentop_timing(data, deadline_seconds, None)
        self._add_agentop_dependencies(data, depends_on, runs_after)

        r = self._request_url('POST', '/instances/' + instance_ref + '/agent/execute',
                              data=data)
        op = r.json()
        self._verify_agentop_dependencies(data, op)
        return self._await_agentop(op, await_seconds=await_seconds)

    def instance_get(self, instance_ref, path,
                     deadline_seconds=None, progress_timeout_seconds=None,
                     await_seconds=None, depends_on=None, runs_after=None):
        if not self.check_capability('instance-get'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'fetching a file from within an instance.')

        data = {'path': path}
        self._add_agentop_timing(data, deadline_seconds, progress_timeout_seconds)
        self._add_agentop_dependencies(data, depends_on, runs_after)

        r = self._request_url('POST', '/instances/' + instance_ref + '/agent/get',
                              data=data)
        op = r.json()
        self._verify_agentop_dependencies(data, op)
        return self._await_agentop(op, await_seconds=await_seconds)

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
        r = self._request_url(
            'PUT', '/auth/namespaces/' + namespace + '/keys/' + key_name,
            data={'key': key})
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

    def get_namespace_claims(self, namespace):
        r = self._request_url(
            'GET', '/auth/namespaces/' + namespace + '/claims')
        return r.json()

    def get_namespace_claim(self, namespace, claim_uuid):
        r = self._request_url(
            'GET', '/auth/namespaces/' + namespace + '/claims/' + claim_uuid)
        return r.json()

    def create_namespace_claim(self, namespace, limit_cpus, limit_memory_mb,
                               limit_disk_gb, expires_in_seconds):
        """Claim aggregate cluster capacity for a namespace.

        expires_in_seconds is a duration, not a timestamp, and is deliberately
        so: the expiry is computed from the cluster's clock, which is the only
        clock the expiry sweep ever compares against. Pass the duration
        through rather than converting a local datetime into one.
        """
        r = self._request_url(
            'POST', '/auth/namespaces/' + namespace + '/claims',
            data={'limit_cpus': limit_cpus,
                  'limit_memory_mb': limit_memory_mb,
                  'limit_disk_gb': limit_disk_gb,
                  'expires_in_seconds': expires_in_seconds})
        return r.json()

    def update_namespace_claim(self, namespace, claim_uuid, limit_cpus=None,
                               limit_memory_mb=None, limit_disk_gb=None,
                               expires_in_seconds=None):
        """Grow, shrink or re-date a capacity claim.

        The server treats the supplied keys as a field mask and changes only
        those, so this sends only the arguments the caller actually passed.
        Reading a claim and sending all four values back is not equivalent: it
        turns a re-date into a resize which races whatever else is moving the
        claim. An empty body is left for the server to reject rather than
        guessed at here.
        """
        data = {}
        for key, value in [('limit_cpus', limit_cpus),
                           ('limit_memory_mb', limit_memory_mb),
                           ('limit_disk_gb', limit_disk_gb),
                           ('expires_in_seconds', expires_in_seconds)]:
            if value is not None:
                data[key] = value

        r = self._request_url(
            'PUT', '/auth/namespaces/' + namespace + '/claims/' + claim_uuid,
            data=data)
        return r.json()

    def delete_namespace_claim(self, namespace, claim_uuid):
        r = self._request_url(
            'DELETE',
            '/auth/namespaces/' + namespace + '/claims/' + claim_uuid)
        return r.json()

    def check_placement(self, shapes):
        """Ask whether a set of requested instance shapes would fit now.

        shapes is a list of mappings, each describing one instance the caller
        is thinking about creating: cpus and memory (in megabytes) are
        required, and disk (in gigabytes), disk_spec (the disk list an
        instance create takes, summed by the server), count (how many of that
        shape, defaulting to one) and affinity are optional. Note that
        disk_spec is the list create_instance() sends as disk: the key is
        named differently here because disk is the gigabyte total, so a
        caller assembling a query from a create body it already holds has
        to move the list across rather than pass it as disk. disk and
        disk_spec are alternatives and sending both is refused rather than
        reconciled. The list is passed through as the caller built it: the
        server is the only validator of a shape, and a second opinion here
        could only disagree with it.

        The set is answered as a set. Each shape is placed in its own right
        and charged to the node it would land on before the next is
        considered, so a set is never told that two of its members fit in one
        node's worth of room, and placement is all or nothing.

        Returns the parsed answer, which always carries fits. A set which
        does not fit is a successful call returning fits False, the pipeline
        stage which refused in stage, and the index of the shape being walked
        in shape -- never an exception. That index counts the expanded
        request order, so a shape asked for with a count of three
        contributes three consecutive indexes and the index lines up with
        the shapes array a successful answer carries. The server answers a
        refusal with a 200 rather than a 507 precisely so a caller can tell
        "the cluster is full" from "the query failed", and raising here would
        discard the distinction the endpoint exists to draw. A 507 remains
        what a create gets.

        A cluster administrator is additionally told, in shapes, which node
        each shape would land on and the ordered candidate list it was chosen
        from -- that node first, then its fallbacks -- in the order the
        shapes were asked about. Every other caller is answered at set level,
        so a caller must not require that key to be present.

        Nothing is created, nothing is reserved and no capacity is held. The
        answer describes the cluster at the moment it was asked, and the room
        it found may be gone by the next request.
        """
        if not self.check_capability('placement-query'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'placement queries.')

        r = self._request_url('POST', '/placement', data={'shapes': shapes})
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
        retries = 0

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
        if not self.check_capability('agentoperations-crud'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'agent operations.')

        r = self._request_url('GET', '/agentoperations/' + operation_uuid)
        return r.json()

    def delete_agent_operation(self, operation_uuid):
        if not self.check_capability('agentoperations-crud'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'agent operations.')

        self._request_url('DELETE', '/agentoperations/' + operation_uuid)

    def get_cluster_cacert(self):
        if not self.check_capability('cluster-cacert'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'fetching a CA certificate for the cluster.')
        r = self._request_url('GET', '/admin/cacert')
        return r.text

    def get_cluster_resources(self):
        if not self.check_capability('cluster-resources'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'reporting cluster resources.')
        r = self._request_url('GET', '/admin/resources')
        return r.json()

    def get_cluster_claims(self, coverage_state=None):
        """List every claim in the cluster, across all namespaces (admin only).

        coverage_state may be 'active' or 'expired' to filter the result.
        The server validates it, so an unknown value is a 400 rather than
        something this client second-guesses.
        """
        if not self.check_capability('cluster-claims'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'listing cluster claims.')
        body = {}
        if coverage_state is not None:
            body['coverage_state'] = coverage_state
        r = self._request_url('GET', '/admin/claims', body)
        return r.json()

    # The following methods are convenience wrappers around methods above.
    def await_instance_create(self, instance_uuid, timeout=600):
        """Wait for an instance to reach a terminal state.

        The default of ten minutes is generous because on a slow morning
        it can take over two minutes just to download a Ubuntu image.

        This is a budget for this call alone. A caller that created the
        instance itself should pass timeout=0 to create_instance, or the
        two waits run back to back on the same condition and the task
        takes the sum of them while reporting only this one.
        """
        start_time = time.time()
        final = False
        # Checked at least once even for a non-positive timeout, so an
        # already-created instance is reported as such rather than as a
        # timeout, and i is never referenced unbound below.
        i = self.get_instance(instance_uuid)
        while True:
            if i['state'] == 'created' or _is_error_state(i['state']):
                final = True
                break
            if time.time() - start_time >= timeout:
                break
            time.sleep(5)
            i = self.get_instance(instance_uuid)

        if _is_error_state(i['state']):
            raise InstanceWillNeverBeReady(
                'failed to start (marked as error state, %s)' % i)

        if not final:
            raise TimeoutException(
                'not created within %d second timeout (last state: %s)'
                % (timeout, i['state']))

    def await_no_pending_operations(self, object_type, object_ref,
                                    namespace=None, timeout=None, interval=1):
        """Wait for an instance, network or artifact to have no pending operations.

        Polls until the object's ``has_pending_operations`` is false and
        returns the final view of the object. Observational operations, such
        as a checksum verification, never make an object pending.

        ``timeout`` defaults to the client's async deadline (governed by
        ``async_strategy``); pass a positive number of seconds to override.
        A negative timeout returns the current view without waiting, and a
        timeout of zero raises as soon as the deadline has passed, normally
        after one check, if the object is pending.
        Raises :class:`TimeoutException` if the object is still pending when
        the timeout elapses, :class:`InvalidException` if ``interval`` is not
        positive, and :class:`IncapableException` if a view does not report
        ``has_pending_operations`` at all. An object deleted while waiting
        raises :class:`ResourceNotFoundException` rather than counting as
        settled.
        """
        getters = {
            'instance': self.get_instance,
            'network': self.get_network,
            'artifact': self.get_artifact
        }
        if object_type not in getters:
            raise InvalidException(
                'object_type must be one of instance, network or artifact, '
                'not %s' % object_type)
        if not self.check_capability('object-pending-operations'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'reporting pending operations on objects.')
        if interval <= 0:
            raise InvalidException(
                'interval must be a positive number of seconds, not %s'
                % interval)

        if timeout is None:
            timeout = _calculate_async_deadline(self.async_strategy)
        getter = getters[object_type]

        def fetch():
            view = getter(object_ref, namespace=namespace)
            # A view without the field is not evidence that nothing is
            # pending, so refuse rather than report the object settled.
            if 'has_pending_operations' not in view:
                raise IncapableException(
                    '%s %s view does not report has_pending_operations'
                    % (object_type, object_ref))
            return view

        if timeout < 0:
            return fetch()

        deadline = time.time() + timeout
        while True:
            view = fetch()
            if not view['has_pending_operations']:
                return view

            if time.time() > deadline:
                raise TimeoutException(
                    '%s %s still had pending operations after %s seconds'
                    % (object_type, object_ref, timeout))

            time.sleep(interval)

    def _instance_await_sanity_check(self, inst):
        if not inst:
            raise InstanceWillNeverBeReady('instance missing')

        if inst['state'] == 'deleted':
            raise InstanceWillNeverBeReady('instance deleted')

        if _is_error_state(inst['state']):
            raise InstanceWillNeverBeReady('instance in error state')

        if 'sf-agent2' not in inst['side_channels']:
            raise InstanceWillNeverBeReady(
                'instance does not have agent side channel')

    def await_agent_ready(self, instance_ref, timeout=600):
        inst = self.get_instance(instance_ref)
        self._instance_await_sanity_check(inst)

        start_time = time.time()
        while time.time() - start_time < timeout:
            if inst['state'] == 'created':
                break
            time.sleep(5)

            inst = self.get_instance(instance_ref)
            self._instance_await_sanity_check(inst)

        self._instance_await_sanity_check(inst)
        if inst['state'] != 'created':
            raise InstanceWillNeverBeReady(
                'instance never reached created state')

        while time.time() - start_time < timeout:
            if inst['agent_state'].startswith('ready'):
                break
            time.sleep(5)

            inst = self.get_instance(instance_ref)
            self._instance_await_sanity_check(inst)

        self._instance_await_sanity_check(inst)
        if not inst['agent_state'].startswith('ready'):
            raise InstanceWillNeverBeReady(
                'instance never reached ready agent state')

    def await_agent_command(self, instance_uuid, command, exit_codes=[0],
                            ignore_stderr=False, timeout=120):
        start_time = time.time()
        self.await_agent_ready(instance_uuid, timeout=timeout)

        # Both budgets are what is *left* of the timeout once the agent
        # became ready, not the whole of it: await_agent_ready() shares
        # start_time with the loops below, so an instance which spent 90 of
        # a 120 second budget becoming ready leaves 30 seconds here. Sending
        # the full timeout would keep the operation alive on the server long
        # after this call gave up on it, and the loops below would be
        # entered already expired. The floor of one second matters because
        # the server reads a deadline of 0 as "no wall clock deadline at
        # all", which is the opposite of what an exhausted budget means.
        remaining = self._remaining_agentop_budget(start_time, timeout)
        try:
            op = self.instance_execute(
                instance_uuid, command, deadline_seconds=remaining,
                await_seconds=remaining)
        except AgentOperationFailed as e:
            # _await_agentop() raises as soon as it polls a terminal failure
            # state, which is the common case for a command which fails
            # promptly. Catch it here so the console data below is actually
            # reachable rather than only being gathered in the narrow window
            # where the state turned terminal after we stopped polling.
            raise self._enriched_agent_failure(
                'execute', e.op_view or {'uuid': e.op_uuid, 'state': 'unknown'},
                instance_uuid) from e

        # There is no state polling loop here, because instance_execute()
        # has already done it: _await_agentop() was handed this call's own
        # budget, so it returns either a complete operation or an in flight
        # one whose deadline has passed, and raises for every terminal
        # failure (caught above). A second loop would be an already expired
        # copy of the first, and a second terminal state check could never
        # see one. All that is left is to report the timeout.
        if op['state'] != 'complete':
            agent_state, cd = self._agent_failure_context(instance_uuid)
            raise AgentAwaitTimeout(
                f'Agent execute operation {op["uuid"]} on instance {instance_uuid} '
                'did not complete within specified timeout\n'
                f'    Timeout: {timeout}\n'
                f'    Operation state: {op["state"]}\n'
                f'    Agent state: {agent_state}\n\n'
                f'    Console data: {cd}')

        # Wait for the operation to have results.
        while time.time() - start_time < timeout:
            if op['results'] != {}:
                break
            time.sleep(5)
            op = self.get_agent_operation(op['uuid'])

        # This guard has to precede the subscripts it protects, or an empty
        # results dict raises KeyError instead of saying what went wrong.
        if not op['results']:
            raise AgentCommandError('operation returned no results')

        exit_code = op['results']['0']['return-code']
        stderr = op['results']['0']['stderr']

        if not ignore_stderr and stderr:
            raise AgentCommandError(f'stderr was "{stderr}", not empty')

        # Short results are directing in the operation, longer results are in
        # a blob.
        if 'stdout' in op['results']['0']:
            data = op['results']['0']['stdout']
        else:
            if 'stdout_blob' not in op['results']['0']:
                raise AgentCommandError('operation returned no stdout blob')

            # Wait for the blob containing stdout to be ready
            b = self.get_blob(op['results']['0']['stdout_blob'])
            while time.time() - start_time < timeout:
                if b['state'] == 'created':
                    break
                time.sleep(5)
                b = self.get_blob(op['results']['0']['stdout_blob'])

            # Fetch the blob containing stdout
            data = ''
            for chunk in self.get_blob_data(op['results']['0']['stdout_blob']):
                data += chunk.decode('utf-8')

        if exit_code not in exit_codes:
            raise AgentCommandError(
                'Unexpected command failure:\n'
                f'command: {command}\n'
                f'exit code: {exit_code}\n'
                f'stdout: {data}\n'
                f'stderr: {stderr}')

        return exit_code, data

    def await_agent_fetch(self, instance_uuid, path, timeout=120):
        start_time = time.time()
        self.await_agent_ready(instance_uuid, timeout=timeout)

        # See await_agent_command() above for why this is the remaining
        # budget rather than the whole timeout, and why it has a floor of
        # one second.
        remaining = self._remaining_agentop_budget(start_time, timeout)
        try:
            op = self.instance_get(
                instance_uuid, path, deadline_seconds=remaining,
                await_seconds=remaining)
        except AgentOperationFailed as e:
            raise self._enriched_agent_failure(
                'fetch', e.op_view or {'uuid': e.op_uuid, 'state': 'unknown'},
                instance_uuid) from e

        # See await_agent_command() above for why there is no state polling
        # loop here: instance_get() has already waited out this call's
        # budget and raised for every terminal failure.
        if op['state'] != 'complete':
            raise AgentAwaitTimeout(
                f'Agent fetch operation {op["uuid"]} did not complete in '
                f'{timeout} seconds with state {op["state"]}')

        # Wait for the operation to have results
        while time.time() - start_time < timeout:
            if op['results'] != {}:
                break
            time.sleep(5)
            op = self.get_agent_operation(op['uuid'])

        if not op['results']:
            raise AgentCommandError('operation returned no results')
        if 'content_blob' not in op['results']['0']:
            raise AgentCommandError('operation returned no content blob')

        # Wait for the blob containing the file to be ready
        b = self.get_blob(op['results']['0']['content_blob'])
        while time.time() - start_time < timeout:
            if b['state'] == 'created':
                break
            time.sleep(5)
            b = self.get_blob(op['results']['0']['content_blob'])

        # Fetch the blob containing the file
        data = ''
        for chunk in self.get_blob_data(op['results']['0']['content_blob']):
            data += chunk.decode('utf-8')

        return data

    def await_agent_add_instance_interface(
            self, instance_uuid, netdesc, timeout=120):
        self.await_agent_ready(instance_uuid, timeout=timeout)
        netdesc = self.add_instance_interface(instance_uuid, netdesc)

        # I don't love this sleep, but we don't have any other way to test
        # whether the command has executed on the hypervisor right now.
        time.sleep(5)

        # List interfaces
        _, data = self._await_agent_command(instance_uuid, 'ip -json link')

        if not netdesc['macaddr'] in data:
            raise AgentCommandError(
                'interface not found in `ip -json link` output:\n%s' % data)

        # Determine which interface the new one was added as
        d = json.loads(data)
        new_interface = None
        for i in d:
            if i['address'] == netdesc['macaddr']:
                new_interface = i['ifname']
        if not new_interface:
            raise AgentCommandError('interface not found')

        # DHCP on the new interface
        _, data = self._await_agent_command(
            instance_uuid, f'dhclient {new_interface}')

        # Ensure interface picked up the right address
        _, data = self._await_agent_command(
            instance_uuid, f'ip -json -o addr show dev {new_interface}')
        d = json.loads(data)
        if d[0]['addr_info'][0]['local'] != netdesc['ipv4']:
            raise AgentCommandError('wrong address assigned to interface')

    def get_cluster_operation(self, op_type, op_uuid):
        if not self.check_capability('get-cluster-operations'):
            raise IncapableException(
                'The API server version you are talking to does not support '
                'fetching a cluster operation.')
        r = self._request_url('GET', f'/clusteroperations/{op_type}/{op_uuid}')
        return r.json()

    # Terminal cluster operation states. ``complete`` is success; the
    # other three are failure modes that surface as
    # ClusterOperationFailed when ``_poll_cluster_operation`` runs.
    _CLUSTER_OP_TERMINAL_STATES = {
        'complete', 'abort', 'error', 'deleted'}
    _CLUSTER_OP_SUCCESS_STATE = 'complete'

    def _poll_cluster_operation(self, op_type, op_uuid, timeout=None,
                                interval=1):
        """Poll a cluster operation until it reaches a terminal state.

        Returns the operation's final ``external_view`` dict on
        success. Raises :class:`ClusterOperationFailed` if the op
        reaches ``error`` / ``abort`` / ``deleted``, or
        :class:`ClusterOperationTimeout` if the timeout elapses
        first.

        ``timeout`` defaults to the client's async deadline
        (governed by ``async_strategy``); pass a positive number of
        seconds to override.
        """
        if timeout is None:
            timeout = _calculate_async_deadline(self.async_strategy)
        if timeout < 0:
            # ASYNC_CONTINUE means "don't block" -- short-circuit by
            # returning the current view without waiting.
            return self.get_cluster_operation(op_type, op_uuid)

        deadline = time.time() + timeout
        view = None
        while True:
            view = self.get_cluster_operation(op_type, op_uuid)
            state = view.get('state')
            if state in self._CLUSTER_OP_TERMINAL_STATES:
                if state == self._CLUSTER_OP_SUCCESS_STATE:
                    return view
                raise ClusterOperationFailed(
                    f'cluster operation {op_uuid} ended in state {state!r}',
                    op_type, op_uuid, op_view=view)

            if time.time() > deadline:
                raise ClusterOperationTimeout(
                    f'cluster operation {op_uuid} did not reach a terminal '
                    f'state within {timeout} seconds (last state: {state!r})',
                    op_type, op_uuid, op_view=view)

            time.sleep(interval)

    def get_cluster_operation_chain(self, op_uuid):
        """Return the transitive depends_on closure for a cluster op.

        Calls ``GET /clusteroperations/<op_uuid>/chain`` (added in
        phase 7 of the network facade). Returns a list of op summary
        dicts (each carrying ``operation_type``, ``uuid``, ``state``
        and ``tasks``), newest-first by ``created_at``.

        The server raises 404 on an unknown ``op_uuid`` (surfaced as
        :class:`ResourceNotFoundException`).
        """
        if not self.check_capability('cluster-operation-chain'):
            raise IncapableException(
                'The API server version you are talking to does not '
                'support cluster-operation chain discovery.')
        r = self._request_url(
            'GET', f'/clusteroperations/{op_uuid}/chain')
        return r.json()

    def list_cluster_operations_for_target(self, target_object_type,
                                           target_uuid):
        """List cluster operations targeting a given object.

        Calls ``GET /clusteroperations?target_object_type=<type>
        &target_uuid=<uuid>`` (added in phase 7 of the network
        facade). Returns a list of op summary dicts scoped to the
        caller's namespace.
        """
        if not self.check_capability('cluster-operations-by-target'):
            raise IncapableException(
                'The API server version you are talking to does not '
                'support listing cluster operations by target.')
        r = self._request_url(
            'GET', '/clusteroperations',
            data={'target_object_type': target_object_type,
                  'target_uuid': target_uuid})
        return r.json()


VERSION_CACHE = None


def get_user_agent():
    global VERSION_CACHE

    if not VERSION_CACHE:
        try:
            from shakenfist_client import _version
            sf_version = VERSION_CACHE = _version.version
        except ImportError:
            sf_version = VERSION_CACHE = 'unreleased development version'
    else:
        sf_version = VERSION_CACHE

    return f'Mozilla/5.0 (Ubuntu; Linux x86_64) Shaken Fist/{sf_version}'
