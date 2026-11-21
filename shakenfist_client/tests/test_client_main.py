import io
import json
import logging
import os
import shutil
import tempfile
from unittest import mock

import click
import testtools
from click.testing import CliRunner

from shakenfist_client import apiclient
from shakenfist_client import credentials
from shakenfist_client import main
from shakenfist_client.commandline import auth


# A structurally valid JWT: three base64url segments, the header being
# base64url('{"alg":"none"}'). The redaction has to key off the shape of
# the string rather than where it came from, so a real shape matters.
JWT = ('eyJhbGciOiJub25lIn0.'
       'eyJzdWIiOiJpbnN0YW5jZSIsImV4cCI6MTc2NzIyNTYwMH0.'
       'c2lnbmF0dXJlLWdvZXMtaGVyZQ')
PROXY_URL = 'https://kerbside.example.com/vdi/console.vv?token=%s' % JWT
CLAIMS_URL = 'http://localhost:13000/auth/namespaces/testspace/claims'


class LoggingStateTestCase(testtools.TestCase):
    """Base class which puts the process-wide logging state back.

    configure_logging() reconfigures the root logger for the whole
    process, so every test here has to hand the interpreter back what it
    was given or the next test -- and stestr's own output -- inherits it.
    """

    def setUp(self):
        super().setUp()

        root = logging.root
        module_logger = logging.getLogger(main.__name__)
        saved = [
            (root, list(root.handlers), root.level, list(root.filters)),
            (module_logger, list(module_logger.handlers),
             module_logger.level, list(module_logger.filters)),
        ]
        propagate = module_logger.propagate

        def restore():
            for logger, handlers, level, filters in saved:
                for handler in list(logger.handlers):
                    handler.removeFilter(main.REDACT_TOKENS)
                logger.handlers = handlers
                logger.setLevel(level)
                logger.filters = filters
            module_logger.propagate = propagate

        self.addCleanup(restore)


class RedactTokensFilterTestCase(testtools.TestCase):
    def _record(self, msg, *args):
        return logging.LogRecord(
            'test', logging.DEBUG, __file__, 1, msg, args or None, None)

    def _filtered(self, msg, *args):
        record = self._record(msg, *args)
        self.assertTrue(main.REDACT_TOKENS.filter(record))
        return record.getMessage()

    def test_jwt_in_url_is_redacted(self):
        message = self._filtered('fetching %s', PROXY_URL)
        self.assertNotIn(JWT, message)
        self.assertIn('*****', message)
        # The rest of the URL survives, because knowing which host was
        # called is the reason the line is being printed at all.
        self.assertIn('kerbside.example.com', message)

    def test_urllib3_shaped_record_is_redacted(self):
        # urllib3.connectionpool logs the request target, query string
        # and all, using exactly this format string.
        message = self._filtered(
            '%s://%s:%s "%s %s %s" %s %s', 'https', 'kerbside.example.com',
            443, 'GET', '/vdi/console.vv?token=%s' % JWT, 'HTTP/1.1', 200, 412)
        self.assertNotIn(JWT, message)

    def test_bare_jwt_is_redacted(self):
        self.assertEqual('*****', self._filtered(JWT))

    def test_percent_signs_survive_redaction(self):
        # Dropping args without also folding them into msg would leave
        # getMessage() trying to interpolate the message a second time.
        message = self._filtered('%s is 50%% done', PROXY_URL)
        self.assertEqual(
            'https://kerbside.example.com/vdi/console.vv?token=***** '
            'is 50% done', message)

    def test_hostname_is_not_redacted(self):
        # Three dotted segments of base64url characters, which a shape
        # test looser than "starts with eyJ" would eat.
        message = self._filtered('connecting to %s', 'mycluster.mycompany.internal')
        self.assertIn('mycluster.mycompany.internal', message)

    def test_ordinary_message_is_untouched(self):
        record = self._record('Client for %s constructed', 'http://sf/api')
        main.REDACT_TOKENS.filter(record)
        self.assertEqual('Client for %s constructed', record.msg)
        self.assertEqual(('http://sf/api',), record.args)


class ConfigureLoggingTestCase(LoggingStateTestCase):
    def test_root_gets_a_handler(self):
        logging.root.handlers = []

        main.configure_logging()

        self.assertNotEqual([], logging.root.handlers)

    def test_our_logger_does_not_propagate(self):
        # Without this our records reach both setup_console()'s handler
        # and root's, and are printed twice.
        module_logger = logging.getLogger(main.__name__)
        module_logger.propagate = True

        main.configure_logging()

        self.assertFalse(module_logger.propagate)

    def test_filter_installed_on_both_handler_sets(self):
        # Records take two routes out of sf-client: our own go to the
        # handler setup_console() installed, everything else goes to
        # root's. Redaction has to be on both.
        logging.root.handlers = []

        main.configure_logging()

        for handler in (logging.root.handlers +
                        logging.getLogger(main.__name__).handlers):
            self.assertIn(main.REDACT_TOKENS, handler.filters)

    def test_root_level_is_set_even_when_a_handler_exists(self):
        # basicConfig() puts its setLevel() inside "if root has no
        # handlers", so a handler attached before us -- by a plugin, or
        # by a dependency calling basicConfig itself -- would otherwise
        # leave root at whatever level it was given.
        logging.root.handlers = [logging.NullHandler()]
        logging.root.setLevel(logging.WARNING)

        main.configure_logging()

        self.assertEqual(logging.INFO, logging.root.level)

    def test_configuring_twice_does_not_stack_filters(self):
        logging.root.handlers = []

        main.configure_logging()
        main.configure_logging()

        for handler in logging.root.handlers:
            self.assertEqual(
                1, handler.filters.count(main.REDACT_TOKENS))


class VerboseTestCase(LoggingStateTestCase):
    def _invoke(self, args):
        stream = io.StringIO()
        with mock.patch.object(main.apiclient, 'Client'):
            with mock.patch.object(logging, 'basicConfig') as basic_config:
                # Stand in for basicConfig so the captured stream is a
                # root handler like the real one, rather than whatever
                # the test runner left behind.
                def install(**kwargs):
                    handler = logging.StreamHandler(stream)
                    handler.setFormatter(
                        logging.Formatter(kwargs.get('format')))
                    logging.root.handlers = [handler]
                    logging.root.setLevel(kwargs.get('level', logging.INFO))
                basic_config.side_effect = install

                result = CliRunner().invoke(
                    main.cli, args, catch_exceptions=False)
        return result, stream

    def test_verbose_raises_urllib3(self):
        # The point of moving root: "why did that call fail" is usually
        # answered by the HTTP stack, not by us.
        self._invoke(['--verbose', 'version'])

        self.assertEqual(
            logging.DEBUG,
            logging.getLogger('urllib3.connectionpool').getEffectiveLevel())

    def test_default_does_not_raise_urllib3(self):
        self._invoke(['version'])

        self.assertEqual(
            logging.INFO,
            logging.getLogger('urllib3.connectionpool').getEffectiveLevel())

    def test_verbose_does_not_print_console_tokens(self):
        # The end to end property: with --verbose in force, a record
        # carrying a Kerbside console URL reaches the terminal with the
        # JWT removed, whichever library logged it.
        _, stream = self._invoke(['--verbose', 'version'])

        logging.getLogger('urllib3.connectionpool').debug(
            '%s://%s:%s "%s %s %s" %s %s', 'https', 'kerbside.example.com',
            443, 'GET', '/vdi/console.vv?token=%s' % JWT, 'HTTP/1.1', 200, 412)

        printed = stream.getvalue()
        self.assertIn('kerbside.example.com', printed)
        self.assertNotIn(JWT, printed)


class GroupCatchExceptionsTestCase(testtools.TestCase):
    """Tests for the CLI's translation of APIExceptions into messages.

    A subclass which escapes this handler reaches the user as a Python
    traceback, which is why each of these asserts the clean exit as well
    as the message: exit code 1 and no traceback is the whole contract.
    """

    def _run(self, exception):
        @click.group(cls=main.GroupCatchExceptions)
        def cli():
            pass

        @cli.command(name='boom')
        def boom():
            raise exception

        with mock.patch.object(main, 'LOG') as log:
            self.assertRaises(SystemExit, cli, ['boom'],
                              standalone_mode=False)
        return log

    def test_service_unavailable_is_reported_and_asks_for_a_retry(self):
        # 503 is a routine answer from the namespace claims API -- the
        # capacity accounting is still building, or the claim was
        # contended -- so it has to read as "try again", not as a fault.
        log = self._run(apiclient.ServiceUnavailableException(
            'API request failed', 'POST', CLAIMS_URL, 503,
            '{"error": "the cluster capacity accounting is not available '
            'yet, please retry"}'))

        log.error.assert_called_once()
        message = log.error.call_args[0][0]
        self.assertIn('Service unavailable, please retry', message)
        self.assertIn('capacity accounting is not available yet', message)

    def test_insufficient_resources_is_still_reported(self):
        # The neighbouring handler, so that adding a case above it did
        # not shadow the one which was already there.
        log = self._run(apiclient.InsufficientResourcesException(
            'API request failed', 'POST', CLAIMS_URL, 507,
            '{"error": "the cluster does not have the capacity"}'))

        log.error.assert_called_once()
        self.assertIn('Insufficient Resources', log.error.call_args[0][0])

    def test_agent_operation_failed_is_reported_not_traced(self):
        # instance execute, upload and download call the creating helpers
        # directly, and those raise as soon as the operation reaches a
        # terminal failure state rather than handing back an in flight
        # operation. "expired" makes that a routine outcome once the server
        # side deadlines deploy, so it has to read as an error message and
        # exit 1 like everything else here, not as a traceback.
        log = self._run(apiclient.AgentOperationFailed(
            'Agent execute operation op1 on instance i1 entered terminal '
            'state "expired"', 'op1', {'uuid': 'op1', 'state': 'expired'}))

        log.error.assert_called_once()
        message = log.error.call_args[0][0]
        self.assertIn('Agent operation failed', message)
        self.assertIn('expired', message)

    def test_an_unmapped_api_exception_is_not_swallowed(self):
        # There is no bare APIException fallback, deliberately: a status
        # nobody has taught the CLI about should be loud rather than
        # reported as some other kind of failure.
        @click.group(cls=main.GroupCatchExceptions)
        def cli():
            pass

        @cli.command(name='boom')
        def boom():
            raise apiclient.APIException(
                'API request failed', 'GET', CLAIMS_URL, 418, 'teapot')

        self.assertRaises(apiclient.APIException, cli, ['boom'],
                          standalone_mode=False)


class UnconfiguredHostTestCase(LoggingStateTestCase):
    """What happens on a host which has told this client nothing.

    That is the normal state for the first command a new user ever runs,
    and the group callback resolves a credential before any verb runs. So
    this is about which verbs survive having nothing to resolve, and what
    the ones which do not say about it.
    """

    # Deliberately unconvincing, and not shaped like the thing it stands
    # in for: a credential scanner runs over this repository's history
    # with a positive control.
    CACHED_KEY = 'not-a-cached-key-1'
    CLUSTER = 'http://localhost:13000'

    def setUp(self):
        super().setUp()

        # No configuration file on the machine running the tests.
        mock.patch('shakenfist_client.apiclient.os.path.exists',
                   return_value=False).start()

        # No credential cache on it either. The cache module is asked
        # where its file is rather than being told, so that one function
        # is the only seam a test needs.
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir, True)
        self.cache_path = os.path.join(self.tempdir, 'shakenfist',
                                       'credentials.json')
        mock.patch('shakenfist_client.credentials.credentials_path',
                   side_effect=lambda path=None: path or self.cache_path).start()

        # And nothing in the environment, which click reads for the group
        # options as well as apiclient reading it for a library caller.
        environ = {k: v for k, v in os.environ.items()
                   if not k.startswith('SHAKENFIST_')}
        mock.patch.dict(os.environ, environ, clear=True).start()

        self.addCleanup(mock.patch.stopall)

    def _write_cache(self, mode=0o600, content=None):
        os.makedirs(os.path.dirname(self.cache_path), mode=0o700,
                    exist_ok=True)
        if content is None:
            content = json.dumps({
                'version': 1,
                'credentials': [{'apiurl': self.CLUSTER, 'namespace': 'ci',
                                 'key_name': 'corporate-aaaa',
                                 'key': self.CACHED_KEY, 'expiry': None,
                                 'scopes': []}],
                'current': None})
        with open(self.cache_path, 'w') as f:
            f.write(content)
        os.chmod(self.cache_path, mode)

    def _run(self, args):
        """Call the group the way a console script does.

        CliRunner invokes main() directly, which goes straight past the
        exception handling the group installs in __call__, so the verbs
        which end in a message rather than a traceback have to be driven
        like this.
        """
        with mock.patch.object(main, 'LOG') as log:
            exit_code = 0
            try:
                main.cli(args, standalone_mode=False)
            except SystemExit as e:
                exit_code = e.code
        return exit_code, log

    def test_an_ordinary_verb_says_what_is_missing(self):
        # It was a traceback until logging in made "nothing configured
        # yet" an ordinary starting state rather than a mistake.
        exit_code, log = self._run(['namespace', 'list'])

        self.assertEqual(1, exit_code)
        log.error.assert_called_once()
        message = log.error.call_args[0][0]
        self.assertIn('Not configured', message)
        self.assertIn('--apiurl', message)
        self.assertIn('sf-client login', message)

    def test_login_runs_and_asks_to_be_told_the_cluster(self):
        # The whole point: a login must be runnable on a host which has
        # nothing, so it cannot be refused for having nothing. What it
        # does need is somewhere to log in to.
        result = CliRunner().invoke(main.cli, ['login'],
                                    catch_exceptions=False)

        self.assertEqual(1, result.exit_code)
        self.assertIn('--apiurl', result.output)
        self.assertIn('SHAKENFIST_API_URL', result.output)

    def test_logout_runs_on_a_host_with_nothing(self):
        result = CliRunner().invoke(main.cli, ['logout'],
                                    catch_exceptions=False)

        self.assertEqual(0, result.exit_code)
        self.assertIn('no cached credentials', result.output)

    def test_an_unreadable_cache_breaks_an_ordinary_verb_with_a_message(self):
        # A cache which cannot be read is a refusal rather than something
        # to pass over, so it takes down every verb which needs a
        # credential. The message has to name the verb which clears it.
        self._write_cache(mode=0o644)

        exit_code, log = self._run(['namespace', 'list'])

        self.assertEqual(1, exit_code)
        message = log.error.call_args[0][0]
        self.assertIn(self.cache_path, message)
        self.assertIn('sf-client logout', message)
        self.assertNotIn(self.CACHED_KEY, message)

    def test_the_logout_advice_is_not_said_twice(self):
        # Several of the cache's own messages name the verb already,
        # because the module which raises them knows which refusals a
        # chmod can fix. The handler must not append a second copy.
        self._write_cache(mode=0o644)

        _, log = self._run(['namespace', 'list'])

        self.assertEqual(1, log.error.call_args[0][0].count('sf-client logout'))

    def test_a_cache_from_a_newer_client_is_also_a_message(self):
        # Not every unusable cache may be replaced: one written by a
        # newer client may hold credentials in fields this one cannot
        # see. That refusal is a different exception class from the
        # repairable ones, so a handler which caught only those would
        # let this reach the terminal as a traceback.
        self._write_cache(content=json.dumps(
            {'version': 99, 'credentials': [], 'current': None}))

        exit_code, log = self._run(['namespace', 'list'])

        self.assertEqual(1, exit_code)
        message = log.error.call_args[0][0]
        self.assertIn('newer client', message)
        self.assertIn(self.cache_path, message)

    def test_the_advice_is_added_where_the_message_lacks_it(self):
        self._write_cache(content='{"version": 1}')

        _, log = self._run(['namespace', 'list'])

        message = log.error.call_args[0][0]
        self.assertIn('holds no credential list', message)
        self.assertIn('sf-client logout', message)

    def test_logout_still_runs_when_the_cache_cannot_be_read(self):
        # The state logout exists to clear is the state in which every
        # other verb refuses, so this is the one which must not need a
        # client.
        self._write_cache(mode=0o644)

        result = CliRunner().invoke(main.cli, ['logout'],
                                    catch_exceptions=False)

        self.assertEqual(0, result.exit_code)
        self.assertFalse(os.path.exists(self.cache_path))
        self.assertNotIn(self.CACHED_KEY, result.output)


class ClientConstructionTestCase(LoggingStateTestCase):
    """Which verbs the group builds an API client for.

    Building one resolves a credential and reaches the cluster for its
    capability list, and the two login verbs must do neither: one runs
    before there is a credential, and the other runs when the credential
    there is cannot be read.
    """

    CLUSTER = 'http://localhost:13000'

    def setUp(self):
        super().setUp()
        self.construct = mock.patch(
            'shakenfist_client.apiclient.Client').start()
        self.addCleanup(mock.patch.stopall)

    def _invoke(self, args):
        return CliRunner().invoke(main.cli, args, catch_exceptions=False)

    def test_an_ordinary_verb_gets_a_client_from_the_group(self):
        # A verb which actually talks to the cluster. This used to be
        # `version`, which does not and no longer gets a client at all,
        # so it could no longer demonstrate what this test is about.
        self._invoke(['--apiurl', self.CLUSTER, 'node', 'list'])

        self.construct.assert_called_once_with(
            namespace=None, key=None, base_url=self.CLUSTER, logger=main.LOG,
            async_strategy='pause')

    def test_logout_gets_no_client_at_all(self):
        with mock.patch.object(credentials, 'delete_credentials',
                               return_value=0):
            result = self._invoke(['--apiurl', self.CLUSTER, 'logout'])

        self.assertEqual(0, result.exit_code)
        self.construct.assert_not_called()

    def test_the_credential_verbs_get_no_client_at_all(self):
        # Showing and choosing among what is cached is local, and has to
        # work where the cluster cannot be reached.
        for verb in ('list', 'select'):
            args = ['credential', verb] + (['ci'] if verb == 'select' else ['--offline'])
            self._invoke(['--apiurl', self.CLUSTER] + args)

        self.construct.assert_not_called()

    def test_login_builds_its_own_client_from_the_api_url(self):
        # Not the group's call, which passes a namespace, a key and a
        # logger: this one skips the credential resolution altogether.
        self.construct.return_value.get_login_issuers.return_value = []

        result = self._invoke(['--apiurl', self.CLUSTER, 'login'])

        self.assertEqual(1, result.exit_code)
        self.construct.assert_called_once_with(
            base_url=self.CLUSTER, verbose=False,
            suppress_configuration_lookup=True)

    def test_the_login_verbs_are_registered(self):
        self.assertIn('login', main.cli.commands)
        self.assertIn('logout', main.cli.commands)
        self.assertIn('credential', main.cli.commands)
        self.assertEqual(frozenset(['login', 'logout', 'credential']),
                         auth.COMMANDS_WITHOUT_A_CLIENT)

    def test_version_needs_no_cluster(self):
        # It answers from the installed package and never touches one.
        # Resolving a credential for it meant it failed on a host which
        # has none -- the state a fresh install is in, and the one
        # somebody is most likely to be asking a version question from.
        result = self._invoke(['version'])

        self.assertEqual(0, result.exit_code)
        self.construct.assert_not_called()

    def test_version_still_answers_with_no_configuration_at_all(self):
        # The end to end version of the above, through the failure it
        # used to produce.
        self.construct.side_effect = apiclient.UnconfiguredException(
            'You have not specified the server to communicate with')

        result = self._invoke(['version'])

        self.assertEqual(0, result.exit_code)
        self.assertIn('shaken fist', result.output.lower())

    def test_the_client_free_set_is_the_login_verbs_and_version(self):
        # Two sets unioned rather than one, because
        # COMMANDS_WITHOUT_A_CLIENT is documented as the verbs which
        # cannot resolve a credential, and version simply has no use for
        # one.
        self.assertEqual(
            frozenset(['login', 'logout', 'credential', 'version']),
            main.CLIENT_FREE_COMMANDS)

    def test_the_group_passes_on_the_cluster_a_login_needs(self):
        # The verb has to be told which cluster was named, because it is
        # building the client the group did not.
        client = mock.MagicMock()
        client.get_login_issuers.return_value = []

        with mock.patch.object(auth, '_client_for_login',
                               return_value=client) as built:
            self._invoke(['--apiurl', self.CLUSTER, 'login'])

        built.assert_called_once_with(self.CLUSTER, verbose=False)

    def test_the_group_passes_on_the_namespace_a_login_selects_with(self):
        client = mock.MagicMock()
        client.base_url = self.CLUSTER
        client.get_login_issuers.return_value = [
            {'name': 'corporate', 'issuer_url': 'https://idp.example.com/',
             'client_id': 'shakenfist', 'oidc_scopes': 'openid',
             'device_authorization_endpoint': 'https://idp.example.com/device',
             'token_endpoint': 'https://idp.example.com/token'}]
        client.check_capability.return_value = True
        client.login.return_value = [{'namespace': 'ci'}]

        with mock.patch.object(auth, '_client_for_login',
                               return_value=client):
            with mock.patch.object(auth, 'device_authorization_login',
                                   return_value='not-an-identity-token-1'):
                with mock.patch.object(
                        auth, '_store',
                        return_value={'credentials': [],
                                      'current': None}) as store:
                    self._invoke(['--apiurl', self.CLUSTER, '--namespace',
                                  'ci', 'login'])

        store.assert_called_once_with(self.CLUSTER, [{'namespace': 'ci'}], 'ci')
