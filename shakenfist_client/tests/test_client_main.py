import io
import logging
from unittest import mock

import testtools
from click.testing import CliRunner

from shakenfist_client import main


# A structurally valid JWT: three base64url segments, the header being
# base64url('{"alg":"none"}'). The redaction has to key off the shape of
# the string rather than where it came from, so a real shape matters.
JWT = ('eyJhbGciOiJub25lIn0.'
       'eyJzdWIiOiJpbnN0YW5jZSIsImV4cCI6MTc2NzIyNTYwMH0.'
       'c2lnbmF0dXJlLWdvZXMtaGVyZQ')
PROXY_URL = 'https://kerbside.example.com/vdi/console.vv?token=%s' % JWT


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
