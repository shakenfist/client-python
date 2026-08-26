import io
import logging
from unittest import mock

import testtools
from click.testing import CliRunner

from shakenfist_client import main


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
                logger.handlers = handlers
                logger.setLevel(level)
                logger.filters = filters
            module_logger.propagate = propagate

        self.addCleanup(restore)


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
