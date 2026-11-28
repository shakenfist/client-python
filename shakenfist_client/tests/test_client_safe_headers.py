from http.server import BaseHTTPRequestHandler

import testtools

from shakenfist_client.commandline import auth
from shakenfist_client.safe_headers import SafeHeaderMixin


class _Recorder:
    """The base class position in a handler, with send_header recorded."""

    def __init__(self):
        self.sent = []

    def send_header(self, keyword, value):
        self.sent.append((keyword, value))


class _Handler(SafeHeaderMixin, _Recorder):
    pass


class SafeHeaderMixinTestCase(testtools.TestCase):
    def test_plain_value_is_unchanged(self):
        handler = _Handler()

        handler.send_header('Content-Type', 'text/html; charset=utf-8')

        self.assertEqual([('Content-Type', 'text/html; charset=utf-8')],
                         handler.sent)

    def test_carriage_return_and_newline_are_stripped(self):
        handler = _Handler()

        handler.send_header('Location', 'http://host/\r\nX-Injected: yes')

        self.assertEqual([('Location', 'http://host/X-Injected: yes')],
                         handler.sent)

    def test_bare_newline_is_stripped(self):
        handler = _Handler()

        handler.send_header('X-Thing', 'a\nb')

        self.assertEqual([('X-Thing', 'ab')], handler.sent)

    def test_non_string_value_is_accepted(self):
        # send_header's callers pass a length as a string today, but the
        # mixin must not be the thing that breaks if one ever does not.
        handler = _Handler()

        handler.send_header('Content-Length', 12)

        self.assertEqual([('Content-Length', '12')], handler.sent)


class RedirectHandlerBasesTestCase(testtools.TestCase):
    def test_mixin_precedes_the_handler_base(self):
        # Listed after BaseHTTPRequestHandler the mixin is in the bases
        # and never runs, which looks identical to a handler which never
        # had it. Assert the order rather than the membership.
        bases = auth._RedirectHandler.__mro__

        self.assertIn(SafeHeaderMixin, bases)
        self.assertIn(BaseHTTPRequestHandler, bases)
        self.assertLess(bases.index(SafeHeaderMixin),
                        bases.index(BaseHTTPRequestHandler))
