"""Header sanitisation for the HTTP servers this client stands up.

This module imports nothing beyond the standard library on purpose. The
loopback listener which uses it (``commandline/auth.py``) is the one
part of the client which answers requests rather than making them, and
keeping the sanitisation free of the client's own dependencies means it
can be read, tested and reused without pulling any of them in.
"""


class SafeHeaderMixin:
    """Strip CR and LF from header values before they reach the wire.

    A header value carrying a line break splits the HTTP response
    (CWE-113), so whatever produced the value can inject headers, or a
    body, of its own. The listener here sends only static and
    length-derived values today, which is exactly why the sanitisation
    belongs in a mixin rather than at the call sites: the property being
    kept is that no header value can carry a line break, not that none
    of today's do.

    Inherit this *before* the handler base class. Listed after it, the
    handler's own ``send_header()`` wins the method resolution order and
    nothing here ever runs -- which is indistinguishable at runtime from
    not having the mixin at all.
    """

    def send_header(self, keyword, value):
        value = str(value).replace('\r', '').replace('\n', '')
        super().send_header(keyword, value)
