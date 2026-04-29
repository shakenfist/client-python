import io
from unittest import mock

import testtools
from click.testing import CliRunner

from shakenfist_client.commandline import instance as instance_cmd


class ConsoleDataCommandTestCase(testtools.TestCase):
    """Tests for ``sf-client instance consoledata``.

    CliRunner replaces ``sys.stdout`` for output capture, which would
    override any patch we apply to the real ``sys.stdout.isatty``. To
    decouple the tests from the runner's stream, we instead replace the
    ``sys`` *module reference* inside the command module with a fake; the
    production code reaches stdout via that reference, so it sees what we
    want it to see regardless of what the runner does to the real stdout.
    """

    def _fake_sys(self, isatty, buffer):
        fake = mock.MagicMock()
        fake.stdout.isatty.return_value = isatty
        fake.stdout.buffer = buffer
        return fake

    def _invoke(self, mock_client, isatty):
        buffer = io.BytesIO()
        with mock.patch.object(
                instance_cmd, 'sys', self._fake_sys(isatty, buffer)):
            runner = CliRunner()
            result = runner.invoke(
                instance_cmd.instance,
                ['consoledata', 'inst-ref', '100'],
                obj={'CLIENT': mock_client, 'OUTPUT': 'pretty'},
                catch_exceptions=False)
        return result, buffer.getvalue()

    def test_requests_raw_bytes_via_decode_none(self):
        client = mock.Mock()
        client.get_console_data.return_value = b'plain bytes'

        self._invoke(client, isatty=False)

        client.get_console_data.assert_called_once_with(
            'inst-ref', length=100, decode=None)

    def test_non_tty_writes_raw_bytes_unsanitized(self):
        # Bytes containing a CSI colour sequence and a NUL byte must be
        # written byte-for-byte when stdout is a pipe -- preserving
        # fidelity for `sf-client ... consoledata > file`.
        client = mock.Mock()
        raw = b'\x1b[31mred\x1b[0m\x00'
        client.get_console_data.return_value = raw

        result, written = self._invoke(client, isatty=False)

        self.assertEqual(0, result.exit_code)
        self.assertEqual(raw, written)

    def test_tty_strips_escape_and_control_bytes(self):
        client = mock.Mock()
        client.get_console_data.return_value = (
            b'\x1b[31mred\x1b[0m\x00 ok\n')

        result, written = self._invoke(client, isatty=True)

        self.assertEqual(0, result.exit_code)
        # CSI sequences and NUL stripped; printable text and newline kept.
        self.assertEqual(b'red ok\n', written)
