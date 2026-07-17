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


def _minimal_instance():
    """The smallest instance dict _show_instance will print in simple
    output mode."""
    return {
        'uuid': 'fake-uuid',
        'name': 'testinst',
        'namespace': 'system',
        'cpus': 1,
        'memory': 1024,
        'disk_spec': [{'type': 'disk', 'bus': None, 'size': 8,
                       'base': 'debian:13'}],
        'video': {'model': 'cirrus', 'memory': 16384, 'vdi': 'spice'},
        'ssh_key': None,
        'user_data': None,
        'side_channels': [],
        'state': 'created',
    }


class CreateSideChannelsTestCase(testtools.TestCase):
    """Tests for the side channel arguments to ``sf-client instance create``.

    The API treats side_channels=None as "apply the server's default set"
    and [] as "explicitly no side channels". click's multiple=True options
    default to an empty tuple, which previously reached the API as [] and
    silently disabled the in-guest agent for every instance created
    without an explicit --side-channel flag.
    """

    def _invoke(self, extra_args):
        client = mock.Mock()
        client.create_instance.return_value = _minimal_instance()
        client.get_instance_metadata.return_value = {}
        client.get_instance_interfaces.return_value = []
        runner = CliRunner()
        result = runner.invoke(
            instance_cmd.instance,
            ['create', 'testinst', '1', '1024', '-d', '8@debian:13'] +
            extra_args,
            obj={'CLIENT': client, 'OUTPUT': 'simple'},
            catch_exceptions=False)
        return result, client

    def test_unspecified_side_channels_sends_none(self):
        result, client = self._invoke([])
        self.assertEqual(0, result.exit_code, result.output)
        kwargs = client.create_instance.call_args.kwargs
        self.assertIsNone(kwargs['side_channels'])

    def test_explicit_side_channel_is_passed(self):
        result, client = self._invoke(['-s', 'sf-agent2'])
        self.assertEqual(0, result.exit_code, result.output)
        kwargs = client.create_instance.call_args.kwargs
        self.assertEqual(['sf-agent2'], kwargs['side_channels'])

    def test_multiple_side_channels_are_passed(self):
        result, client = self._invoke(
            ['-s', 'sf-agent', '-s', 'sf-agent2'])
        self.assertEqual(0, result.exit_code, result.output)
        kwargs = client.create_instance.call_args.kwargs
        self.assertEqual(
            ['sf-agent', 'sf-agent2'], kwargs['side_channels'])

    def test_no_side_channels_flag_sends_empty_list(self):
        result, client = self._invoke(['--no-side-channels'])
        self.assertEqual(0, result.exit_code, result.output)
        kwargs = client.create_instance.call_args.kwargs
        self.assertEqual([], kwargs['side_channels'])

    def test_conflicting_side_channel_flags_refused(self):
        result, client = self._invoke(
            ['-s', 'sf-agent2', '--no-side-channels'])
        client.create_instance.assert_not_called()
        self.assertIn('cannot specify both', result.output)
