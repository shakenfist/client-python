import io
import os
import tempfile
from unittest import mock

import testtools
from click.testing import CliRunner

from shakenfist_client import util
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


class VdiConsoleCommandTestCase(testtools.TestCase):
    """Tests for ``sf-client instance vdiconsole``.

    The command chooses between the Kerbside proxy path and the direct
    path (from ``check_capability``/``--direct``) and between the ``ryll``
    and ``remote-viewer`` executables (from ``shutil.which``/``--viewer``).
    We mock ``shutil.which``, ``subprocess.run``, ``tempfile.mkstemp`` and
    the client methods so no real viewer is launched and no real file is
    needed.
    """

    def _which(self, present):
        """Return a fake ``shutil.which`` resolving names in ``present``.

        ``present`` maps a viewer name to its absolute path; anything else
        resolves to ``None`` (not on PATH).
        """
        def which(name):
            return present.get(name)
        return which

    def _mkstemp(self):
        """Return a real ``tempfile.mkstemp`` result so the write/unlink in
        the temp-file branches operate on an actual (throwaway) file, while
        recording the name so tests can assert on it."""
        handle, name = self._real_mkstemp()
        self._temp_names.append(name)
        self.addCleanup(
            lambda: os.path.exists(name) and os.unlink(name))
        return handle, name

    def _invoke(self, client, args, which_present, verbose=False):
        self._temp_names = []
        self._real_mkstemp = tempfile.mkstemp
        run = mock.Mock(return_value=mock.Mock(returncode=0))
        mkstemp = mock.Mock(side_effect=self._mkstemp)
        with mock.patch.object(
                instance_cmd.shutil, 'which',
                side_effect=self._which(which_present)) as which, \
            mock.patch.object(
                instance_cmd.subprocess, 'run', run), \
            mock.patch.object(
                instance_cmd.tempfile, 'mkstemp', mkstemp):
            runner = CliRunner()
            result = runner.invoke(
                instance_cmd.instance,
                ['vdiconsole'] + args,
                obj={'CLIENT': client, 'VERBOSE': verbose},
                catch_exceptions=False)
        return result, run, mkstemp, which

    def _client(self, proxy_cap=True, helper_cap=True):
        client = mock.Mock()

        def check_capability(cap):
            if cap == 'vdi-console-proxy':
                return proxy_cap
            if cap == 'vdi-console-helper':
                return helper_cap
            return False
        client.check_capability.side_effect = check_capability
        client.get_vdi_console_proxy.return_value = {
            'url': 'https://kerbside.example/sf-console.vv?token=jwt',
            'expires_at': 1234,
        }
        client.get_vdi_console_proxy_file.return_value = '[proxy vv]'
        client.get_vdi_console_helper.return_value = '[direct vv]'
        return client

    def test_proxy_ryll_uses_url_and_no_tempfile(self):
        # Matrix cell: proxy + ryll. ryll does the token exchange itself,
        # so we pass --url and never mint/fetch or write the .vv.
        client = self._client(proxy_cap=True)
        result, run, mkstemp, which = self._invoke(
            client, ['inst-ref'], {'ryll': '/usr/bin/ryll'})

        self.assertEqual(0, result.exit_code, result.output)
        run.assert_called_once_with(
            ['/usr/bin/ryll', '--url',
             'https://kerbside.example/sf-console.vv?token=jwt'])
        mkstemp.assert_not_called()
        client.get_vdi_console_proxy.assert_called_once_with('inst-ref')
        client.get_vdi_console_proxy_file.assert_not_called()
        client.get_vdi_console_helper.assert_not_called()

    def test_proxy_remote_viewer_writes_tempfile(self):
        # Matrix cell: proxy + remote-viewer (ryll absent). We fetch the
        # .vv via the proxy-file convenience, write it to a temp file, and
        # launch remote-viewer against the file.
        client = self._client(proxy_cap=True)
        result, run, mkstemp, which = self._invoke(
            client, ['inst-ref'], {'remote-viewer': '/usr/bin/remote-viewer'})

        self.assertEqual(0, result.exit_code, result.output)
        mkstemp.assert_called_once()
        temp_name = self._temp_names[0]
        run.assert_called_once_with(['/usr/bin/remote-viewer', temp_name])
        client.get_vdi_console_proxy_file.assert_called_once_with('inst-ref')
        client.get_vdi_console_proxy.assert_not_called()
        client.get_vdi_console_helper.assert_not_called()
        # Temp file cleaned up.
        self.assertFalse(os.path.exists(temp_name))

    def test_direct_flag_forces_direct_path(self):
        # Matrix cell: --direct forces the direct-to-hypervisor helper even
        # though the proxy capability is present.
        client = self._client(proxy_cap=True, helper_cap=True)
        result, run, mkstemp, which = self._invoke(
            client, ['--direct', 'inst-ref'], {'ryll': '/usr/bin/ryll'})

        self.assertEqual(0, result.exit_code, result.output)
        mkstemp.assert_called_once()
        temp_name = self._temp_names[0]
        run.assert_called_once_with(['/usr/bin/ryll', '--file', temp_name])
        client.get_vdi_console_helper.assert_called_once_with('inst-ref')
        client.get_vdi_console_proxy.assert_not_called()
        client.get_vdi_console_proxy_file.assert_not_called()
        self.assertFalse(os.path.exists(temp_name))

    def test_direct_when_server_lacks_proxy_capability(self):
        # Matrix cell: no --direct, but the server does not advertise the
        # proxy, so we fall back to the direct helper path.
        client = self._client(proxy_cap=False, helper_cap=True)
        result, run, mkstemp, which = self._invoke(
            client, ['inst-ref'], {'ryll': '/usr/bin/ryll'})

        self.assertEqual(0, result.exit_code, result.output)
        mkstemp.assert_called_once()
        temp_name = self._temp_names[0]
        run.assert_called_once_with(['/usr/bin/ryll', '--file', temp_name])
        client.get_vdi_console_helper.assert_called_once_with('inst-ref')
        client.get_vdi_console_proxy.assert_not_called()
        client.get_vdi_console_proxy_file.assert_not_called()

    def test_viewer_override_is_honoured(self):
        # --viewer forces remote-viewer even when ryll is on PATH.
        client = self._client(proxy_cap=True)
        result, run, mkstemp, which = self._invoke(
            client, ['--viewer', 'remote-viewer', 'inst-ref'],
            {'ryll': '/usr/bin/ryll',
             'remote-viewer': '/usr/bin/remote-viewer'})

        self.assertEqual(0, result.exit_code, result.output)
        temp_name = self._temp_names[0]
        run.assert_called_once_with(['/usr/bin/remote-viewer', temp_name])
        client.get_vdi_console_proxy_file.assert_called_once_with('inst-ref')

    def test_viewer_not_found_exits_nonzero(self):
        # Neither ryll nor remote-viewer resolve on PATH.
        client = self._client(proxy_cap=True)
        result, run, mkstemp, which = self._invoke(
            client, ['inst-ref'], {})

        self.assertEqual(1, result.exit_code)
        self.assertIn('not found on PATH', result.output)
        run.assert_not_called()
        mkstemp.assert_not_called()

    def test_explicit_viewer_not_found_exits_nonzero(self):
        # An explicit --viewer that does not resolve also errors out.
        client = self._client(proxy_cap=True)
        result, run, mkstemp, which = self._invoke(
            client, ['--viewer', 'nope', 'inst-ref'],
            {'ryll': '/usr/bin/ryll'})

        self.assertEqual(1, result.exit_code)
        self.assertIn("Viewer 'nope' not found", result.output)
        run.assert_not_called()

    def test_ryll_gets_no_debug_when_verbose(self):
        # Verbose must not add --debug to a ryll launch (not ryll's flag).
        client = self._client(proxy_cap=False, helper_cap=True)
        result, run, mkstemp, which = self._invoke(
            client, ['inst-ref'], {'ryll': '/usr/bin/ryll'}, verbose=True)

        self.assertEqual(0, result.exit_code, result.output)
        temp_name = self._temp_names[0]
        run.assert_called_once_with(['/usr/bin/ryll', '--file', temp_name])
        self.assertNotIn('--debug', run.call_args.args[0])

    def test_remote_viewer_gets_debug_when_verbose(self):
        # Verbose adds --debug for remote-viewer, before the temp file.
        client = self._client(proxy_cap=False, helper_cap=True)
        result, run, mkstemp, which = self._invoke(
            client, ['inst-ref'],
            {'remote-viewer': '/usr/bin/remote-viewer'}, verbose=True)

        self.assertEqual(0, result.exit_code, result.output)
        temp_name = self._temp_names[0]
        run.assert_called_once_with(
            ['/usr/bin/remote-viewer', '--debug', temp_name])


class VdiConsoleFileCommandTestCase(testtools.TestCase):
    """Tests for ``sf-client instance vdiconsolefile`` (decision 5)."""

    def _invoke(self, client, args):
        runner = CliRunner()
        result = runner.invoke(
            instance_cmd.instance,
            ['vdiconsolefile'] + args,
            obj={'CLIENT': client, 'VERBOSE': False},
            catch_exceptions=False)
        return result

    def _client(self, proxy_cap=True, helper_cap=True):
        client = mock.Mock()

        def check_capability(cap):
            if cap == 'vdi-console-proxy':
                return proxy_cap
            if cap == 'vdi-console-helper':
                return helper_cap
            return False
        client.check_capability.side_effect = check_capability
        client.get_vdi_console_proxy_file.return_value = '[proxy vv]'
        client.get_vdi_console_helper.return_value = '[direct vv]'
        return client

    def test_proxy_path_prints_proxy_file(self):
        client = self._client(proxy_cap=True)
        result = self._invoke(client, ['inst-ref'])

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('[proxy vv]', result.output)
        client.get_vdi_console_proxy_file.assert_called_once_with('inst-ref')
        client.get_vdi_console_helper.assert_not_called()

    def test_direct_flag_prints_helper(self):
        client = self._client(proxy_cap=True, helper_cap=True)
        result = self._invoke(client, ['--direct', 'inst-ref'])

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('[direct vv]', result.output)
        client.get_vdi_console_helper.assert_called_once_with('inst-ref')
        client.get_vdi_console_proxy_file.assert_not_called()

    def test_no_proxy_capability_prints_helper(self):
        client = self._client(proxy_cap=False, helper_cap=True)
        result = self._invoke(client, ['inst-ref'])

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('[direct vv]', result.output)
        client.get_vdi_console_helper.assert_called_once_with('inst-ref')
        client.get_vdi_console_proxy_file.assert_not_called()

    def test_no_capabilities_exits_nonzero(self):
        client = self._client(proxy_cap=False, helper_cap=False)
        result = self._invoke(client, ['inst-ref'])

        self.assertEqual(1, result.exit_code)
        self.assertIn('does not implement VDI console helpers', result.output)


class ExecuteDeadlineOptionTestCase(testtools.TestCase):
    """Tests for ``--deadline`` on ``sf-client instance execute``."""

    def _invoke(self, extra_args, capable=True):
        client = mock.Mock()
        client.check_capability.return_value = capable
        client.instance_execute.return_value = {'results': {'0': {
            'return-code': 0, 'stdout': '', 'stderr': ''}}}
        runner = CliRunner()
        result = runner.invoke(
            instance_cmd.instance,
            ['execute', 'inst-ref', 'true'] + extra_args,
            obj={'CLIENT': client, 'OUTPUT': 'pretty'},
            catch_exceptions=False)
        return result, client

    def test_unspecified_deadline_sends_none(self):
        result, client = self._invoke([])
        self.assertEqual(0, result.exit_code, result.output)
        client.instance_execute.assert_called_once_with(
            'inst-ref', 'true', deadline_seconds=None)

    def test_explicit_deadline_is_passed(self):
        result, client = self._invoke(['--deadline', '30'])
        self.assertEqual(0, result.exit_code, result.output)
        client.instance_execute.assert_called_once_with(
            'inst-ref', 'true', deadline_seconds=30)

    def test_zero_deadline_is_passed_not_dropped(self):
        # 0 means "no wall clock deadline at all" to the server, so it must
        # survive the client's `is not None` tests all the way to the wire.
        result, client = self._invoke(['--deadline', '0'])
        self.assertEqual(0, result.exit_code, result.output)
        client.instance_execute.assert_called_once_with(
            'inst-ref', 'true', deadline_seconds=0)

    def test_explicit_deadline_warns_when_server_is_incapable(self):
        # An omitted flag means "whatever the server does by default", which
        # is exactly what an old server gives, so silence is right there. A
        # value the user typed is a request, and dropping it silently hands
        # them a budget they did not ask for with no clue why.
        result, _ = self._invoke(['--deadline', '30'], capable=False)
        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('does not support --deadline', result.output)

    def test_omitted_deadline_does_not_warn_when_server_is_incapable(self):
        result, _ = self._invoke([], capable=False)
        self.assertEqual(0, result.exit_code, result.output)
        self.assertNotIn('does not support', result.output)

    def test_no_progress_timeout_option(self):
        result = CliRunner().invoke(
            instance_cmd.instance, ['execute', '--help'],
            obj={'CLIENT': mock.Mock(), 'OUTPUT': 'pretty'})
        self.assertEqual(0, result.exit_code, result.output)
        self.assertNotIn('--progress-timeout', result.output)


class UploadDeadlineOptionsTestCase(testtools.TestCase):
    """Tests for ``--deadline`` and ``--progress-timeout`` on
    ``sf-client instance upload``.
    """

    def _invoke(self, extra_args, capable=True, recycle=False):
        """Drive ``instance upload`` against a client with known capabilities.

        ``check_capability`` is answered per capability rather than with one
        blanket value, because the two this command consults are
        independent: ``blob-search-by-hash`` decides whether the checksum
        shortcut runs, and ``agentoperation-deadlines`` decides whether the
        timing flags reach the wire. A single ``return_value`` ties them
        together and leaves half of that matrix untested.
        """
        capabilities = {'blob-search-by-hash': recycle,
                        'agentoperation-deadlines': capable}
        client = mock.Mock()
        client.check_capability.side_effect = lambda name: capabilities[name]
        client.blob_artifact.return_value = {
            'uuid': 'art-uuid', 'blob_uuid': 'blob-uuid'}
        runner = CliRunner()
        with tempfile.NamedTemporaryFile() as source:
            with mock.patch.object(
                    util, 'upload_artifact_with_progress',
                    return_value={'uuid': 'art-uuid', 'blob_uuid': 'blob-uuid'}), \
                    mock.patch.object(
                        util, 'checksum_with_progress',
                        return_value={'uuid': 'blob-uuid'}):
                result = runner.invoke(
                    instance_cmd.instance,
                    ['upload', 'inst-ref', source.name, '/dest'] + extra_args,
                    obj={'CLIENT': client, 'OUTPUT': 'pretty'},
                    catch_exceptions=False)
        return result, client

    def test_unspecified_options_send_none(self):
        result, client = self._invoke([])
        self.assertEqual(0, result.exit_code, result.output)
        kwargs = client.instance_put_blob.call_args.kwargs
        self.assertIsNone(kwargs['deadline_seconds'])
        self.assertIsNone(kwargs['progress_timeout_seconds'])

    def test_explicit_options_are_passed(self):
        result, client = self._invoke(
            ['--deadline', '30', '--progress-timeout', '10'])
        self.assertEqual(0, result.exit_code, result.output)
        kwargs = client.instance_put_blob.call_args.kwargs
        self.assertEqual(30, kwargs['deadline_seconds'])
        self.assertEqual(10, kwargs['progress_timeout_seconds'])
        self.assertNotIn('does not support', result.output)

    def test_explicit_options_are_passed_when_recycling_a_blob(self):
        # The other branch of the upload: a blob with this checksum already
        # exists in the cluster, so nothing is transferred. The flags have
        # to reach the agent operation just the same, because the agent
        # operation is the part they bound.
        result, client = self._invoke(
            ['--deadline', '30', '--progress-timeout', '10'], recycle=True)
        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('Recycling existing blob', result.output)
        kwargs = client.instance_put_blob.call_args.kwargs
        self.assertEqual(30, kwargs['deadline_seconds'])
        self.assertEqual(10, kwargs['progress_timeout_seconds'])

    def test_explicit_options_warn_when_server_is_incapable(self):
        result, _ = self._invoke(
            ['--deadline', '30', '--progress-timeout', '10'], capable=False)
        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('--deadline, --progress-timeout', result.output)

    def test_omitted_options_do_not_warn_when_server_is_incapable(self):
        result, _ = self._invoke([], capable=False)
        self.assertEqual(0, result.exit_code, result.output)
        self.assertNotIn('does not support', result.output)

    def test_the_warning_precedes_the_upload(self):
        # The capability check needs nothing the upload produces, so
        # emitting the warning afterwards tells a user their --deadline
        # cannot be honoured only once a multi-gigabyte file has finished
        # crossing the network -- long past the point where they could
        # have done anything about it.
        calls = []
        client = mock.Mock()
        client.check_capability.side_effect = lambda name: False
        real_warn = instance_cmd._warn_if_timing_unsupported

        def warn(ctx, timings):
            calls.append('warn')
            return real_warn(ctx, timings)

        def upload(*args, **kwargs):
            calls.append('upload')
            return {'uuid': 'art-uuid', 'blob_uuid': 'blob-uuid'}

        with tempfile.NamedTemporaryFile() as source, \
                mock.patch.object(
                    instance_cmd, '_warn_if_timing_unsupported', warn), \
                mock.patch.object(util, 'upload_artifact_with_progress', upload):
            result = CliRunner().invoke(
                instance_cmd.instance,
                ['upload', 'inst-ref', source.name, '/dest', '--deadline', '30'],
                obj={'CLIENT': client, 'OUTPUT': 'pretty'},
                catch_exceptions=False)

        self.assertEqual(0, result.exit_code, result.output)
        self.assertEqual(['warn', 'upload'], calls)
        self.assertIn('does not support --deadline', result.output)


class DownloadDeadlineOptionsTestCase(testtools.TestCase):
    """Tests for ``--deadline`` and ``--progress-timeout`` on
    ``sf-client instance download``.
    """

    def _invoke(self, extra_args):
        client = mock.Mock()
        client.instance_get.return_value = {
            'results': {'0': {'content_blob': 'blob-uuid'}}}
        client.get_blob_data.return_value = [b'data']
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            destination = os.path.join(tmpdir, 'dest')
            result = runner.invoke(
                instance_cmd.instance,
                ['download', 'inst-ref', '/source', destination] + extra_args,
                obj={'CLIENT': client, 'OUTPUT': 'pretty'},
                catch_exceptions=False)
        return result, client

    def test_unspecified_options_send_none(self):
        result, client = self._invoke([])
        self.assertEqual(0, result.exit_code, result.output)
        client.instance_get.assert_called_once_with(
            'inst-ref', '/source', deadline_seconds=None,
            progress_timeout_seconds=None)

    def test_explicit_options_are_passed(self):
        result, client = self._invoke(
            ['--deadline', '30', '--progress-timeout', '10'])
        self.assertEqual(0, result.exit_code, result.output)
        client.instance_get.assert_called_once_with(
            'inst-ref', '/source', deadline_seconds=30,
            progress_timeout_seconds=10)

    def test_zero_options_are_passed_not_dropped(self):
        result, client = self._invoke(
            ['--deadline', '0', '--progress-timeout', '0'])
        self.assertEqual(0, result.exit_code, result.output)
        client.instance_get.assert_called_once_with(
            'inst-ref', '/source', deadline_seconds=0,
            progress_timeout_seconds=0)

    def test_explicit_options_warn_when_server_is_incapable(self):
        client = mock.Mock()
        client.check_capability.return_value = False
        client.instance_get.return_value = {
            'results': {'0': {'content_blob': 'blob-uuid'}}}
        client.get_blob_data.return_value = [b'data']
        with tempfile.TemporaryDirectory() as tmpdir:
            result = CliRunner().invoke(
                instance_cmd.instance,
                ['download', 'inst-ref', '/source',
                 os.path.join(tmpdir, 'dest'),
                 '--deadline', '30', '--progress-timeout', '10'],
                obj={'CLIENT': client, 'OUTPUT': 'pretty'},
                catch_exceptions=False)

        self.assertEqual(0, result.exit_code, result.output)
        self.assertIn('--deadline, --progress-timeout', result.output)
