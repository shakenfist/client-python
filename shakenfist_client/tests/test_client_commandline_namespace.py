import datetime
import json
from unittest import mock

import click
import testtools
from click.shell_completion import ShellComplete
from click.testing import CliRunner

from shakenfist_client.commandline import namespace as namespace_cmd


CLAIM = {
    'uuid': 'a-claim',
    'namespace': 'testspace',
    'state': 'created',
    'coverage_state': 'expired',
    'limit_cpus': 40,
    'limit_memory_mb': 81920,
    'limit_disk_gb': 2000,
    'used_cpus': 12,
    'used_memory_mb': 24576,
    'used_disk_gb': 600,
    'expires_at': 1755300000.0,
    'updated_at': 1755213600.0,
}


class NamespaceClaimCommandTestCase(testtools.TestCase):
    """Tests for ``sf-client namespace claim``."""

    def _invoke(self, args, output='pretty'):
        client = mock.MagicMock()
        client.get_namespace_claims.return_value = [CLAIM]
        client.get_namespace_claim.return_value = CLAIM
        client.create_namespace_claim.return_value = CLAIM
        client.update_namespace_claim.return_value = CLAIM
        client.delete_namespace_claim.return_value = CLAIM

        runner = CliRunner()
        result = runner.invoke(
            namespace_cmd.namespace, args,
            obj={'CLIENT': client, 'OUTPUT': output},
            catch_exceptions=False)
        return result, client

    def test_create_passes_every_dimension(self):
        result, client = self._invoke([
            'claim', 'create', 'testspace', '--cpus', '4', '--memory-mb',
            '4096', '--disk-gb', '40', '--expires-in', '3600'])

        self.assertEqual(0, result.exit_code)
        client.create_namespace_claim.assert_called_with(
            'testspace', 4, 4096, 40, 3600)

    def test_update_sends_only_what_was_named(self):
        # The server reads the body as a field mask, so an update which
        # names only an expiry must not carry limits read from anywhere
        # else -- that would turn a re-date into a resize.
        result, client = self._invoke([
            'claim', 'update', 'testspace', 'a-claim', '--expires-in', '60'])

        self.assertEqual(0, result.exit_code)
        client.update_namespace_claim.assert_called_with(
            'testspace', 'a-claim', limit_cpus=None, limit_memory_mb=None,
            limit_disk_gb=None, expires_in_seconds=60)

    def test_update_passes_the_dimensions_it_is_given(self):
        result, client = self._invoke([
            'claim', 'update', 'testspace', 'a-claim', '--cpus', '8'])

        self.assertEqual(0, result.exit_code)
        client.update_namespace_claim.assert_called_with(
            'testspace', 'a-claim', limit_cpus=8, limit_memory_mb=None,
            limit_disk_gb=None, expires_in_seconds=None)

    def test_show_does_not_merge_the_two_states(self):
        # state is the object's existence, coverage_state is whether the
        # claim covers placements. An expired claim is state created,
        # coverage_state expired, and a view which collapsed them would
        # hide the one thing an operator can act on.
        result, _ = self._invoke(['claim', 'show', 'testspace', 'a-claim'])

        self.assertEqual(0, result.exit_code)
        self.assertIn('state', result.output)
        self.assertIn('created', result.output)
        self.assertIn('coverage_state', result.output)
        self.assertIn('expired', result.output)

    def test_list_reports_both_states_and_the_drawdown(self):
        result, _ = self._invoke(['claim', 'list', 'testspace'],
                                 output='simple')

        self.assertEqual(0, result.exit_code)
        self.assertIn('coverage', result.output)
        self.assertIn('created', result.output)
        self.assertIn('expired', result.output)
        self.assertIn('12 / 40', result.output)

    def test_delete_asks_the_client_to_delete(self):
        result, client = self._invoke(
            ['claim', 'delete', 'testspace', 'a-claim'])

        self.assertEqual(0, result.exit_code)
        client.delete_namespace_claim.assert_called_with(
            'testspace', 'a-claim')

    def test_a_claim_with_no_expiry_renders_as_empty(self):
        # The point of the guard in _claim_expiry is that a null expiry
        # renders as nothing rather than crashing fromtimestamp() or
        # printing the string 'None' at an operator, so the empty string
        # is the thing worth asserting -- exit code 0 alone would pass
        # for either of the wrong answers.
        client = mock.MagicMock()
        client.get_namespace_claim.return_value = dict(CLAIM, expires_at=None)

        runner = CliRunner()
        result = runner.invoke(
            namespace_cmd.namespace,
            ['claim', 'show', 'testspace', 'a-claim'],
            obj={'CLIENT': client, 'OUTPUT': 'pretty'},
            catch_exceptions=False)

        self.assertEqual(0, result.exit_code)
        self.assertIn('expires_at      : \n', result.output)
        self.assertNotIn('None', result.output)

    def test_claim_expiry_renders_a_timestamp(self):
        # The other half of the same helper: a real expiry has to come
        # out as a readable local time, not as the raw unix float.
        rendered = namespace_cmd._claim_expiry(CLAIM)

        self.assertEqual(
            str(datetime.datetime.fromtimestamp(CLAIM['expires_at'])),
            rendered)
        self.assertNotIn('1755300000', rendered)

    def test_claim_expiry_is_empty_when_the_expiry_is_null(self):
        self.assertEqual('', namespace_cmd._claim_expiry(
            dict(CLAIM, expires_at=None)))

    def test_show_renders_the_expiry_it_was_given(self):
        result, _ = self._invoke(['claim', 'show', 'testspace', 'a-claim'])

        self.assertEqual(0, result.exit_code)
        self.assertIn(
            str(datetime.datetime.fromtimestamp(CLAIM['expires_at'])),
            result.output)

    def test_update_with_no_options_does_not_call_the_server(self):
        # The CLI knows the user named nothing, so it says which options
        # they could have used rather than spending a round trip on the
        # server's 400. The library layer deliberately still sends the
        # empty body -- that behaviour is asserted in the apiclient tests.
        result, client = self._invoke(
            ['claim', 'update', 'testspace', 'a-claim'])

        self.assertEqual(1, result.exit_code)
        self.assertIn('--cpus', result.output)
        self.assertIn('--expires-in', result.output)
        client.update_namespace_claim.assert_not_called()

    def test_create_prints_the_new_claim_uuid(self):
        result, _ = self._invoke([
            'claim', 'create', 'testspace', '--cpus', '4', '--memory-mb',
            '4096', '--disk-gb', '40', '--expires-in', '3600'])

        self.assertEqual(0, result.exit_code)
        self.assertEqual('a-claim', result.output.strip())

    def test_json_output_is_the_claim_the_server_sent(self):
        # The json mode is what scripts consume, so it has to be the
        # server's payload rather than anything this CLI rearranged.
        result, _ = self._invoke(['claim', 'show', 'testspace', 'a-claim'],
                                 output='json')

        self.assertEqual(0, result.exit_code)
        self.assertEqual(CLAIM, json.loads(result.output))

    def test_list_of_no_claims_renders_a_header_and_nothing_else(self):
        client = mock.MagicMock()
        client.get_namespace_claims.return_value = []

        runner = CliRunner()
        result = runner.invoke(
            namespace_cmd.namespace, ['claim', 'list', 'testspace'],
            obj={'CLIENT': client, 'OUTPUT': 'simple'},
            catch_exceptions=False)

        self.assertEqual(0, result.exit_code)
        self.assertEqual(','.join(namespace_cmd.CLAIM_COLUMNS),
                         result.output.strip())


class ClaimCompletionTestCase(testtools.TestCase):
    """Tests for claim uuid shell completion.

    click calls a shell_complete callback as (ctx, param, incomplete),
    so the namespace has to come out of the partially parsed context --
    the second argument is a click Parameter and has no arguments in it.
    """

    def _ctx(self, params):
        client = mock.MagicMock()
        client.get_namespace_claims.return_value = [
            {'uuid': 'aaaa-claim'}, {'uuid': 'bbbb-claim'}]
        ctx = mock.MagicMock()
        ctx.obj = {'CLIENT': client}
        ctx.params = params
        return ctx, client

    def test_claims_of_the_named_namespace_are_offered(self):
        ctx, client = self._ctx({'namespace': 'testspace'})

        self.assertEqual(
            ['aaaa-claim', 'bbbb-claim'],
            namespace_cmd._get_claims(ctx, mock.sentinel.param, ''))
        client.get_namespace_claims.assert_called_with('testspace')

    def test_completion_is_filtered_by_what_was_typed(self):
        ctx, _ = self._ctx({'namespace': 'testspace'})

        self.assertEqual(
            ['bbbb-claim'],
            namespace_cmd._get_claims(ctx, mock.sentinel.param, 'bb'))

    def test_no_namespace_yet_asks_the_server_nothing(self):
        # The namespace is the argument before this one, so it is absent
        # while the user is still typing it. There is nothing to list,
        # and no useful request to make.
        ctx, client = self._ctx({})

        self.assertEqual(
            [], namespace_cmd._get_claims(ctx, mock.sentinel.param, ''))
        client.get_namespace_claims.assert_not_called()

    def test_completion_works_through_click(self):
        # The tests above hand _get_claims a context they built, so they
        # would pass even if click never populated ctx.params with the
        # namespace. This one drives click's own completion machinery,
        # which is the thing that has to hold.
        client = mock.MagicMock()
        client.get_namespace_claims.return_value = [
            {'uuid': 'aaaa-claim'}, {'uuid': 'bbbb-claim'}]

        @click.group()
        @click.pass_context
        def cli(ctx):
            ctx.obj = {'CLIENT': client}

        cli.add_command(namespace_cmd.namespace)

        completer = ShellComplete(
            cli, {'obj': {'CLIENT': client}}, 'sf-client', '_SF_COMPLETE')
        completions = completer.get_completions(
            ['namespace', 'claim', 'show', 'testspace'], '')

        self.assertEqual(['aaaa-claim', 'bbbb-claim'],
                         [c.value for c in completions])
        client.get_namespace_claims.assert_called_with('testspace')
