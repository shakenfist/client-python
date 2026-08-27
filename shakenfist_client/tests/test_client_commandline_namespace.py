from unittest import mock

import testtools
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

    def test_a_claim_with_no_expiry_renders(self):
        client = mock.MagicMock()
        client.get_namespace_claim.return_value = dict(CLAIM, expires_at=None)

        runner = CliRunner()
        result = runner.invoke(
            namespace_cmd.namespace,
            ['claim', 'show', 'testspace', 'a-claim'],
            obj={'CLIENT': client, 'OUTPUT': 'pretty'},
            catch_exceptions=False)

        self.assertEqual(0, result.exit_code)
