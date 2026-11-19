import json
import os
import shutil
import stat
import tempfile
from unittest import mock

import testtools

from shakenfist_client import credentials


# Deliberately unconvincing. A credential scanner runs over this
# repository's history with a positive control, and a fixture which
# looks like a real namespace key is a finding whether or not it ever
# unlocked anything.
CLUSTER = 'http://localhost:13000'
OTHER_CLUSTER = 'http://elsewhere:13000'
FAKE_KEY = 'not-a-key-1'
OTHER_FAKE_KEY = 'not-a-key-2'


def minted(namespace='ci', key_name='corporate-aaaa', key=FAKE_KEY,
           expiry=None, scopes=None):
    """One entry shaped as the cluster's mint returns it."""
    return {
        'namespace': namespace,
        'key_name': key_name,
        'key': key,
        'expiry': expiry,
        'scopes': scopes if scopes is not None else ['blob.read'],
    }


def cached(apiurl=CLUSTER, **kwargs):
    """One entry shaped as this module stores it."""
    entry = minted(**kwargs)
    entry['apiurl'] = apiurl
    return entry


class CredentialsPathTestCase(testtools.TestCase):
    def test_xdg_config_home_is_honoured(self):
        with mock.patch.dict(os.environ, {'XDG_CONFIG_HOME': '/xdg'}):
            self.assertEqual('/xdg/shakenfist/credentials.json',
                             credentials.credentials_path())

    def test_home_is_the_fallback(self):
        environ = dict(os.environ)
        environ.pop('XDG_CONFIG_HOME', None)
        with mock.patch.dict(os.environ, environ, clear=True):
            with mock.patch('os.path.expanduser', return_value='/home/fake'):
                self.assertEqual('/home/fake/.config/shakenfist/credentials.json',
                                 credentials.credentials_path())

    def test_an_explicit_path_wins(self):
        with mock.patch.dict(os.environ, {'XDG_CONFIG_HOME': '/xdg'}):
            self.assertEqual('/elsewhere/creds.json',
                             credentials.credentials_path('/elsewhere/creds.json'))


class CredentialsTestCase(testtools.TestCase):
    def setUp(self):
        super().setUp()
        # Never the real ~/.config/shakenfist: every call here is
        # given an explicit path inside a temporary directory, which is
        # why the module takes one.
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir, True)
        self.directory = os.path.join(self.tempdir, 'shakenfist')
        self.path = os.path.join(self.directory, 'credentials.json')

    def mode_of(self, path):
        return stat.S_IMODE(os.stat(path).st_mode)

    def write_raw(self, content, mode=0o600):
        os.makedirs(self.directory, exist_ok=True)
        os.chmod(self.directory, 0o700)
        with open(self.path, 'w') as f:
            f.write(content)
        os.chmod(self.path, mode)

    def read_raw(self):
        with open(self.path) as f:
            return f.read()

    # The modes, which are the whole of what protects a namespace key
    # at rest.
    def test_the_file_is_created_owner_only(self):
        credentials.write_credentials([cached()], path=self.path)
        self.assertEqual(0o600, self.mode_of(self.path))

    def test_the_directory_is_created_owner_only(self):
        credentials.write_credentials([cached()], path=self.path)
        self.assertEqual(0o700, self.mode_of(self.directory))

    def test_the_temporary_file_is_created_owner_only_beside_the_cache(self):
        # The mode matters on the temporary file as much as on the
        # final one -- the secret is in it -- and it has to be in the
        # same directory, because a rename across filesystems is not
        # atomic.
        seen = []
        real_mkstemp = tempfile.mkstemp

        def recording_mkstemp(*args, **kwargs):
            fd, name = real_mkstemp(*args, **kwargs)
            seen.append((os.path.dirname(name), self.mode_of(name)))
            return fd, name

        with mock.patch('shakenfist_client.credentials.tempfile.mkstemp',
                        recording_mkstemp):
            credentials.write_credentials([cached()], path=self.path)

        self.assertEqual([(self.directory, 0o600)], seen)

    def test_an_existing_directory_keeps_its_mode(self):
        # 0750 is not what this module would have created, but it
        # grants nobody else a way in, so it is not refused either.
        os.makedirs(self.directory)
        os.chmod(self.directory, 0o750)
        credentials.write_credentials([cached()], path=self.path)
        self.assertEqual(0o750, self.mode_of(self.directory))
        self.assertEqual(0o600, self.mode_of(self.path))

    def test_a_group_writable_directory_is_refused(self):
        os.makedirs(self.directory)
        os.chmod(self.directory, 0o770)
        e = self.assertRaises(credentials.CredentialCacheException,
                              credentials.write_credentials,
                              [cached()], path=self.path)
        self.assertIn(self.directory, str(e))
        self.assertFalse(os.path.exists(self.path))

    def test_a_group_writable_directory_is_refused_on_read(self):
        credentials.write_credentials([cached()], path=self.path)
        os.chmod(self.directory, 0o770)
        self.addCleanup(os.chmod, self.directory, 0o700)
        e = self.assertRaises(credentials.CredentialCacheException,
                              credentials.read_credentials, path=self.path)
        self.assertIn(self.directory, str(e))

    def test_a_readable_file_is_refused_and_names_itself(self):
        self.write_raw(json.dumps({'version': 1, 'credentials': [cached()],
                                   'current': None}), mode=0o644)
        e = self.assertRaises(credentials.CredentialCacheException,
                              credentials.read_credentials, path=self.path)
        self.assertIn(self.path, str(e))
        self.assertIn('0644', str(e))
        # Refused, not used: nothing of the secret is in the message.
        self.assertNotIn(FAKE_KEY, str(e))

    def test_every_group_and_other_bit_is_refused(self):
        document = json.dumps({'version': 1, 'credentials': [], 'current': None})
        for mode in (0o640, 0o620, 0o610, 0o604, 0o602, 0o601, 0o660,
                     0o644, 0o666):
            self.write_raw(document, mode=mode)
            self.assertRaises(credentials.CredentialCacheException,
                              credentials.read_credentials, path=self.path)

    def test_owner_only_modes_are_accepted(self):
        document = json.dumps({'version': 1, 'credentials': [], 'current': None})
        for mode in (0o600, 0o400):
            self.write_raw(document, mode=mode)
            self.assertEqual([], credentials.read_credentials(path=self.path)['credentials'])

    # A missing file, a corrupt one, and one from the future.
    def test_a_missing_file_is_not_an_error(self):
        self.assertIsNone(credentials.read_credentials(path=self.path))
        self.assertEqual([], credentials.credentials_for_apiurl(CLUSTER, path=self.path))
        self.assertIsNone(credentials.current_credential(path=self.path))
        self.assertEqual(0, credentials.delete_credentials(path=self.path))
        self.assertEqual(0, credentials.purge_expired(path=self.path))

    def test_a_truncated_file_is_refused_not_a_traceback(self):
        self.write_raw('{"version": 1, "credentials": [{"namespa')
        e = self.assertRaises(credentials.CredentialCacheException,
                              credentials.read_credentials, path=self.path)
        self.assertIn(self.path, str(e))

    def test_an_empty_file_is_refused(self):
        self.write_raw('')
        self.assertRaises(credentials.CredentialCacheException,
                          credentials.read_credentials, path=self.path)

    def test_a_file_which_is_not_an_object_is_refused(self):
        self.write_raw(json.dumps([cached()]))
        self.assertRaises(credentials.CredentialCacheException,
                          credentials.read_credentials, path=self.path)

    def test_a_file_with_no_version_is_refused(self):
        self.write_raw(json.dumps({'credentials': [], 'current': None}))
        self.assertRaises(credentials.CredentialCacheException,
                          credentials.read_credentials, path=self.path)

    def test_a_file_with_no_credential_list_is_refused(self):
        self.write_raw(json.dumps({'version': 1, 'current': None}))
        self.assertRaises(credentials.CredentialCacheException,
                          credentials.read_credentials, path=self.path)

    def test_a_credential_with_no_cluster_is_refused(self):
        # Which is the point of recording one: a key with no cluster
        # could be presented to a cluster which never issued it.
        entry = cached()
        del entry['apiurl']
        self.write_raw(json.dumps({'version': 1, 'credentials': [entry],
                                   'current': None}))
        e = self.assertRaises(credentials.CredentialCacheException,
                              credentials.read_credentials, path=self.path)
        self.assertIn('apiurl', str(e))

    def test_a_credential_with_an_unusable_expiry_is_refused(self):
        self.write_raw(json.dumps(
            {'version': 1, 'credentials': [cached(expiry='soon')],
             'current': None}))
        self.assertRaises(credentials.CredentialCacheException,
                          credentials.read_credentials, path=self.path)

    def test_a_newer_format_version_is_refused(self):
        document = json.dumps(
            {'version': credentials.CACHE_FORMAT_VERSION + 1,
             'credentials': [cached()], 'current': None})
        self.write_raw(document)
        e = self.assertRaises(credentials.CredentialCacheException,
                              credentials.read_credentials, path=self.path)
        self.assertIn(self.path, str(e))
        self.assertIn(str(credentials.CACHE_FORMAT_VERSION + 1), str(e))
        # And not overwritten: a newer client may hold credentials in
        # fields this one does not know about. This is the one refusal
        # a login may not override, so it is not the replaceable kind.
        self.assertEqual(document, self.read_raw())
        self.assertNotIsInstance(e, credentials.CredentialCacheUnusable)

    def test_an_unknown_older_format_version_is_refused(self):
        # Refused to read, but replaceable: an older format holds
        # nothing a fresh login needs to preserve.
        self.write_raw(json.dumps({'version': 0, 'credentials': [],
                                   'current': None}))
        self.assertRaises(credentials.CredentialCacheUnusable,
                          credentials.read_credentials, path=self.path)

    # Expiry.
    def test_an_expired_credential_is_dropped_on_read(self):
        live = cached(namespace='ops', key_name='corporate-bbbb',
                      key=OTHER_FAKE_KEY, expiry=2000.0)
        dead = cached(namespace='ci', key_name='corporate-aaaa',
                      key=FAKE_KEY, expiry=900.0)
        credentials.write_credentials([live, dead], path=self.path)

        cache = credentials.read_credentials(path=self.path, now=1000.0)
        self.assertEqual([live], cache['credentials'])
        self.assertEqual([live], credentials.credentials_for_apiurl(
            CLUSTER, path=self.path, now=1000.0))

    def test_a_credential_expiring_exactly_now_is_dropped(self):
        credentials.write_credentials([cached(expiry=1000.0)], path=self.path)
        self.assertEqual([], credentials.read_credentials(
            path=self.path, now=1000.0)['credentials'])

    def test_a_credential_with_no_expiry_is_kept(self):
        entry = cached(expiry=None)
        credentials.write_credentials([entry], path=self.path)
        self.assertEqual([entry], credentials.read_credentials(
            path=self.path, now=1000.0)['credentials'])

    def test_a_selection_naming_an_expired_credential_is_dropped(self):
        entry = cached(expiry=900.0)
        credentials.write_credentials(
            [entry], current={'apiurl': CLUSTER, 'namespace': 'ci',
                              'key_name': 'corporate-aaaa'},
            path=self.path)
        self.assertIsNone(credentials.read_credentials(
            path=self.path, now=1000.0)['current'])
        self.assertIsNone(credentials.current_credential(
            path=self.path, now=1000.0))

    def test_reading_does_not_rewrite_the_file(self):
        # Dropping an expired credential from what a read returns is
        # not permission to rewrite someone's file underneath them.
        credentials.write_credentials([cached(expiry=900.0)], path=self.path)
        before = self.read_raw()
        credentials.read_credentials(path=self.path, now=1000.0)
        self.assertEqual(before, self.read_raw())

    def test_purge_expired_rewrites_the_file(self):
        live = cached(namespace='ops', key_name='corporate-bbbb',
                      key=OTHER_FAKE_KEY, expiry=2000.0)
        credentials.write_credentials([live, cached(expiry=900.0)], path=self.path)

        self.assertEqual(1, credentials.purge_expired(path=self.path, now=1000.0))
        self.assertNotIn(FAKE_KEY, self.read_raw())
        self.assertIn(OTHER_FAKE_KEY, self.read_raw())
        self.assertEqual(0, credentials.purge_expired(path=self.path, now=1000.0))

    # The atomic write.
    def test_a_failed_write_leaves_the_previous_cache_intact(self):
        credentials.write_credentials(
            [cached()], current={'apiurl': CLUSTER, 'namespace': 'ci'},
            path=self.path)
        before = self.read_raw()

        with mock.patch('shakenfist_client.credentials.json.dump',
                        side_effect=OSError(28, 'No space left on device')):
            self.assertRaises(
                credentials.CredentialCacheException,
                credentials.write_credentials,
                [cached(namespace='ops', key_name='corporate-bbbb',
                        key=OTHER_FAKE_KEY)],
                path=self.path)

        self.assertEqual(before, self.read_raw())
        self.assertEqual(['credentials.json'], os.listdir(self.directory))

    def test_a_successful_write_leaves_no_temporary_file(self):
        credentials.write_credentials([cached()], path=self.path)
        credentials.write_credentials([cached()], path=self.path)
        self.assertEqual(['credentials.json'], os.listdir(self.directory))

    def test_a_write_into_an_unwritable_directory_refuses(self):
        # Which is also what a hostile umask produces: a umask with
        # owner bits in it leaves a directory this module cannot write
        # into, and that has to be a message rather than a traceback.
        credentials.write_credentials([cached()], path=self.path)
        os.chmod(self.directory, 0o500)
        self.addCleanup(os.chmod, self.directory, 0o700)

        e = self.assertRaises(credentials.CredentialCacheException,
                              credentials.write_credentials,
                              [cached(namespace='ops', key_name='corporate-bbbb',
                                      key=OTHER_FAKE_KEY)],
                              path=self.path)
        self.assertIn(self.path, str(e))
        self.assertIn(FAKE_KEY, self.read_raw())

    def test_a_write_which_cannot_verify_the_mode_refuses(self):
        # The mode of the temporary file is verified rather than
        # assumed, so that a platform where mkstemp() or the umask
        # behaved differently would be a refusal and not a leak.
        real_mkstemp = tempfile.mkstemp

        def permissive_mkstemp(*args, **kwargs):
            fd, name = real_mkstemp(*args, **kwargs)
            os.chmod(name, 0o644)
            return fd, name

        with mock.patch('shakenfist_client.credentials.tempfile.mkstemp',
                        permissive_mkstemp):
            self.assertRaises(credentials.CredentialCacheException,
                              credentials.write_credentials,
                              [cached()], path=self.path)

        self.assertFalse(os.path.exists(self.path))
        self.assertEqual([], os.listdir(self.directory))

    # Writing a credential set, and the selection.
    def test_a_sole_credential_becomes_the_selection(self):
        result = credentials.store_credential_set(
            CLUSTER, [minted(expiry=2000.0)], path=self.path, now=1000.0)

        self.assertEqual({'apiurl': CLUSTER, 'namespace': 'ci',
                          'key_name': 'corporate-aaaa'}, result['current'])
        current = credentials.current_credential(path=self.path, now=1000.0)
        self.assertEqual(CLUSTER, current['apiurl'])
        self.assertEqual(FAKE_KEY, current['key'])

    def test_the_cluster_is_recorded_against_every_credential(self):
        credentials.store_credential_set(
            CLUSTER,
            [minted(expiry=2000.0),
             minted(namespace='ops', key_name='corporate-bbbb',
                    key=OTHER_FAKE_KEY, expiry=2000.0)],
            path=self.path, now=1000.0)

        cache = credentials.read_credentials(path=self.path, now=1000.0)
        self.assertEqual([CLUSTER, CLUSTER],
                         [c['apiurl'] for c in cache['credentials']])

    def test_several_credentials_leave_the_selection_unmade(self):
        result = credentials.store_credential_set(
            CLUSTER,
            [minted(expiry=2000.0),
             minted(namespace='ops', key_name='corporate-bbbb',
                    key=OTHER_FAKE_KEY, expiry=2000.0)],
            path=self.path, now=1000.0)
        self.assertIsNone(result['current'])
        self.assertIsNone(credentials.current_credential(path=self.path, now=1000.0))

    def test_a_named_namespace_is_selected_from_several(self):
        result = credentials.store_credential_set(
            CLUSTER,
            [minted(expiry=2000.0),
             minted(namespace='ops', key_name='corporate-bbbb',
                    key=OTHER_FAKE_KEY, expiry=2000.0)],
            namespace='ops', path=self.path)
        self.assertEqual('ops', result['current']['namespace'])

    def test_a_second_login_replaces_only_that_clusters_credentials(self):
        credentials.store_credential_set(
            OTHER_CLUSTER, [minted(key=OTHER_FAKE_KEY, expiry=2000.0)],
            path=self.path, now=1000.0)
        credentials.store_credential_set(
            CLUSTER, [minted(key=FAKE_KEY, expiry=2000.0)], path=self.path,
            now=1000.0)
        credentials.store_credential_set(
            CLUSTER, [minted(key_name='corporate-cccc', key='not-a-key-3',
                             expiry=2000.0)],
            path=self.path, now=1000.0)

        cache = credentials.read_credentials(path=self.path, now=1000.0)
        self.assertEqual(
            [(OTHER_CLUSTER, 'corporate-aaaa'), (CLUSTER, 'corporate-cccc')],
            [(c['apiurl'], c['key_name']) for c in cache['credentials']])

    def test_a_login_elsewhere_leaves_another_clusters_selection(self):
        credentials.store_credential_set(
            OTHER_CLUSTER, [minted(key=OTHER_FAKE_KEY, expiry=2000.0)],
            path=self.path, now=1000.0)
        credentials.store_credential_set(
            CLUSTER,
            [minted(expiry=2000.0),
             minted(namespace='ops', key_name='corporate-bbbb',
                    key='not-a-key-3', expiry=2000.0)],
            path=self.path, now=1000.0)

        # Nothing was selected here, so the selection made against the
        # other cluster stands.
        current = credentials.current_credential(path=self.path, now=1000.0)
        self.assertEqual(OTHER_CLUSTER, current['apiurl'])

    def test_a_login_over_an_unreadable_cache_replaces_it(self):
        # The person is holding secrets which will never be issued
        # again, and what was in the file could not be read anyway.
        self.write_raw('{not json at all')
        credentials.store_credential_set(
            CLUSTER, [minted(expiry=2000.0)], path=self.path, now=1000.0)
        self.assertEqual(
            [FAKE_KEY],
            [c['key'] for c in credentials.read_credentials(
                path=self.path, now=1000.0)['credentials']])

    def test_a_login_over_a_newer_cache_refuses_and_keeps_it(self):
        # The counterpart of the test above. A newer client's cache may
        # hold credentials in fields this one cannot see, so a login
        # here must not replace it -- which is the whole reason the
        # replaceable refusals are a separate kind.
        document = json.dumps(
            {'version': credentials.CACHE_FORMAT_VERSION + 1,
             'credentials': [cached()], 'current': None})
        self.write_raw(document)

        self.assertRaises(
            credentials.CredentialCacheException,
            credentials.store_credential_set, CLUSTER,
            [minted(expiry=2000.0)], path=self.path, now=1000.0)
        self.assertEqual(document, self.read_raw())

    def test_a_login_over_a_cache_in_an_unsafe_directory_refuses(self):
        # Writing a fresh secret into a directory other users can
        # write to does not make it safe, so this refuses rather than
        # replacing the file.
        self.write_raw(json.dumps({'version': 1, 'credentials': [cached()],
                                   'current': None}))
        directory = os.path.dirname(self.path)
        os.chmod(directory, 0o770)
        self.addCleanup(os.chmod, directory, 0o700)

        self.assertRaises(
            credentials.CredentialCacheException,
            credentials.store_credential_set, CLUSTER,
            [minted(expiry=2000.0)], path=self.path, now=1000.0)

    def test_the_selection_is_cluster_scoped(self):
        credentials.store_credential_set(
            CLUSTER, [minted(expiry=2000.0)], path=self.path, now=1000.0)
        self.assertIsNotNone(credentials.current_credential(
            apiurl=CLUSTER, path=self.path, now=1000.0))
        self.assertIsNone(credentials.current_credential(
            apiurl=OTHER_CLUSTER, path=self.path, now=1000.0))

    def test_credentials_for_apiurl_answers_for_one_cluster_only(self):
        credentials.store_credential_set(
            CLUSTER, [minted(expiry=2000.0)], path=self.path, now=1000.0)
        credentials.store_credential_set(
            OTHER_CLUSTER, [minted(key=OTHER_FAKE_KEY, expiry=2000.0)],
            path=self.path, now=1000.0)

        self.assertEqual(
            [FAKE_KEY],
            [c['key'] for c in credentials.credentials_for_apiurl(
                CLUSTER, path=self.path, now=1000.0)])
        self.assertEqual([], credentials.credentials_for_apiurl(
            'http://nowhere:13000', path=self.path, now=1000.0))

    def test_select_current_switches_the_selection(self):
        credentials.store_credential_set(
            CLUSTER,
            [minted(expiry=2000.0),
             minted(namespace='ops', key_name='corporate-bbbb',
                    key=OTHER_FAKE_KEY, expiry=2000.0)],
            path=self.path, now=1000.0)

        chosen = credentials.select_current(CLUSTER, 'ops', path=self.path,
                                            now=1000.0)
        self.assertEqual(OTHER_FAKE_KEY, chosen['key'])
        self.assertEqual(
            'ops',
            credentials.current_credential(path=self.path, now=1000.0)['namespace'])

    def test_select_current_refuses_an_unknown_namespace(self):
        credentials.store_credential_set(
            CLUSTER, [minted(expiry=2000.0)], path=self.path, now=1000.0)
        e = self.assertRaises(credentials.CredentialCacheException,
                              credentials.select_current, CLUSTER, 'nope',
                              path=self.path, now=1000.0)
        self.assertIn('nope', str(e))

    def test_select_current_refuses_an_ambiguous_namespace(self):
        credentials.store_credential_set(
            CLUSTER,
            [minted(expiry=2000.0),
             minted(key_name='corporate-bbbb', key=OTHER_FAKE_KEY,
                    expiry=2000.0)],
            path=self.path, now=1000.0)

        e = self.assertRaises(credentials.CredentialCacheException,
                              credentials.select_current, CLUSTER, 'ci',
                              path=self.path, now=1000.0)
        self.assertIn('corporate-aaaa', str(e))
        self.assertIn('corporate-bbbb', str(e))

        chosen = credentials.select_current(CLUSTER, 'ci',
                                            key_name='corporate-bbbb',
                                            path=self.path, now=1000.0)
        self.assertEqual(OTHER_FAKE_KEY, chosen['key'])

    def test_select_current_refuses_an_expired_credential(self):
        credentials.write_credentials([cached(expiry=900.0)], path=self.path)
        self.assertRaises(credentials.CredentialCacheException,
                          credentials.select_current, CLUSTER, 'ci',
                          path=self.path, now=1000.0)

    def test_select_current_with_no_cache_refuses(self):
        self.assertRaises(credentials.CredentialCacheException,
                          credentials.select_current, CLUSTER, 'ci',
                          path=self.path, now=1000.0)

    def test_writing_a_credential_with_no_cluster_is_refused(self):
        entry = cached()
        del entry['apiurl']
        e = self.assertRaises(credentials.CredentialCacheException,
                              credentials.write_credentials, [entry],
                              path=self.path)
        self.assertIn('apiurl', str(e))
        self.assertFalse(os.path.exists(self.path))

    def test_selecting_a_credential_which_is_not_cached_is_refused(self):
        self.assertRaises(
            credentials.CredentialCacheException,
            credentials.write_credentials, [cached()],
            current={'apiurl': CLUSTER, 'namespace': 'ops'}, path=self.path)

    # Deleting the lot.
    def test_delete_removes_the_file_and_counts_what_went(self):
        credentials.store_credential_set(
            CLUSTER,
            [minted(expiry=2000.0),
             minted(namespace='ops', key_name='corporate-bbbb',
                    key=OTHER_FAKE_KEY, expiry=2000.0)],
            path=self.path, now=1000.0)

        self.assertEqual(2, credentials.delete_credentials(path=self.path))
        self.assertFalse(os.path.exists(self.path))
        self.assertIsNone(credentials.read_credentials(path=self.path))

    def test_delete_removes_a_file_this_module_would_not_read(self):
        # A cache which cannot be read is the one most worth removing,
        # so an unsafe mode does not stop a logout. The count is not
        # knowable, and says so rather than claiming zero.
        self.write_raw(json.dumps({'version': 1, 'credentials': [cached()],
                                   'current': None}), mode=0o644)
        self.assertIsNone(credentials.delete_credentials(path=self.path))
        self.assertFalse(os.path.exists(self.path))

    def test_delete_removes_a_corrupt_file(self):
        self.write_raw('{truncated')
        self.assertIsNone(credentials.delete_credentials(path=self.path))
        self.assertFalse(os.path.exists(self.path))
