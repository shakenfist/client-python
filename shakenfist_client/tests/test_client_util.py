import testtools

from shakenfist_client import util


class SanitizeTerminalBytesTestCase(testtools.TestCase):
    def test_plain_text_is_unchanged(self):
        self.assertEqual(
            b'hello world\n',
            util.sanitize_terminal_bytes(b'hello world\n'))

    def test_tabs_and_newlines_preserved(self):
        self.assertEqual(
            b'a\tb\nc\rd',
            util.sanitize_terminal_bytes(b'a\tb\nc\rd'))

    def test_csi_color_sequence_stripped(self):
        self.assertEqual(
            b'OK Finished',
            util.sanitize_terminal_bytes(b'\x1b[0;32mOK\x1b[0m Finished'))

    def test_osc_set_window_title_stripped(self):
        # OSC 0;title BEL — used to set the terminal window title.
        self.assertEqual(
            b'after',
            util.sanitize_terminal_bytes(b'\x1b]0;pwn\x07after'))

    def test_osc_terminated_by_st_stripped(self):
        self.assertEqual(
            b'after',
            util.sanitize_terminal_bytes(b'\x1b]0;pwn\x1b\\after'))

    def test_dcs_stripped(self):
        self.assertEqual(
            b'tail',
            util.sanitize_terminal_bytes(b'\x1bPpayload\x1b\\tail'))

    def test_lone_escape_dropped(self):
        # ESC followed by a single Fe byte (e.g. ESC c — full reset).
        self.assertEqual(
            b'after',
            util.sanitize_terminal_bytes(b'\x1bcafter'))

    def test_other_c0_controls_stripped(self):
        # NUL, BS, VT, FF, SO, SI, etc. removed; \t \n \r preserved (above).
        self.assertEqual(
            b'ab',
            util.sanitize_terminal_bytes(b'a\x00\x01\x02\x07\x08\x0bb'))

    def test_c1_controls_stripped(self):
        self.assertEqual(
            b'ab',
            util.sanitize_terminal_bytes(b'a\x80\x9bb'))

    def test_del_stripped(self):
        self.assertEqual(
            b'ab',
            util.sanitize_terminal_bytes(b'a\x7fb'))

    def test_empty_input(self):
        self.assertEqual(b'', util.sanitize_terminal_bytes(b''))

    def test_large_input_completes(self):
        # Smoke test for ReDoS resistance. The patterns use disjoint
        # character classes and bounded `[^\x07\x1b]*` / `[^\x1b]*` runs,
        # so backtracking is linear; verify a 1 MB worst-case-shaped input
        # finishes and produces the expected output.
        chunk = b'\x1b[' + b'0;' * 50 + b'm' + b'a'
        payload = chunk * 10000
        self.assertGreater(len(payload), 1_000_000)
        self.assertEqual(b'a' * 10000, util.sanitize_terminal_bytes(payload))
