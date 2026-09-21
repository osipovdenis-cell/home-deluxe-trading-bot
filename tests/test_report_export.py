import json
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from bot.audit import AuditLog
from bot.main import handle_observer_commands
from bot.report_export import collect_reports, ReportExportWorker, write_encrypted_report
from bot.trading import PaperTrader


class ReportExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.folder = Path(cls.directory.name)
        cls.key = cls.folder / 'key.pem'
        cls.cert = cls.folder / 'cert.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-days', '1', '-subj', '/CN=Test', '-keyout', str(cls.key),
                        '-out', str(cls.cert)], capture_output=True, check=True)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def decrypt(self, encrypted):
        return subprocess.run(['openssl', 'cms', '-decrypt', '-binary', '-inform', 'DER',
                               '-inkey', str(self.key)], input=encrypted, capture_output=True)

    def test_only_ciphertext_written_authenticated_and_previous_survives_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'latest.p7m'
            bundle = {'reports': ['PRIVATE_SENTINEL_баланс_12345']}
            write_encrypted_report(bundle, path, self.cert)
            encrypted = path.read_bytes()
            self.assertNotIn(b'PRIVATE_SENTINEL', encrypted)
            self.assertEqual(json.loads(self.decrypt(encrypted).stdout), bundle)
            self.assertEqual([p.name for p in path.parent.iterdir()], ['latest.p7m'])
            changed = bytearray(encrypted)
            changed[-1] ^= 1
            self.assertNotEqual(self.decrypt(bytes(changed)).returncode, 0)
            with self.assertRaisesRegex(RuntimeError, 'encryption failed'):
                write_encrypted_report(bundle, path, self.folder / 'missing.pem')
            self.assertEqual(path.read_bytes(), encrypted)

    def test_real_learning_report_collected_without_messaging_or_new_trades(self):
        audit = AuditLog(':memory:')
        trader = PaperTrader(':memory:', 200, 50, 4, 70, .5, .7, 1, 1.5, 1, 0, .2)
        try:
            trader.open_on_signal('LSKUSDT', 100, 'лидер', 80, 1)
            trader.update_positions({'LSKUSDT': 96.27}, 2)
            audit.record_signal(1, 'LSKUSDT', 100, 'лидер', 3, 20, 1000000,
                                80, 'ok', volume_ratio_5m=2.5, ai_decision='BUY',
                                ai_reason='PRIVATE_FREE_TEXT')
            audit.record_signal(3, 'LSKUSDT', 105, 'лидер', 8, 25, 1000000,
                                95, 'future', volume_ratio_5m=9)
            trader.connection.execute('CREATE TABLE secrets(token TEXT)')
            trader.connection.execute("INSERT INTO secrets VALUES('DO_NOT_EXPORT')")
            with (patch('bot.telegram.TelegramClient.send', side_effect=AssertionError('network')),
                  patch.object(trader, 'open_on_signal', side_effect=AssertionError('new trade'))):
                bundle = collect_reports(audit, trader, {}, 100000,
                                         handle_observer_commands, True)
            self.assertGreater(len(bundle['reports']), 8)
            self.assertIn('С запуска', '\n'.join(bundle['reports']))
            self.assertEqual(bundle['positions'][0]['symbol'], 'LSKUSDT')
            self.assertAlmostEqual(bundle['positions'][0]['realized_pnl_usdt'], -1.965)
            self.assertEqual(len(bundle['fills']), 2)
            self.assertEqual(len(bundle['positions'][0]['entry_signals']), 1)
            self.assertEqual(bundle['positions'][0]['entry_signals'][0]['volume_ratio_5m'], 2.5)
            self.assertEqual(bundle['positions'][0]['confirmation_candidates'], [])
            self.assertNotIn('PRIVATE_FREE_TEXT', json.dumps(bundle))
            self.assertNotIn('DO_NOT_EXPORT', json.dumps(bundle))
            self.assertEqual(trader.connection.execute('SELECT COUNT(*) FROM paper_positions').fetchone()[0], 1)
            self.assertTrue(bundle['exit_monitor_healthy'])
            self.assertEqual(bundle['history_lookup_probe']['symbol'], 'LSKUSDT')
            self.assertGreaterEqual(bundle['history_lookup_probe']['elapsed_seconds'],0)
            self.assertEqual(bundle['entry_latency'],[])
        finally:
            audit.close()
            trader.close()

    def test_background_collection_does_not_block_caller_and_retries_without_data_in_logs(self):
        entered, release, saved = threading.Event(), threading.Event(), threading.Event()
        calls = []
        def collect(prices, now):
            calls.append((threading.get_ident(), prices))
            if len(calls) == 1:
                entered.set()
                release.wait(2)
                raise ValueError('PRIVATE_ERROR_CONTENT')
            return {'reports': ['ok']}
        worker = ReportExportWorker(collect, interval=100, retry_interval=.01)
        worker.set_prices({'LSKUSDT': 100})
        with (patch('bot.report_export.write_encrypted_report', side_effect=lambda *a: saved.set()),
              patch('builtins.print') as logged):
            worker.start()
            try:
                self.assertTrue(entered.wait(1))
                worker.set_prices({'LSKUSDT': 101})  # Must remain available during report work.
                release.set()
                self.assertTrue(saved.wait(2))
            finally:
                release.set()
                worker.close()
        self.assertNotEqual(calls[0][0], threading.get_ident())
        self.assertEqual(calls[1][1], {'LSKUSDT': 101})
        self.assertNotIn('PRIVATE_ERROR_CONTENT', str(logged.call_args_list))


if __name__ == '__main__':
    unittest.main()
