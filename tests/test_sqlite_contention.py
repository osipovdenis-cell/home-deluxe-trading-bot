import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from bot.audit import AuditLog
from bot.rocket_recovery_shadow import RecoveryShadow, VERSION
from bot.sqlite_safety import recover_busy, write_batches, retry_initialization
from bot.trading import PaperTrader


class ContentionTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.path = str(Path(folder.name) / 'bot.db')
        self.trader = PaperTrader(self.path, 200, 50, 4, 70, 1, .7, 1, 1.5, 1, 0, .2)
        self.addCleanup(self.trader.close)
        self.db = self.trader.connection
        self.other = sqlite3.connect(self.path, timeout=.02)
        self.addCleanup(self.other.close)
        self.other.execute('CREATE TABLE competing_writer(value INTEGER)')
        self.other.commit()

    def competing_write(self):
        # A second real SQLite connection models the exit/quote writer.
        with self.other:
            self.other.execute('INSERT INTO competing_writer VALUES(1)')

    def test_scalp_batch_releases_writer_before_next_batch(self):
        from bot.scalp_shadow import ScalpShadow
        model = ScalpShadow(self.db)
        calls = []
        def quote(*args):
            if len(calls) % 100 == 0:
                self.competing_write()
            self.db.execute('INSERT INTO competing_writer VALUES(2)')
            calls.append(args)
        with patch.object(model, 'quote', side_effect=quote):
            model.process_quotes([(1, 'X', 100, 101)] * 205, 1)
        self.assertEqual(len(calls), 205)
        self.assertFalse(self.db.in_transaction)
        self.assertEqual(self.other.execute('SELECT count(*) FROM competing_writer WHERE value=1').fetchone()[0], 3)

    def test_retention_preserves_recent_rows_and_bounds_each_symbol(self):
        from bot.rocket_cards import schema, prune_bid_paths
        schema(self.db)
        self.db.executemany('INSERT INTO rocket_bid_path VALUES(?,?,?)',
            [(s,t,100) for s in ('X','Y') for t in range(12)])
        self.db.commit()
        prune_bid_paths(self.db, 10, batch_size=3)
        self.assertEqual(self.db.execute('SELECT count(*) FROM rocket_bid_path').fetchone()[0], 18)
        self.assertEqual(self.db.execute('SELECT count(*) FROM rocket_bid_path WHERE timestamp>=10').fetchone()[0], 4)
        self.competing_write()

    def test_initialization_retries_real_lock_and_closes_failed_connections(self):
        ready = threading.Event()
        release = threading.Event()
        def hold_writer():
            with sqlite3.connect(self.path) as db:
                db.execute('INSERT INTO competing_writer VALUES(7)')
                ready.set()
                release.wait(2)
        thread = threading.Thread(target=hold_writer)
        thread.start()
        self.assertTrue(ready.wait(1))
        attempts = []
        class Resource:
            @retry_initialization
            def __init__(resource, path):
                resource.connection = sqlite3.connect(path, timeout=.001)
                attempts.append(resource.connection)
                resource.connection.execute('INSERT INTO competing_writer VALUES(8)')
                resource.connection.commit()
        def unblock(_):
            release.set()
            thread.join(2)
        try:
            with patch('bot.sqlite_safety.time.sleep', side_effect=unblock), patch('builtins.print'):
                resource = Resource(self.path)
            self.addCleanup(resource.connection.close)
            self.assertEqual(len(attempts), 2)
            with self.assertRaises(sqlite3.ProgrammingError):
                attempts[0].execute('SELECT 1')
            self.assertEqual(self.other.execute('SELECT count(*) FROM competing_writer WHERE value=8').fetchone()[0], 1)
        finally:
            release.set(); thread.join(2)

    def test_initialization_does_not_retry_non_lock_errors(self):
        class Resource:
            @retry_initialization
            def __init__(resource):
                resource.connection = sqlite3.connect(':memory:')
                resource.connection.execute('SELECT * FROM missing')
        with patch('bot.sqlite_safety.time.sleep') as sleep:
            with self.assertRaises(sqlite3.OperationalError):
                Resource()
            sleep.assert_not_called()

    def test_report_replay_does_not_lock_writer_during_next_case_calculation(self):
        for symbol in ('X', 'Y'):
            self.trader.open_on_signal(symbol, 100, 'лидер', 80, 1)
            self.trader.update_positions({symbol: 98}, 2)
        calls = []
        def evaluate(row, now, minutes):
            calls.append(row['id'])
            self.competing_write()
            return {'position_id': row['id']}
        with patch.object(self.trader.stop_audit, '_evaluate', side_effect=evaluate):
            complete, pending, incomplete = self.trader.stop_audit.collect(4000)
        self.assertEqual((len(complete), pending, incomplete), (2, 0, 0))
        self.assertEqual(len(calls), 2)
        self.assertFalse(self.db.in_transaction)
        self.assertEqual(self.db.execute('SELECT count(*) FROM rocket_stop_replays').fetchone()[0], 2)

    def test_recovery_probes_never_hold_writer_between_pairs(self):
        model = RecoveryShadow(self.db)
        for ident in (1, 2):
            state = dict(symbol='X', start=100, stop=1, cost=.2, signal_price=100,
                         probe={}, last_check=100, missing=False,
                         A=dict(status='CLOSED', net=0), B=dict(status='WAIT', net=None))
            self.db.execute('INSERT INTO rocket_recovery_pairs VALUES(?,?,NULL,?)',
                            (ident, VERSION, json.dumps(state)))
        self.db.commit()
        calls = []
        def probe(symbol, original):
            calls.append(symbol)
            self.competing_write()
            return None
        model.probe_provider = probe
        model.tick(101)
        self.db.commit()
        self.assertEqual(len(calls), 2)
        # RecoveryShadow catches probe exceptions; check writes, not only calls.
        self.assertEqual(self.other.execute('SELECT count(*) FROM competing_writer').fetchone()[0], 2)

    def test_confirmation_calculation_does_not_hold_writer_for_next_event(self):
        audit = AuditLog(self.path)
        self.addCleanup(audit.close)
        from types import SimpleNamespace
        for symbol in ('X', 'Y'):
            audit.record_confirmation_event(SimpleNamespace(started_at=0, resolved_at=1,
                symbol=symbol, trigger_price=100, resolution_price=100, accepted=True,
                reason='ok', signal_kind='лидер'))
        audit.record_confirmation_prices({'X': 100, 'Y': 100}, 1)
        audit.record_confirmation_prices({'X': 101, 'Y': 101}, 9)
        original = audit._path_outcome
        def calculate(*args):
            self.competing_write()
            return original(*args)
        with patch.object(audit, '_path_outcome', side_effect=calculate):
            self.assertEqual(audit.refresh_confirmation_outcomes(11, .7, 1, 10), 2)
        self.assertFalse(audit.connection.in_transaction)

    def test_busy_recovery_retains_committed_trade_and_allows_next_write(self):
        self.trader.open_on_signal('X', 100, 'лидер', 80, 1)
        self.db.execute('PRAGMA busy_timeout=1')
        self.other.execute('BEGIN IMMEDIATE')
        try:
            self.db.execute('INSERT INTO competing_writer VALUES(9)')
        except sqlite3.OperationalError as error:
            with patch('builtins.print') as logged:
                recover_busy(error, self.db)
            self.assertIn('SQLite busy recovered', str(logged.call_args_list))
            self.assertNotIn('INSERT INTO', str(logged.call_args_list))
        else:
            self.fail('Expected a real SQLite writer conflict')
        self.other.rollback()
        self.assertFalse(self.db.in_transaction)
        self.competing_write()
        self.assertEqual(self.db.execute('SELECT count(*) FROM paper_positions').fetchone()[0], 1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM paper_fills').fetchone()[0], 1)

    def test_busy_recovery_rolls_back_uncommitted_work_but_not_other_errors(self):
        self.db.execute('INSERT INTO competing_writer VALUES(9)')
        with patch('builtins.print'):
            recover_busy(sqlite3.OperationalError('database is locked'), self.db)
        self.assertEqual(self.other.execute('SELECT count(*) FROM competing_writer').fetchone()[0], 0)
        for message in ('no such table: broken', 'disk I/O error', 'database disk image is malformed'):
            with self.subTest(message=message), self.assertRaises(sqlite3.OperationalError):
                recover_busy(sqlite3.OperationalError(message), self.db)

    def test_failed_batch_rolls_back_and_releases_writer(self):
        self.db.execute('CREATE TABLE unique_rows(value INTEGER PRIMARY KEY)')
        with self.assertRaises(sqlite3.IntegrityError):
            write_batches(self.db, 'INSERT INTO unique_rows VALUES(?)', [(1,), (1,)])
        self.assertFalse(self.db.in_transaction)
        self.assertEqual(self.db.execute('SELECT count(*) FROM unique_rows').fetchone()[0], 0)
        self.competing_write()

    def test_report_failure_during_calculation_leaves_no_writer_lock(self):
        for symbol in ('X', 'Y'):
            self.trader.open_on_signal(symbol, 100, 'лидер', 80, 1)
            self.trader.update_positions({symbol: 98}, 2)
        with patch.object(self.trader.stop_audit, '_evaluate',
                          side_effect=[{'position_id': 1}, ValueError('calculation failed')]):
            with self.assertRaises(ValueError):
                self.trader.stop_audit.collect(4000)
        self.assertFalse(self.db.in_transaction)
        self.competing_write()

    def test_scalp_prediction_reads_published_model_without_training_or_future_leak(self):
        from bot.probability import FEATURE_NAMES, ProbabilityModel
        audit = AuditLog(self.path)
        self.addCleanup(audit.close)
        n = len(FEATURE_NAMES)
        model = ProbabilityModel(FEATURE_NAMES, (0.,)*n, (0.,)*n, (1.,)*n,
                                 (0.,)*n, 0., 500, 100, 50., 50., .25, ())
        audit.model_journal.register('scalp-v1', 100, model, .5)
        with patch('bot.scalp_shadow.train_probability_model', side_effect=AssertionError('must not train')):
            self.assertEqual(audit.scalp_shadow.predict(101, {}), 50.)
            self.assertIsNone(audit.scalp_shadow.predict(99, {}))
            audit.scalp_shadow.learning_status(101)
        self.assertFalse(audit.connection.in_transaction)
