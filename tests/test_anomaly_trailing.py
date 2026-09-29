import json
import tempfile
import unittest
from pathlib import Path

from bot.exit_policy import ANOMALY_LEGACY_VERSION, ANOMALY_VERSION, policy_for
from bot.rocket_cards import build_card, format_card, schema
from bot.trading import PaperTrader


def trader(path=':memory:'):
    return PaperTrader(path, 200, 50, 4, 70, .5, .7, 1, 1.5, 1, 0, .2)


def set_v1(t, symbol):
    row = t.connection.execute('SELECT * FROM paper_positions WHERE symbol=?', (symbol,)).fetchone()
    policy = dict(policy_for(row), version=ANOMALY_LEGACY_VERSION)
    t.connection.execute('UPDATE paper_positions SET exit_policy_json=? WHERE id=?',
                         (json.dumps(policy), row['id']))
    t.connection.commit()


class AnomalyTrailingTests(unittest.TestCase):
    def test_thresholds_include_exact_activation_and_large_rises(self):
        for peak, level in [(108, 107), (108.09, 107.09), (109.5, 108.5), (120, 119)]:
            with self.subTest(peak=peak):
                t = trader()
                try:
                    t.open_on_signal('A', 100, 'аномальный лидер', 80, 0)
                    self.assertEqual(t.update_positions({'A':peak}, 1), [])
                    self.assertEqual(t.update_positions({'A':level + .001}, 2), [])
                    notices = t.update_positions({'A':level}, 3)
                    self.assertEqual(len(notices), 1)
                    self.assertIn(f'уровень выхода {level - 100:+.2f}%', notices[0].reason)
                    self.assertIn('трейлинг включён от +8%', notices[0].reason)
                    self.assertEqual(notices[0].remaining_percent, 0)
                finally:
                    t.close()

    def test_small_init_like_dip_then_new_peak_raises_exit(self):
        t = trader(); self.addCleanup(t.close)
        t.open_on_signal('INIT', 100, 'аномальный лидер', 80, 0)
        for at, price in enumerate([108.09, 108, 107.81, 110.52, 109.82, 109.53], 1):
            self.assertEqual(t.update_positions({'INIT':price}, at), [])
        notice, = t.update_positions({'INIT':109.52}, 7)
        self.assertIn('уровень выхода +9.52%', notice.reason)

    def test_before_activation_only_original_stop_and_gap_execution(self):
        t = trader(); self.addCleanup(t.close)
        t.open_on_signal('A', 100, 'аномальный лидер', 80, 0)
        for at, price in enumerate([107.99, 106, 93.01], 1):
            self.assertEqual(t.update_positions({'A':price}, at), [])
        notice, = t.update_positions({'A':92.5}, 4)
        self.assertEqual(notice.reason, 'стоп-лосс')
        self.assertEqual(notice.price, 92.5)
        t.open_on_signal('B', 100, 'аномальный лидер', 80, 5)
        t.update_positions({'B':108}, 6)
        notice, = t.update_positions({'B':106.5}, 7)
        self.assertEqual(notice.price, 106.5)
        self.assertAlmostEqual(notice.pnl_percent, 6.3)

    def test_migration_preserves_state_history_and_restart_activation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'db')
            t = trader(path)
            for symbol in ['ARMED', 'WAIT', 'CLOSED']:
                t.open_on_signal(symbol, 100, 'аномальный лидер', 80, 0)
                set_v1(t, symbol)
            t.open_on_signal('LEADER', 100, 'лидер', 80, 0)
            t.update_positions({'ARMED':108.09, 'WAIT':107.99, 'CLOSED':108}, 1)
            t.update_positions({'CLOSED':107.99}, 2)
            before = {r['symbol']:dict(r) for r in t.connection.execute('SELECT * FROM paper_positions')}
            t.close()
            t = trader(path)
            after = {r['symbol']:dict(r) for r in t.connection.execute('SELECT * FROM paper_positions')}
            for symbol in ['ARMED', 'WAIT']:
                old_policy = json.loads(before[symbol]['exit_policy_json'])
                upgraded = json.loads(after[symbol]['exit_policy_json'])
                self.assertEqual(upgraded['version'], ANOMALY_VERSION)
                self.assertEqual(upgraded['previous_policy'], old_policy)
                self.assertGreater(upgraded['policy_changed_at'], 0)
                self.assertEqual({k:v for k,v in before[symbol].items() if k!='exit_policy_json'},
                                 {k:v for k,v in after[symbol].items() if k!='exit_policy_json'})
            for symbol in ['CLOSED', 'LEADER']:
                self.assertEqual(before[symbol], after[symbol])
            schema(t.connection)
            for symbol, label in [('ARMED', 'включение трейлинга от +8%'), ('CLOSED', 'защита +8%')]:
                card = build_card(t.connection, after[symbol], 5000)
                self.assertIn(label, format_card(card))
                self.assertIn('exit_policy', card)
                if symbol == 'ARMED':
                    self.assertIn('Правило выхода обновлено:', format_card(card))
            self.assertEqual(t.stop_audit.collect(5000), ([], 0, 0))
            self.assertEqual(t.update_positions({'ARMED':107.81, 'WAIT':106}, 3), [])
            t.close()
            t = trader(path); self.addCleanup(t.close)
            for symbol in ['ARMED', 'WAIT']:
                saved = t.connection.execute('SELECT exit_policy_json FROM paper_positions WHERE symbol=?', (symbol,)).fetchone()[0]
                self.assertEqual(saved, after[symbol]['exit_policy_json'])
            notice, = t.update_positions({'ARMED':107.09, 'WAIT':105}, 4)
            self.assertEqual(notice.symbol, 'ARMED')
            self.assertEqual(t.update_positions({'WAIT':108}, 5), [])
            t.close()
            t = trader(path); self.addCleanup(t.close)
            self.assertEqual(len(t.update_positions({'WAIT':107}, 6)), 1)
