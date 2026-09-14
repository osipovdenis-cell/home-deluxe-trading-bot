import tempfile
import unittest
from pathlib import Path

from bot.trading import PaperTrader


def make_trader(path: str) -> PaperTrader:
    trader = PaperTrader(path, 150, 50, 3, 55, 0.5, 0.7, 1, 1.5, 1, 0, 0.2)
    trader.connection.execute(
        "UPDATE paper_account SET report_started_at = 0 WHERE id = 1"
    )
    trader.connection.commit()
    return trader


class PaperTraderTests(unittest.TestCase):
    def test_post_stop_report_tracks_further_fall_and_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.connection.execute(
                    "CREATE TABLE samples(timestamp REAL,symbol TEXT,price REAL)"
                )
                trader.open_on_signal(
                    "STOPUSDT", 100, "аномальный лидер", 20, 0,
                    bypass_min_score=True,
                )
                trader.update_positions({"STOPUSDT": 99.5}, 10)
                trader.connection.executemany(
                    "INSERT INTO samples(timestamp,symbol,price) VALUES(?,?,?)",
                    ((20, "STOPUSDT", 99.0), (300, "STOPUSDT", 98.0),
                     (1200, "STOPUSDT", 100.8), (3600, "STOPUSDT", 100.2)),
                )
                trader.connection.commit()
                report = trader.post_stop_report_text(3700, 0)
                self.assertIn("Созрело 60-минутных наблюдений: 1", report)
                self.assertIn("максимально на -1.51%", report)
                self.assertIn("к цене входа: 1/1", report)
                self.assertIn("достигли +0,7% от входа: 1/1", report)
                self.assertIn("возможных ложных стопов: 1/1", report)
                self.assertIn(
                    "цель +0,7% раньше нового стопа — 0/1", report
                )
                self.assertIn("стоп −1% раньше цели — 1/1", report)
            finally:
                trader.close()

    def test_post_stop_report_detects_target_before_wider_stop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.connection.execute(
                    "CREATE TABLE samples(timestamp REAL,symbol TEXT,price REAL)"
                )
                trader.open_on_signal("RECOVERUSDT", 100, "лидер", 20, 0,
                                      bypass_min_score=True)
                trader.update_positions({"RECOVERUSDT": 99.5}, 10)
                trader.connection.executemany(
                    "INSERT INTO samples(timestamp,symbol,price) VALUES(?,?,?)",
                    ((20, "RECOVERUSDT", 99.4),
                     (300, "RECOVERUSDT", 99.2),
                     (900, "RECOVERUSDT", 100.8),
                     (3600, "RECOVERUSDT", 100.9)),
                )
                trader.connection.commit()
                report = trader.post_stop_report_text(3700, 0)
                self.assertIn(
                    "цель +0,7% раньше нового стопа — 1/1", report
                )
                self.assertIn("стоп −1% раньше цели — 0/1", report)
            finally:
                trader.close()

    def test_rocket_keeps_full_position_and_trails_one_point_from_peak(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                opened = trader.open_on_signal(
                    "ROCKETUSDT", 100, "аномальный лидер", 20, 0,
                    bypass_min_score=True,
                )
                self.assertIsNotNone(opened)
                self.assertEqual(trader.update_positions({"ROCKETUSDT": 101}, 10), [])
                row = trader.connection.execute(
                    "SELECT remaining_quantity,take_1_done FROM paper_positions"
                ).fetchone()
                self.assertEqual(int(row[1]), 1)
                self.assertAlmostEqual(float(row[0]), opened.quantity)
                self.assertEqual(trader.update_positions({"ROCKETUSDT": 110}, 20), [])
                closed = trader.update_positions({"ROCKETUSDT": 109}, 30)
                self.assertEqual(len(closed), 1)
                self.assertEqual(closed[0].remaining_percent, 0)
                self.assertIn("откат 1 п.п.", closed[0].reason)
                self.assertGreater(closed[0].pnl_percent, 8)
                report = trader.rocket_report_text({"ROCKETUSDT": 109}, 40)
                self.assertIn("Входов: 1", report)
                self.assertIn("Резервных входов без ответа AI: 0", report)
                self.assertIn("выходов по откату: 1", report)
                self.assertIn("Максимальный рост после входа: 10.00%", report)
            finally:
                trader.close()

    def test_empty_intelligence_report_without_signal_table(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                intelligence = trader.build_intelligence(100)
                self.assertEqual(intelligence.closed_positions, 0)
                self.assertEqual(intelligence.trade_breakdown_texts(), [])
            finally:
                trader.close()

    def test_lists_open_symbols_for_realtime_subscription(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.open_on_signal("AAAUSDT", 1, "ранний", 60, 0)
                trader.open_on_signal("BBBUSDT", 1, "ранний", 60, 1)
                self.assertEqual(trader.open_symbols(), ("AAAUSDT", "BBBUSDT"))
            finally:
                trader.close()

    def test_rejects_signal_below_minimum_score(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                notice = trader.open_on_signal("TESTUSDT", 100, "ранний", 54, 0)
                self.assertIsNone(notice)
            finally:
                trader.close()

    def test_takes_two_staged_profits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                self.assertIsNotNone(
                    trader.open_on_signal("TESTUSDT", 100, "ранний", 63, 0)
                )
                first = trader.update_positions({"TESTUSDT": 100.7}, 60)
                self.assertEqual(len(first), 1)
                self.assertEqual(first[0].remaining_percent, 50)
                self.assertEqual(first[0].reason, "фиксация +0,7%")
                second = trader.update_positions({"TESTUSDT": 101}, 120)
                self.assertEqual(len(second), 1)
                self.assertEqual(second[0].remaining_percent, 0)
                self.assertEqual(second[0].reason, "фиксация +1%")
                self.assertGreater(second[0].total_position_pnl_usdt, 0)
                summary = trader.summary({"TESTUSDT": 101}, 200)
                self.assertEqual(summary.closed_positions, 1)
                self.assertEqual(summary.profitable_positions, 1)
                self.assertGreater(summary.equity_usdt, 150)
            finally:
                trader.close()

    def test_never_intentionally_gives_back_first_profit_level(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.open_on_signal("TESTUSDT", 100, "ранний", 63, 0)
                first = trader.update_positions({"TESTUSDT": 100.7}, 60)
                self.assertEqual(first[0].remaining_percent, 50)
                final = trader.update_positions({"TESTUSDT": 100.69}, 120)
                self.assertEqual(len(final), 1)
                self.assertEqual(final[0].reason, "защита прибыли +0,7%")
                self.assertEqual(final[0].remaining_percent, 0)
            finally:
                trader.close()

    def test_respects_total_budget(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = PaperTrader(
                str(Path(directory) / "trades.db"),
                150,
                50,
                5,
                55,
                1,
                1.5,
                3,
                5,
                1,
                900,
                0.2,
            )
            try:
                trader.connection.execute(
                    "UPDATE paper_account SET report_started_at = 0 WHERE id = 1"
                )
                trader.connection.commit()
                for index in range(5):
                    notice = trader.open_on_signal(
                        f"COIN{index}USDT", 1, "ранний", 60, 0
                    )
                    self.assertIsNotNone(notice)
                    self.assertAlmostEqual(notice.position_usdt, 30)
                self.assertIsNone(
                    trader.open_on_signal("COIN5USDT", 1, "ранний", 60, 0)
                )
                summary = trader.summary({}, 100)
                self.assertEqual(summary.cash_balance_usdt, 0)
                self.assertAlmostEqual(summary.equity_usdt, 149.7)
            finally:
                trader.close()

    def test_distributes_entire_available_bank_between_free_slots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.connection.execute(
                    "UPDATE paper_account SET cash_balance_usdt = 130 WHERE id = 1"
                )
                trader.connection.commit()
                notices = [
                    trader.open_on_signal(f"COIN{index}USDT", 1, "ранний", 60, 0)
                    for index in range(3)
                ]
                self.assertTrue(all(notice is not None for notice in notices))
                for notice in notices:
                    self.assertAlmostEqual(notice.position_usdt, 130 / 3)
                summary = trader.summary({}, 100)
                self.assertAlmostEqual(summary.cash_balance_usdt, 0)
            finally:
                trader.close()

    def test_trade_notice_includes_bank_balance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                notice = trader.open_on_signal("TESTUSDT", 100, "ранний", 60, 0)
                text = trader.notice_telegram_text(notice, {"TESTUSDT": 100}, 0)
                self.assertIn("Стартовый капитал: 150.00 USDT", text)
                self.assertIn("Текущий баланс:", text)
                self.assertIn("Прибыль/убыток:", text)
                self.assertIn("Свободно:", text)
                self.assertIn("В открытых позициях:", text)
            finally:
                trader.close()

    def test_builds_trade_intelligence_and_control_strategies(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.connection.execute(
                    "CREATE TABLE samples(timestamp REAL, symbol TEXT, price REAL)"
                )
                trader.connection.execute(
                    "CREATE TABLE signal_events(id INTEGER PRIMARY KEY, timestamp REAL, "
                    "symbol TEXT, change_percent REAL)"
                )
                trader.connection.execute(
                    "INSERT INTO signal_events(timestamp, symbol, change_percent) "
                    "VALUES(0, 'TESTUSDT', 3.2)"
                )
                trader.open_on_signal("TESTUSDT", 100, "ранний", 63, 0)
                for timestamp, price in ((60, 101.5), (120, 103), (180, 105)):
                    trader.connection.execute(
                        "INSERT INTO samples(timestamp, symbol, price) VALUES(?, ?, ?)",
                        (timestamp, "TESTUSDT", price),
                    )
                    trader.connection.commit()
                    trader.update_positions({"TESTUSDT": price}, timestamp)
                intelligence = trader.build_intelligence(200)
                self.assertEqual(intelligence.closed_positions, 1)
                self.assertGreater(intelligence.actual_pnl_usdt, 0)
                self.assertAlmostEqual(intelligence.average_mfe_percent, 1.5)
                self.assertAlmostEqual(intelligence.average_mae_percent, 0)
                self.assertGreater(intelligence.target_0_7_pnl_usdt, 0)
                self.assertGreater(intelligence.target_1_pnl_usdt, 0)
                self.assertGreater(intelligence.target_1_5_pnl_usdt, 0)
                self.assertIn("Параллельный пересчёт", intelligence.telegram_text())
                self.assertIn("всё на +1,5%", intelligence.telegram_text())
                self.assertIn("от 3%", intelligence.telegram_text())
                details = "\n".join(intelligence.trade_breakdown_texts())
                self.assertIn("TESTUSDT: вход +3.20%", details)
                self.assertIn("0,7% ✅ / 1% ✅", details)
            finally:
                trader.close()

    def test_reports_after_full_day(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.connection.execute(
                    "UPDATE paper_account SET report_started_at = 0 WHERE id = 1"
                )
                trader.connection.commit()
                self.assertFalse(trader.report_due(86399))
                self.assertTrue(trader.report_due(86400))
                trader.finish_report({}, 86400)
                self.assertFalse(trader.report_due(86401))
            finally:
                trader.close()

    def test_closes_on_stop_loss(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.open_on_signal("TESTUSDT", 100, "сильный", 70, 0)
                notices = trader.update_positions({"TESTUSDT": 99}, 60)
                self.assertEqual(len(notices), 1)
                self.assertEqual(notices[0].reason, "стоп-лосс")
                self.assertLess(notices[0].total_position_pnl_usdt, 0)
            finally:
                trader.close()

    def test_does_not_close_only_because_time_passed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = PaperTrader(
                str(Path(directory) / "trades.db"),
                150,
                50,
                3,
                55,
                1,
                1.5,
                3,
                5,
                1,
                1,
                0.2,
            )
            try:
                trader.open_on_signal("TESTUSDT", 100, "ранний", 60, 0)
                notices = trader.update_positions({"TESTUSDT": 100.2}, 86400)
                self.assertEqual(notices, [])
                summary = trader.summary({"TESTUSDT": 100.2}, 86400)
                self.assertEqual(summary.open_positions, 1)
            finally:
                trader.close()

    def test_exits_stagnant_impulse_only_with_net_profit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.connection.execute(
                    "CREATE TABLE samples(timestamp REAL, symbol TEXT, price REAL)"
                )
                trader.open_on_signal("TESTUSDT", 100, "ранний", 60, 0)
                trader.connection.executemany(
                    "INSERT INTO samples(timestamp, symbol, price) VALUES(?, ?, ?)",
                    (
                        (600, "TESTUSDT", 100.6),
                        (1000, "TESTUSDT", 100.4),
                        (1800, "TESTUSDT", 100.3),
                    ),
                )
                trader.connection.commit()
                notices = trader.update_positions(
                    {"TESTUSDT": 100.3}, 1800, {"TESTUSDT": (0.7, 45, -10)}
                )
                self.assertEqual(len(notices), 1)
                self.assertEqual(
                    notices[0].reason, "15 минут без роста (выход в плюс)"
                )
                self.assertGreater(notices[0].pnl_usdt, 0)
            finally:
                trader.close()

    def test_keeps_stagnant_position_when_exit_would_be_negative(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.connection.execute(
                    "CREATE TABLE samples(timestamp REAL, symbol TEXT, price REAL)"
                )
                trader.open_on_signal("TESTUSDT", 100, "ранний", 60, 0)
                trader.connection.executemany(
                    "INSERT INTO samples(timestamp, symbol, price) VALUES(?, ?, ?)",
                    (
                        (600, "TESTUSDT", 100.4),
                        (1000, "TESTUSDT", 99.9),
                        (1800, "TESTUSDT", 99.8),
                    ),
                )
                trader.connection.commit()
                notices = trader.update_positions(
                    {"TESTUSDT": 99.8}, 1800, {"TESTUSDT": (0.7, 45, -10)}
                )
                self.assertEqual(notices, [])
            finally:
                trader.close()

    def test_exits_profitable_stagnation_after_fifteen_minutes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.connection.execute(
                    "CREATE TABLE samples(timestamp REAL, symbol TEXT, price REAL)"
                )
                trader.open_on_signal("TESTUSDT", 100, "ранний", 60, 0)
                trader.connection.executemany(
                    "INSERT INTO samples(timestamp, symbol, price) VALUES(?, ?, ?)",
                    (
                        (300, "TESTUSDT", 100.6),
                        (700, "TESTUSDT", 100.5),
                        (900, "TESTUSDT", 100.4),
                    ),
                )
                trader.connection.commit()
                notices = trader.update_positions({"TESTUSDT": 100.4}, 900)
                self.assertEqual(len(notices), 1)
                self.assertEqual(
                    notices[0].reason, "15 минут без роста (выход в плюс)"
                )
                self.assertGreater(notices[0].pnl_usdt, 0)
            finally:
                trader.close()


if __name__ == "__main__":
    unittest.main()
