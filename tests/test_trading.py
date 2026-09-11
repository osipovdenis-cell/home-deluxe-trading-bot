import tempfile
import unittest
from pathlib import Path

from bot.trading import PaperTrader


def make_trader(path: str) -> PaperTrader:
    trader = PaperTrader(path, 150, 50, 3, 55, 1, 1.5, 3, 1, 900, 0.2)
    trader.connection.execute(
        "UPDATE paper_account SET report_started_at = 0 WHERE id = 1"
    )
    trader.connection.commit()
    return trader


class PaperTraderTests(unittest.TestCase):
    def test_rejects_signal_below_minimum_score(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                notice = trader.open_on_signal("TESTUSDT", 100, "ранний", 54, 0)
                self.assertIsNone(notice)
            finally:
                trader.close()

    def test_takes_partial_profit_and_closes_on_trailing_drawdown(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                self.assertIsNotNone(
                    trader.open_on_signal("TESTUSDT", 100, "ранний", 63, 0)
                )
                first = trader.update_positions({"TESTUSDT": 101.5}, 60)
                self.assertEqual(len(first), 1)
                self.assertEqual(first[0].remaining_percent, 60)
                second = trader.update_positions({"TESTUSDT": 103}, 120)
                self.assertEqual(len(second), 1)
                self.assertAlmostEqual(second[0].remaining_percent, 20)
                final = trader.update_positions({"TESTUSDT": 101.9}, 180)
                self.assertEqual(len(final), 1)
                self.assertEqual(final[0].remaining_percent, 0)
                self.assertGreater(final[0].total_position_pnl_usdt, 0)
                summary = trader.summary({"TESTUSDT": 101.9}, 200)
                self.assertEqual(summary.closed_positions, 1)
                self.assertEqual(summary.profitable_positions, 1)
                self.assertGreater(summary.equity_usdt, 150)
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
                1,
                900,
                0.2,
            )
            try:
                trader.connection.execute(
                    "UPDATE paper_account SET report_started_at = 0 WHERE id = 1"
                )
                trader.connection.commit()
                for index in range(3):
                    self.assertIsNotNone(
                        trader.open_on_signal(f"COIN{index}USDT", 1, "ранний", 60, 0)
                    )
                self.assertIsNone(
                    trader.open_on_signal("COIN3USDT", 1, "ранний", 60, 0)
                )
                summary = trader.summary({}, 100)
                self.assertEqual(summary.cash_balance_usdt, 0)
                self.assertAlmostEqual(summary.equity_usdt, 149.7)
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


if __name__ == "__main__":
    unittest.main()
