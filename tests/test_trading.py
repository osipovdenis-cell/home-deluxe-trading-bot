import tempfile
import unittest
from pathlib import Path

from bot.trading import PaperTrader


def make_trader(path: str) -> PaperTrader:
    return PaperTrader(path, 50, 3, 55, 1, 1.5, 3, 1, 900, 0.2)


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
                summary = trader.summary_since(0, 200)
                self.assertEqual(summary.closed_positions, 1)
                self.assertEqual(summary.profitable_positions, 1)
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
