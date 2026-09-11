import tempfile
import unittest
from pathlib import Path

from bot.trading import PaperTrader


def make_trader(path: str) -> PaperTrader:
    trader = PaperTrader(path, 150, 50, 3, 55, 1, 1.5, 3, 5, 1, 0, 0.2)
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

    def test_takes_three_staged_profits(self) -> None:
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
                final = trader.update_positions({"TESTUSDT": 105}, 180)
                self.assertEqual(len(final), 1)
                self.assertEqual(final[0].remaining_percent, 0)
                self.assertEqual(final[0].reason, "фиксация +5%")
                self.assertGreater(final[0].total_position_pnl_usdt, 0)
                summary = trader.summary({"TESTUSDT": 105}, 200)
                self.assertEqual(summary.closed_positions, 1)
                self.assertEqual(summary.profitable_positions, 1)
                self.assertGreater(summary.equity_usdt, 150)
            finally:
                trader.close()

    def test_protects_last_twenty_percent_at_second_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.open_on_signal("TESTUSDT", 100, "ранний", 63, 0)
                trader.update_positions({"TESTUSDT": 101.5}, 60)
                second = trader.update_positions({"TESTUSDT": 103}, 120)
                self.assertAlmostEqual(second[0].remaining_percent, 20)
                final = trader.update_positions({"TESTUSDT": 102.99}, 180)
                self.assertEqual(len(final), 1)
                self.assertEqual(final[0].reason, "защита прибыли +3%")
                self.assertEqual(final[0].remaining_percent, 0)
            finally:
                trader.close()

    def test_never_intentionally_gives_back_first_profit_level(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.open_on_signal("TESTUSDT", 100, "ранний", 63, 0)
                first = trader.update_positions({"TESTUSDT": 101.5}, 60)
                self.assertEqual(first[0].remaining_percent, 60)
                final = trader.update_positions({"TESTUSDT": 101.49}, 120)
                self.assertEqual(len(final), 1)
                self.assertEqual(final[0].reason, "защита прибыли +1,5%")
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
            trader = make_trader(str(Path(directory) / "trades.db"))
            try:
                trader.open_on_signal("TESTUSDT", 100, "ранний", 60, 0)
                notices = trader.update_positions({"TESTUSDT": 100.2}, 86400)
                self.assertEqual(notices, [])
                summary = trader.summary({"TESTUSDT": 100.2}, 86400)
                self.assertEqual(summary.open_positions, 1)
            finally:
                trader.close()


if __name__ == "__main__":
    unittest.main()
