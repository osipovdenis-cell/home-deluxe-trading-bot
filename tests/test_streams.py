import json
import unittest

from bot.streams import AllMarketMiniTickerStream, PositionBookTickerStream


class AllMarketMiniTickerStreamTests(unittest.TestCase):
    def test_keeps_only_active_symbols_above_volume_filter(self) -> None:
        stream = AllMarketMiniTickerStream({"AAAUSDT", "LOWUSDT"}, 500_000)
        stream.ingest(
            [
                {"s": "AAAUSDT", "c": "102", "o": "100", "q": "750000"},
                {"s": "LOWUSDT", "c": "5", "o": "4", "q": "100"},
                {"s": "OTHERUSDT", "c": "9", "o": "8", "q": "999999"},
            ]
        )
        prices, stats = stream.snapshot()
        self.assertEqual(prices, {"AAAUSDT": 102.0})
        self.assertEqual(stats["AAAUSDT"][0], 750000.0)
        self.assertAlmostEqual(stats["AAAUSDT"][1], 2.0)


class PositionBookTickerStreamTests(unittest.TestCase):
    def test_uses_best_bid_as_executable_sell_price(self) -> None:
        stream = PositionBookTickerStream()
        stream.set_symbols(("AAAUSDT",))
        stream.ingest(
            json.dumps(
                {"stream": "aaausdt@bookTicker", "data": {"s": "AAAUSDT", "b": "1.25", "a": "1.26"}}
            )
        )
        self.assertEqual(stream.drain(), {"AAAUSDT": 1.25})
        self.assertEqual(stream.drain(), {})

    def test_preserves_intracycle_high_and_low_for_exit_rules(self) -> None:
        stream = PositionBookTickerStream()
        stream.set_symbols(("AAAUSDT",))
        stream.ingest({"s": "AAAUSDT", "b": "1.015"})
        stream.ingest({"s": "AAAUSDT", "b": "1.010"})
        prices = [price for _, _, price in stream.drain_events()]
        self.assertIn(1.015, prices)
        self.assertIn(1.010, prices)

    def test_builds_combined_subscription_for_three_positions(self) -> None:
        stream = PositionBookTickerStream()
        stream.set_symbols(("AAAUSDT", "BBBUSDT", "CCCUSDT"))
        url = stream.subscription_url()
        self.assertIn("aaausdt@bookTicker", url)
        self.assertIn("bbbusdt@bookTicker", url)
        self.assertIn("cccusdt@bookTicker", url)

    def test_rejects_more_than_three_positions(self) -> None:
        stream = PositionBookTickerStream()
        with self.assertRaises(ValueError):
            stream.set_symbols(("AUSDT", "BUSDT", "CUSDT", "DUSDT"))


if __name__ == "__main__":
    unittest.main()
