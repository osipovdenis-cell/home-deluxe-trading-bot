import json
import unittest

from bot.streams import (
    AllMarketMiniTickerStream,
    LeaderOrderFlowStream,
    PositionBookTickerStream,
)


class LeaderOrderFlowStreamTests(unittest.TestCase):
    def test_tracks_aggressive_flow_price_response_and_depth_changes(self) -> None:
        stream = LeaderOrderFlowStream()
        stream.set_symbols(("LEADERUSDT",))
        url = stream.subscription_url()
        self.assertIn("leaderusdt@aggTrade", url)
        self.assertIn("leaderusdt@bookTicker", url)
        self.assertIn("leaderusdt@depth@100ms", url)

        stream.ingest({"e": "aggTrade", "s": "LEADERUSDT", "p": "100", "q": "10", "m": False}, 40)
        stream.ingest({"e": "aggTrade", "s": "LEADERUSDT", "p": "100.2", "q": "5", "m": True}, 50)
        stream.ingest({"e": "aggTrade", "s": "LEADERUSDT", "p": "100.4", "q": "20", "m": False}, 59)
        stream.ingest({"s": "LEADERUSDT", "b": "100", "B": "5", "a": "100.1", "A": "4"}, 40)
        stream.ingest({"s": "LEADERUSDT", "b": "100.35", "B": "6", "a": "100.4", "A": "3"}, 59)
        stream.ingest({"e": "depthUpdate", "s": "LEADERUSDT", "b": [["100", "10"]], "a": [["101", "10"]]}, 40)
        stream.ingest({"e": "depthUpdate", "s": "LEADERUSDT", "b": [["100", "12"]], "a": [["101", "6"]]}, 59)

        snapshot = stream.snapshot("LEADERUSDT", 60)
        self.assertIsNotNone(snapshot)
        self.assertGreater(snapshot.buy_60s_usdt, snapshot.sell_60s_usdt)
        self.assertGreater(snapshot.cvd_60s_percent, 0)
        self.assertGreater(snapshot.price_change_60s_percent, 0)
        self.assertEqual(snapshot.bid_support_percent, 100)
        self.assertEqual(snapshot.ask_depletion_percent, 100)
        self.assertLess(snapshot.spread_bps, 10)


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
