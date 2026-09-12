import unittest
import sys
from types import SimpleNamespace
from unittest.mock import Mock

try:
    import httpx  # noqa: F401
except ModuleNotFoundError:
    class DummyHTTPError(Exception):
        pass

    class DummyClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    sys.modules["httpx"] = SimpleNamespace(
        Client=DummyClient,
        HTTPError=DummyHTTPError,
    )

from bot.market import MarketMonitor, SignalMarketContext


class MarketMonitorTests(unittest.TestCase):
    def test_rejects_wide_spread_before_entry(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("BTTUSDT",), 300, 3, 1800
        )
        try:
            monitor.tick_sizes["BTTUSDT"] = 0.00000001
            context = SignalMarketContext(1_000_000, 2, 100, 60, 312.5)
            safe, reason, _tick = monitor.execution_safety(
                "BTTUSDT", 0.00000033, context
            )
            self.assertFalse(safe)
            self.assertIn("спред", reason)
        finally:
            monitor.close()

    def test_rejects_large_price_tick_before_entry(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("COARSEUSDT",), 300, 3, 1800
        )
        try:
            monitor.tick_sizes["COARSEUSDT"] = 0.01
            context = SignalMarketContext(1_000_000, 2, 100, 60, 5)
            safe, reason, tick = monitor.execution_safety(
                "COARSEUSDT", 1, context
            )
            self.assertFalse(safe)
            self.assertAlmostEqual(tick, 1)
            self.assertIn("шаг цены", reason)
        finally:
            monitor.close()

    def test_emits_pump_signal_after_full_window(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("DOGEUSDT",), 300, 3, 1800,
            entry_confirmation_seconds=0,
        )
        try:
            self.assertEqual(monitor.update({"DOGEUSDT": 100}, now=0), [])
            self.assertEqual(monitor.update({"DOGEUSDT": 104}, now=300), [])
            signals = monitor.update({"DOGEUSDT": 104.1}, now=300)
            self.assertEqual(len(signals), 1)
            self.assertAlmostEqual(signals[0].change_percent, 4.1)
        finally:
            monitor.close()

    def test_respects_alert_cooldown(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("DOGEUSDT",), 300, 3, 1800,
            entry_confirmation_seconds=0,
        )
        try:
            monitor.update({"DOGEUSDT": 100}, now=0)
            self.assertEqual(monitor.update({"DOGEUSDT": 104}, now=300), [])
            self.assertEqual(len(monitor.update({"DOGEUSDT": 104.1}, now=300)), 1)
            self.assertEqual(monitor.update({"DOGEUSDT": 105}, now=600), [])
        finally:
            monitor.close()

    def test_early_and_strong_signals_are_classified(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com",
            ("DOGEUSDT", "PEPEUSDT"),
            300,
            3,
            1800,
            early_threshold_percent=1,
            entry_confirmation_seconds=0,
        )
        try:
            monitor.update({"DOGEUSDT": 100, "PEPEUSDT": 100}, now=0)
            monitor.update({"DOGEUSDT": 101.5, "PEPEUSDT": 104}, now=300)
            signals = monitor.update(
                {"DOGEUSDT": 101.6, "PEPEUSDT": 104.1}, now=300
            )
            kinds = {signal.symbol: signal.kind for signal in signals}
            self.assertEqual(kinds["DOGEUSDT"], "ранний")
            self.assertEqual(kinds["PEPEUSDT"], "сильный")
        finally:
            monitor.close()

    def test_limits_number_of_signals_per_cycle(self) -> None:
        symbols = tuple(f"COIN{index}USDT" for index in range(10))
        monitor = MarketMonitor(
            "https://api.binance.com",
            symbols,
            300,
            3,
            1800,
            early_threshold_percent=1,
            max_signals_per_cycle=3,
            entry_confirmation_seconds=0,
        )
        try:
            monitor.update({symbol: 100 for symbol in symbols}, now=0)
            monitor.update(
                {symbol: 102 + index for index, symbol in enumerate(symbols)},
                now=300,
            )
            signals = monitor.update(
                {symbol: 102.1 + index for index, symbol in enumerate(symbols)},
                now=300,
            )
            self.assertEqual(len(signals), 3)
            self.assertGreaterEqual(
                signals[0].change_percent, signals[-1].change_percent
            )
        finally:
            monitor.close()

    def test_dynamic_universe_filters_non_usdt_and_low_volume(self) -> None:
        exchange_response = Mock()
        exchange_response.raise_for_status.return_value = None
        exchange_response.json.return_value = {
            "symbols": [
                {
                    "symbol": "AAAUSDT",
                    "baseAsset": "AAA",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "isSpotTradingAllowed": True,
                },
                {
                    "symbol": "BBBUSD",
                    "baseAsset": "BBB",
                    "quoteAsset": "USD",
                    "status": "TRADING",
                    "isSpotTradingAllowed": True,
                },
            ]
        }
        ticker_response = Mock()
        ticker_response.raise_for_status.return_value = None
        ticker_response.json.return_value = [
            {
                "symbol": "AAAUSDT",
                "lastPrice": "2",
                "quoteVolume": "750000",
                "priceChangePercent": "12",
            },
            {
                "symbol": "LOWUSDT",
                "lastPrice": "1",
                "quoteVolume": "100",
                "priceChangePercent": "50",
            },
        ]
        monitor = MarketMonitor(
            "https://api.binance.com",
            (),
            300,
            3,
            1800,
            scan_all_usdt=True,
            min_quote_volume_usdt=500000,
            early_threshold_percent=1,
        )
        monitor.client = Mock()
        monitor.client.get.side_effect = [exchange_response, ticker_response]
        prices = monitor.fetch_prices()
        self.assertEqual(prices, {"AAAUSDT": 2.0})
        self.assertEqual(monitor.eligible_count, 1)

    def test_builds_realtime_volume_context(self) -> None:
        rows = []
        for index in range(25):
            quote_volume = 100 if index < 20 else 300
            taker_buy_quote = quote_volume * 0.6
            rows.append(
                [index, "1", "1", "1", "1", "1", index, quote_volume, 10, "0", taker_buy_quote]
            )
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = rows
        monitor = MarketMonitor("https://api.binance.com", ("AAAUSDT",), 300, 3, 1800)
        monitor.client = Mock()
        monitor.client.get.return_value = response
        context = monitor.fetch_signal_context("AAAUSDT")
        self.assertEqual(context.quote_volume_5m_usdt, 1500)
        self.assertEqual(context.volume_ratio_5m, 3)
        self.assertEqual(context.trades_5m, 50)
        self.assertEqual(context.taker_buy_ratio_percent, 60)

    def test_builds_order_book_context(self) -> None:
        rows = []
        for index in range(25):
            rows.append([index, "1", "1", "1", "1", "1", index, 100, 10, "0", 55])
        candles = Mock()
        candles.raise_for_status.return_value = None
        candles.json.return_value = rows
        depth = Mock()
        depth.raise_for_status.return_value = None
        depth.json.return_value = {
            "bids": [["100", "2"], ["99", "1"]],
            "asks": [["100.1", "1"], ["101", "1"]],
        }
        monitor = MarketMonitor(
            "https://api.binance.com", ("AAAUSDT",), 300, 3, 1800
        )
        monitor.client = Mock()
        monitor.client.get.side_effect = [candles, depth]
        context = monitor.fetch_signal_context("AAAUSDT")
        self.assertAlmostEqual(context.spread_bps, 10)
        self.assertAlmostEqual(context.bid_depth_usdt, 299)
        self.assertAlmostEqual(context.ask_depth_usdt, 201.1)
        self.assertGreater(context.order_book_imbalance_percent, 0)

    def test_entry_quality_rejects_weak_buy_pressure(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("AAAUSDT",), 300, 3, 1800
        )
        try:
            for timestamp in range(0, 301, 15):
                monitor.history["AAAUSDT"].append(
                    (timestamp, 100 + timestamp / 300)
                )
            dynamics = monitor.entry_dynamics("AAAUSDT", 300)
            context = SignalMarketContext(
                100_000, 2, 500, 49, 2, 50_000, 40_000, 11
            )
            safe, reason = monitor.entry_quality(context, dynamics)
            self.assertFalse(safe)
            self.assertIn("покупатели", reason)
        finally:
            monitor.close()

    def test_confirmation_rejects_fading_impulse(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("AAAUSDT",), 300, 3, 1800,
            early_threshold_percent=0.5, entry_confirmation_seconds=20,
        )
        try:
            for timestamp in range(0, 301, 15):
                price = 100 if timestamp < 300 else 100.6
                monitor.update({"AAAUSDT": price}, now=timestamp)
            self.assertEqual(
                monitor.update({"AAAUSDT": 100.5}, now=321), []
            )
            rejected = monitor.drain_confirmation_rejections()
            self.assertEqual(rejected[0][1], "AAAUSDT")
            self.assertTrue(
                "нет продолжения" in rejected[0][2]
                or "импульс исчез" in rejected[0][2]
            )
            event = monitor.drain_confirmation_events()[0]
            self.assertFalse(event.accepted)
            self.assertEqual(event.symbol, "AAAUSDT")
            self.assertAlmostEqual(event.trigger_price, 100.6)
        finally:
            monitor.close()

    def test_confirmation_records_accepted_candidate(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("AAAUSDT",), 300, 3, 1800,
            early_threshold_percent=0.5, entry_confirmation_seconds=20,
        )
        try:
            for timestamp in range(0, 301, 15):
                monitor.update(
                    {"AAAUSDT": 100 if timestamp < 300 else 100.6},
                    now=timestamp,
                )
            signals = monitor.update({"AAAUSDT": 100.7}, now=321)
            self.assertEqual(len(signals), 1)
            event = monitor.drain_confirmation_events()[0]
            self.assertTrue(event.accepted)
            self.assertAlmostEqual(event.resolution_price, 100.7)
            self.assertGreater(event.progress_percent, 0)
            self.assertIsNotNone(signals[0].confirmation_change_5s_percent)
        finally:
            monitor.close()


if __name__ == "__main__":
    unittest.main()
