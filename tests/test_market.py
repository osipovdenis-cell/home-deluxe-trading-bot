import unittest
import sys
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

try:
    import httpx  # noqa: F401
except ModuleNotFoundError:
    class DummyHTTPError(Exception):
        pass

    class DummyHTTPStatusError(DummyHTTPError):
        pass

    class DummyClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    sys.modules["httpx"] = SimpleNamespace(
        Client=DummyClient,
        HTTPError=DummyHTTPError,
        HTTPStatusError=DummyHTTPStatusError,
    )

from bot.market import EntryDynamics, MarketMonitor, SignalMarketContext


class MarketMonitorTests(unittest.TestCase):
    def test_leader_quality_requires_effective_order_flow(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("LEADERUSDT",), 300, 3, 1800
        )
        try:
            context = SignalMarketContext(
                100000, 2, 100, 60, 10, 1000, 900, 5,
                flow_cvd_60s_percent=20,
                flow_trade_rate_acceleration=2,
                flow_price_change_60s_percent=0.3,
                flow_price_efficiency_per_10k=1.5,
                flow_spread_change_bps=-2,
            )
            dynamics = EntryDynamics(0.1, 0.2, 0.3, 1, 2, -0.05, 0, 0, 40)
            self.assertEqual(monitor.leader_entry_quality(context, dynamics), (True, None))
            for change in (None, -0.1, 0.0, 0.01, 0.05):
                with self.subTest(change_60s=change):
                    self.assertEqual(monitor.leader_entry_quality(
                        replace(context, flow_price_change_60s_percent=change), dynamics
                    ), (True, None))
            absorbed = replace(context, flow_price_efficiency_per_10k=0.0)
            safe, reason = monitor.leader_entry_quality(absorbed, dynamics)
            self.assertFalse(safe)
            self.assertIn("поглощается", reason)
        finally:
            monitor.close()

    def test_bad_rolling_ticker_symbol_does_not_crash_refresh(self) -> None:
        import httpx

        if not hasattr(httpx, "HTTPError"):
            httpx.HTTPError = RuntimeError

        monitor = MarketMonitor(
            "https://api.binance.com", ("GOODUSDT", "BADUSDT"),
            300, 3, 1800,
        )

        def response_for(_path, params=None):
            symbols = json.loads(params["symbols"])
            if "BADUSDT" in symbols:
                response = Mock()
                if hasattr(httpx, "HTTPStatusError") and hasattr(httpx, "Request"):
                    request = httpx.Request(
                        "GET", "https://api.binance.com/api/v3/ticker"
                    )
                    raw_response = httpx.Response(400, request=request)
                    error = httpx.HTTPStatusError(
                        "bad symbol", request=request, response=raw_response
                    )
                else:
                    error = httpx.HTTPError("bad symbol")
                    error.response = SimpleNamespace(status_code=400)
                response.raise_for_status.side_effect = error
                return response
            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = [
                {"symbol": symbol, "openPrice": "100", "lastPrice": "101"}
                for symbol in symbols
            ]
            return response

        monitor.client = Mock()
        monitor.client.get.side_effect = response_for
        changes = monitor.refresh_12h_changes(
            ("GOODUSDT", "BADUSDT"), now=1000
        )
        self.assertAlmostEqual(changes["GOODUSDT"], 1)
        self.assertNotIn("BADUSDT", changes)

    def test_refreshes_rolling_12h_changes_from_binance(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = [
            {"symbol": "GREENUSDT", "openPrice": "100", "lastPrice": "102"},
            {"symbol": "REDUSDT", "openPrice": "100", "lastPrice": "99"},
        ]
        monitor = MarketMonitor(
            "https://api.binance.com", ("GREENUSDT", "REDUSDT"),
            300, 3, 1800,
        )
        monitor.client = Mock()
        monitor.client.get.return_value = response
        changes = monitor.refresh_12h_changes(
            ("GREENUSDT", "REDUSDT"), now=1000
        )
        self.assertAlmostEqual(changes["GREENUSDT"], 2)
        self.assertAlmostEqual(changes["REDUSDT"], -1)
        params = monitor.client.get.call_args.kwargs["params"]
        self.assertEqual(params["windowSize"], "12h")
        self.assertEqual(params["type"], "MINI")
        self.assertEqual(monitor.last_12h_refresh, 1000)

    def test_excludes_red_12h_coin_before_signal(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("REDUSDT",), 300, 3, 1800,
            early_threshold_percent=0.5, entry_confirmation_seconds=0,
        )
        try:
            monitor.change_12h_percent = {"REDUSDT": -0.01}
            monitor.market_stats["REDUSDT"] = (2_000_000, 10)
            for timestamp in range(300):
                monitor.update({"REDUSDT": 100}, now=timestamp)
            monitor.update({"REDUSDT": 104}, now=300)
            self.assertEqual(
                monitor.update({"REDUSDT": 104.1}, now=300), []
            )
            self.assertNotIn("REDUSDT", monitor.pending_candidates)
            self.assertNotIn("REDUSDT", monitor.leaders)
        finally:
            monitor.close()

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
            self.assertEqual(kinds["PEPEUSDT"], "аномальный лидер")
        finally:
            monitor.close()

    def test_single_large_candle_becomes_anomalous_leader(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("ROCKETUSDT",), 300, 3, 1800,
            early_threshold_percent=0.5, entry_confirmation_seconds=0,
        )
        try:
            monitor.market_stats["ROCKETUSDT"] = (2_000_000, 4)
            monitor.update({"ROCKETUSDT": 100}, now=0)
            monitor.update({"ROCKETUSDT": 104}, now=300)
            signals = monitor.update({"ROCKETUSDT": 104.1}, now=300)
            self.assertEqual(len(signals), 1)
            self.assertEqual(signals[0].kind, "аномальный лидер")
            self.assertIn("ROCKETUSDT", monitor.leaders)
        finally:
            monitor.close()

    def test_anomaly_uses_ten_minutes_without_changing_scalp_history(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("ROCKETUSDT",), 300, 3, 1800,
            early_threshold_percent=3, entry_confirmation_seconds=20,
        )
        try:
            monitor.market_stats["ROCKETUSDT"] = (2_000_000, 4)
            monitor.change_12h_percent["ROCKETUSDT"] = 1
            for now, price in ((0, 100), (280, 102.8), (300, 102.8)):
                monitor.update({"ROCKETUSDT": price}, now=now)
            self.assertEqual(monitor.update({"ROCKETUSDT": 103.2}, now=580), [])
            signals = monitor.update({"ROCKETUSDT": 103.3}, now=600)
            self.assertEqual(len(signals), 1)
            self.assertEqual(signals[0].kind, "аномальный лидер")
            self.assertEqual(signals[0].window_seconds, 600)
            self.assertAlmostEqual(signals[0].change_percent, 3.3)
            self.assertEqual(monitor.window_seconds, 300)
            self.assertTrue(all(t >= 300 for t, _ in monitor.history["ROCKETUSDT"]))
        finally:
            monitor.close()

    def test_anomaly_excludes_prices_older_than_ten_minutes(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("ROCKETUSDT",), 300, 3, 1800,
            early_threshold_percent=3,
        )
        try:
            monitor.market_stats["ROCKETUSDT"] = (2_000_000, 4)
            for now, price in ((0, 100), (300, 102.8), (320, 102.8), (601, 103.2)):
                self.assertEqual(monitor.update({"ROCKETUSDT": price}, now=now), [])
            self.assertNotIn("ROCKETUSDT", monitor.leaders)
            self.assertTrue(all(t >= 1 for t, _ in monitor.anomaly_history["ROCKETUSDT"]))
        finally:
            monitor.close()

    def test_leader_can_reenter_after_pullback_and_reacceleration(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("LEADERUSDT",), 300, 3, 1800,
            early_threshold_percent=0.5, entry_confirmation_seconds=0,
        )
        try:
            monitor.market_stats["LEADERUSDT"] = (5_000_000, 20)
            for timestamp in range(300):
                monitor.update({"LEADERUSDT": 100}, now=timestamp)
            monitor.update({"LEADERUSDT": 104}, now=300)
            first = monitor.update({"LEADERUSDT": 104.1}, now=300)
            self.assertEqual(len(first), 1)
            for timestamp in range(301, 310):
                monitor.update({"LEADERUSDT": 104.1}, now=timestamp)
            monitor.update({"LEADERUSDT": 105}, now=310)
            for timestamp in range(311, 320):
                monitor.update({"LEADERUSDT": 105}, now=timestamp)
            monitor.update({"LEADERUSDT": 104.6}, now=320)
            for timestamp in range(321, 330):
                price = 104.6 + (timestamp - 320) * 0.02
                monitor.update({"LEADERUSDT": price}, now=timestamp)
            monitor.update({"LEADERUSDT": 104.8}, now=330)
            second = monitor.update({"LEADERUSDT": 104.9}, now=330)
            self.assertEqual(len(second), 1)
            self.assertIn("лидер", second[0].kind)
            self.assertTrue(second[0].is_leader_reentry)
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

    def test_builds_multi_hour_trend_context(self) -> None:
        rows = []
        for index in range(241):
            close = 1 + index / 1000
            rows.append(
                [index, str(close), str(close), str(close), str(close), "1",
                 index, 100, 10, "0", 55]
            )
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = rows
        monitor = MarketMonitor(
            "https://api.binance.com", ("AAAUSDT",), 300, 3, 1800
        )
        monitor.client = Mock()
        monitor.client.get.return_value = response
        context = monitor.fetch_signal_context("AAAUSDT")
        self.assertGreater(context.trend_change_15m_percent, 1)
        self.assertGreater(context.trend_change_60m_percent, 5)
        self.assertGreater(context.trend_change_240m_percent, 20)
        self.assertAlmostEqual(context.trend_efficiency_240m_percent, 100)

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
        aggregate = Mock()
        aggregate.raise_for_status.return_value = None
        aggregate.json.return_value = [
            {"p": "1", "q": "100", "T": index * 1000, "m": False}
            for index in range(18)
        ] + [
            {"p": "1", "q": "2000", "T": 18_000, "m": False},
            {"p": "1", "q": "2000", "T": 19_000, "m": True},
        ]
        monitor = MarketMonitor(
            "https://api.binance.com", ("AAAUSDT",), 300, 3, 1800
        )
        monitor.client = Mock()
        monitor.client.get.side_effect = [candles, depth, aggregate]
        context = monitor.fetch_signal_context("AAAUSDT")
        self.assertAlmostEqual(context.spread_bps, 10)
        self.assertAlmostEqual(context.bid_depth_usdt, 299)
        self.assertAlmostEqual(context.ask_depth_usdt, 201.1)
        self.assertGreater(context.order_book_imbalance_percent, 0)
        self.assertEqual(context.large_trade_threshold_usdt, 2000)
        self.assertEqual(context.large_trade_count_60s, 2)
        self.assertEqual(context.large_trade_imbalance_60s_percent, 0)
        self.assertIsNotNone(context.bid_wall_share_percent)

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

    def test_rejected_candidate_gets_one_rescue_on_fresh_acceleration(self) -> None:
        monitor = MarketMonitor(
            "https://api.binance.com", ("AAAUSDT",), 300, 3, 1800,
            early_threshold_percent=0.5, entry_confirmation_seconds=20,
            rescue_window_seconds=90,
        )
        try:
            for timestamp in range(0, 301, 15):
                price = 100 if timestamp < 300 else 100.6
                monitor.update({"AAAUSDT": price}, now=timestamp)
            self.assertEqual(monitor.update({"AAAUSDT": 100.5}, now=321), [])
            self.assertEqual(monitor.update({"AAAUSDT": 100.51}, now=326), [])
            signals = monitor.update({"AAAUSDT": 100.62}, now=331)
            self.assertEqual(len(signals), 1)
            self.assertEqual(signals[0].symbol, "AAAUSDT")
            self.assertTrue(signals[0].is_rescue)
            events = monitor.drain_confirmation_events()
            self.assertTrue(any(event.accepted for event in events))
            self.assertIn("повторное ускорение", events[-1].reason)
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
