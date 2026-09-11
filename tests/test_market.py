import unittest
import sys
from types import SimpleNamespace
from unittest.mock import Mock

try:
    import httpx  # noqa: F401
except ModuleNotFoundError:
    class DummyClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def close(self) -> None:
            pass

    sys.modules["httpx"] = SimpleNamespace(Client=DummyClient)

from bot.market import MarketMonitor


class MarketMonitorTests(unittest.TestCase):
    def test_emits_pump_signal_after_full_window(self) -> None:
        monitor = MarketMonitor("https://api.binance.com", ("DOGEUSDT",), 300, 3, 1800)
        try:
            self.assertEqual(monitor.update({"DOGEUSDT": 100}, now=0), [])
            signals = monitor.update({"DOGEUSDT": 104}, now=300)
            self.assertEqual(len(signals), 1)
            self.assertAlmostEqual(signals[0].change_percent, 4)
        finally:
            monitor.close()

    def test_respects_alert_cooldown(self) -> None:
        monitor = MarketMonitor("https://api.binance.com", ("DOGEUSDT",), 300, 3, 1800)
        try:
            monitor.update({"DOGEUSDT": 100}, now=0)
            self.assertEqual(len(monitor.update({"DOGEUSDT": 104}, now=300)), 1)
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
        )
        try:
            monitor.update({"DOGEUSDT": 100, "PEPEUSDT": 100}, now=0)
            signals = monitor.update({"DOGEUSDT": 101.5, "PEPEUSDT": 104}, now=300)
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
        )
        try:
            monitor.update({symbol: 100 for symbol in symbols}, now=0)
            signals = monitor.update(
                {symbol: 102 + index for index, symbol in enumerate(symbols)},
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


if __name__ == "__main__":
    unittest.main()
