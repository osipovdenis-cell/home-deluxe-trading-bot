import unittest
import sys
from types import SimpleNamespace

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


if __name__ == "__main__":
    unittest.main()
