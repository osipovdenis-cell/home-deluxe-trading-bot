import math
import unittest

from bot.market import MarketMonitor


def monitor():
    market = MarketMonitor("https://api.binance.com", ("X",), 300, 3, 1800, False, 0, 0.5, 5, 20)
    market.change_12h_percent = {"X": 5.0}
    return market


def run(market, price_at, seconds):
    signals = []
    for t in range(seconds):
        for signal in market.update({"X": price_at(t)}, now=float(t)):
            signals.append((t, signal))
    return signals


def rocket(rate, amplitude=0.0, period=90):
    def price(t):
        if t < 300:
            return 100.0
        k = t - 300
        return 100 * (1 + rate * k) * (1 + amplitude * math.sin(2 * math.pi * k / period))
    return price


class LeaderAfterOrdinarySignalTests(unittest.TestCase):
    def stalled_ordinary_candidate(self):
        market = monitor()
        self.addCleanup(market.close)
        signals = run(market, lambda t: 100.0 if t < 300 else 100.6, 321)
        self.assertEqual(signals, [])
        self.assertIn("X", market.rescue_candidates)
        self.assertIsNone(market.rescue_candidates["X"].signal_kind)
        return market

    def test_second_chance_promotes_recovered_ordinary_candidate_to_leader(self):
        market = self.stalled_ordinary_candidate()
        market.update({"X": 100.6}, now=325)
        market.update({"X": 100.6}, now=330)
        signals = market.update({"X": 103.2}, now=335)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].kind, "аномальный лидер")
        self.assertTrue(signals[0].is_rescue)
        self.assertTrue(market.drain_confirmation_events()[-1].accepted)
        self.assertEqual(market.update({"X": 103.3}, now=336), [])
        self.assertNotIn("X", market.pending_candidates)

    def test_second_chance_promotion_does_not_extend_expiry_or_bypass_recovery(self):
        market = self.stalled_ordinary_candidate()
        started_at = market.rescue_candidates["X"].started_at
        market.market_stats = {"X": (1e7, 20.0)}
        self.assertEqual(market.update({"X": 100.6}, now=400), [])
        self.assertEqual(market.rescue_candidates["X"].started_at, started_at)
        self.assertEqual(market.update({"X": 103.2}, now=started_at + 90), [])
        self.assertNotIn("X", market.rescue_candidates)
        self.assertFalse(market.drain_confirmation_events()[-1].accepted)
        self.assertEqual(market.update({"X": 103.3}, now=started_at + 91), [])
        self.assertNotIn("X", market.pending_candidates)

    def test_gradual_rocket_gets_leader_signal_without_30_minute_wait(self):
        market = monitor()
        signals = run(market, rocket(0.0001, 0.004), 2400)
        leader = [(t, s) for t, s in signals if "лидер" in s.kind]
        self.assertTrue(signals and "лидер" not in signals[0][1].kind)
        self.assertTrue(leader)
        first_leader_minutes = (leader[0][0] - 300) / 60
        self.assertLess(first_leader_minutes, 10)
        self.assertGreater(leader[0][0], signals[0][0])

    def test_sharp_rocket_is_classified_at_confirmation_time(self):
        market = monitor()
        signals = run(market, rocket(0.0012), 360)
        self.assertEqual(signals[0][1].kind, "аномальный лидер")

    def test_one_leader_signal_then_usual_leader_cooldown(self):
        market = monitor()
        signals = run(market, rocket(0.0001), 2000)  # monotonic, no pullback
        leader = [s for _, s in signals if "лидер" in s.kind]
        self.assertEqual(len(leader), 1)
        self.assertFalse(leader[0].is_leader_reentry)

    def test_ordinary_coin_keeps_ordinary_cooldown(self):
        market = monitor()
        # +1% then flat: never a leader, one ordinary signal per cooldown.
        price = lambda t: 100.0 if t < 300 else 100 * (1 + min(0.01, 0.0005 * (t - 300)))
        signals = run(market, price, 1500)
        self.assertEqual(len(signals), 1)
        self.assertNotIn("лидер", signals[0][1].kind)

    def test_top24_leader_behaviour_unchanged(self):
        market = monitor()
        market.market_stats = {"X": (1e7, 20.0)}
        signals = run(market, rocket(0.0001, 0.004), 800)
        self.assertEqual(signals[0][1].kind, "лидер")
        self.assertTrue(any(s.is_leader_reentry for _, s in signals[1:]))


if __name__ == "__main__":
    unittest.main()
