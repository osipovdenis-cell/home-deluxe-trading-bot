import unittest
from types import SimpleNamespace

from bot.main import exceptional_new_entry, history_entry_policy


class ExceptionalEntryTests(unittest.TestCase):
    def _objects(self):
        analysis = SimpleNamespace(decision="BUY", score=85)
        context = SimpleNamespace(
            volume_ratio_5m=2,
            taker_buy_ratio_percent=60,
            order_book_imbalance_percent=15,
        )
        dynamics = SimpleNamespace(
            change_15s_percent=0.05,
            change_60s_percent=0.3,
            pullback_from_5m_high_percent=-0.05,
            btc_change_300s_percent=-0.1,
            market_breadth_60s_percent=45,
        )
        return analysis, context, dynamics

    def test_accepts_only_exceptionally_strong_cold_start(self) -> None:
        self.assertTrue(exceptional_new_entry(*self._objects()))

    def test_rejects_cold_start_when_one_condition_is_weak(self) -> None:
        analysis, context, dynamics = self._objects()
        context.volume_ratio_5m = 1.99
        self.assertFalse(exceptional_new_entry(analysis, context, dynamics))

    def test_cold_history_uses_base_score(self) -> None:
        behavior = SimpleNamespace(impulses=(), favorable=False)
        self.assertEqual(history_entry_policy(behavior, False, 70), (True, 70))

    def test_bad_mature_history_requires_exceptional_85(self) -> None:
        behavior = SimpleNamespace(impulses=(1, 2, 3), favorable=False)
        self.assertEqual(history_entry_policy(behavior, False, 70), (False, 85))
        self.assertEqual(history_entry_policy(behavior, True, 70), (True, 85))


if __name__ == "__main__":
    unittest.main()
