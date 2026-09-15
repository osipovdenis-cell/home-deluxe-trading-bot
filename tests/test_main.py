import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace

from bot.ai import AIError
from bot.main import (
    analyze_momentum_with_retries,
    exceptional_new_entry,
    history_entry_policy,
    leader_ai_entry_policy,
    openai_error_kind,
    process_signal,
)
from bot.market import PumpSignal, SignalMarketContext, EntryDynamics


class ExceptionalEntryTests(unittest.TestCase):
    def test_leader_volume_gate_prevents_purchase_before_ai(self):
        for volume in (.99, float('nan'), float('inf')):
            with self.subTest(volume=volume):
                market, audit, trader, ai = Mock(), Mock(), Mock(), Mock()
                context = SignalMarketContext(1000, volume, 100, 60, spread_bps=10)
                signal = PumpSignal("TEST", 100, 3, 300, "лидер")
                self.assertFalse(process_signal(signal, {}, 100, market, audit,
                    trader, ai, Mock(), "chat", SimpleNamespace(), context))
                trader.open_on_signal.assert_not_called()
                ai.analyze.assert_not_called()
                market.execution_safety.assert_not_called()
                audit.record_entry_rejection.assert_called_once()

    def test_volume_boundary_continues_to_existing_execution_gate(self):
        market, audit, trader = Mock(), Mock(), Mock()
        market.execution_safety.return_value = (False, "test spread", .01)
        context = SignalMarketContext(1000, 1, 100, 60, spread_bps=10)
        signal = PumpSignal("TEST", 100, 3, 300, "лидер")
        self.assertFalse(process_signal(signal, {}, 100, market, audit,
            trader, Mock(), Mock(), "chat", SimpleNamespace(), context))
        market.execution_safety.assert_called_once()
        trader.open_on_signal.assert_not_called()

    def test_shadow_pair_records_wait_without_opening_actual_trade(self):
        context = SignalMarketContext(1000, 2, 100, 60, spread_bps=10)
        market, audit, trader = Mock(), Mock(), Mock()
        market.execution_safety.return_value = (True, None, 0.01)
        market.leader_entry_quality.return_value = (True, None)
        market.entry_dynamics.return_value = EntryDynamics(.1, .1, .2, .3, .5, -.1, 0, 0, 50)
        settings = SimpleNamespace(
            early_threshold_percent=.5, paper_take_profit_1_percent=.7,
            paper_take_profit_2_percent=1, paper_stop_loss_percent=.5,
            estimated_round_trip_cost_percent=.2,
        )
        signal = PumpSignal("TEST", 100, 3, 300, "лидер")
        analysis = SimpleNamespace(decision="WAIT", score=60, verdict="wait", reason="test", risk="test")
        with patch("bot.main.analyze_momentum_with_retries", return_value=(analysis, None, 1)), \
                patch("bot.main.time.time", return_value=150), patch("builtins.print"):
            opened = process_signal(signal, {}, 100, market, audit, trader, Mock(),
                                    Mock(), "chat", settings, context)
        self.assertFalse(opened)
        trader.open_on_signal.assert_not_called()
        audit.rocket_comparison.candidate.assert_called_once_with(
            "TEST", "WAIT", False, 150, 10, .5, .2
        )

    def test_failed_execution_cannot_enter_shadow_comparison(self):
        market, audit, trader = Mock(), Mock(), Mock()
        market.execution_safety.return_value = (False, "спред", .01)
        signal = PumpSignal("TEST", 100, 3, 300, "лидер")
        context = SignalMarketContext(1000, 2, 100, 60, spread_bps=100)
        with patch("builtins.print"):
            opened = process_signal(signal, {}, 100, market, audit, trader, Mock(),
                                    Mock(), "chat", Mock(), context)
        self.assertFalse(opened)
        audit.rocket_comparison.candidate.assert_not_called()
        trader.open_on_signal.assert_not_called()

    def test_ai_wait_delays_only_first_leader_entry(self) -> None:
        analysis = SimpleNamespace(decision="WAIT", score=62)
        allowed, reason = leader_ai_entry_policy(analysis, False)
        self.assertFalse(allowed)
        self.assertIn("отложен", reason)
        self.assertEqual(leader_ai_entry_policy(analysis, True), (True, None))
        analysis.decision = "BUY"
        self.assertEqual(leader_ai_entry_policy(analysis, False), (True, None))

    def test_retries_ai_twice_then_returns_analysis(self) -> None:
        expected = SimpleNamespace(decision="BUY", score=70)

        class FlakyAI:
            calls = 0

            def analyze_momentum(self):
                self.calls += 1
                if self.calls < 3:
                    raise AIError("Некорректный формат ответа OpenAI")
                return expected

        ai = FlakyAI()
        with patch("bot.main.time.sleep"):
            analysis, error, attempts = analyze_momentum_with_retries(ai)
        self.assertIs(analysis, expected)
        self.assertIsNone(error)
        self.assertEqual(attempts, 3)
        self.assertEqual(openai_error_kind(AIError("Некорректный формат ответа OpenAI")), "format")

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
