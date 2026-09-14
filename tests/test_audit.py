import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from bot.audit import AuditLog, detect_pumps
from bot.probability import FEATURE_NAMES, train_probability_model


class AuditTests(unittest.TestCase):
    def test_reports_full_path_for_all_leaders(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                events = (
                    ("WINUSDT", True, ((1, 100), (30, 101), (61, 100.4))),
                    ("STOPUSDT", False, ((2, 100), (30, 99.4), (62, 99.6))),
                )
                for index, (symbol, accepted, points) in enumerate(events):
                    event = SimpleNamespace(
                        started_at=1 + index, resolved_at=21 + index,
                        symbol=symbol, trigger_price=100, resolution_price=100,
                        accepted=accepted, reason="тест", progress_percent=0,
                        pullback_percent=0, change_5s_percent=0,
                        change_10s_percent=0, signal_kind="лидер",
                    )
                    log.record_confirmation_event(event)
                    log.connection.executemany(
                        "INSERT INTO samples(timestamp,symbol,price) VALUES(?,?,?)",
                        ((timestamp, symbol, price) for timestamp, price in points),
                    )
                log.connection.commit()
                text = log.leader_path_report_text(
                    100, lookback_seconds=200, horizon_seconds=60
                )
                self.assertIn("Путь всех лидеров за 1 минут", text)
                self.assertIn("цель/стоп/нейтр. 1/1/0", text)
                self.assertIn("прошли 20 секунд: 1", text)
                self.assertIn("не прошли 20 секунд: 1", text)
                self.assertIn("достигли +1/+3/+5%: 1/0/0", text)
            finally:
                log.close()

    def test_order_flow_report_separates_targets_stops_and_neutral(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                for index, outcome in enumerate(("target", "stop", "neutral")):
                    event = SimpleNamespace(
                        started_at=100 + index, resolved_at=120 + index,
                        symbol=f"LEADER{index}USDT", trigger_price=100,
                        resolution_price=100, accepted=True, reason="подтверждён",
                        progress_percent=0.1, pullback_percent=-0.1,
                        change_5s_percent=0.05, change_10s_percent=0.1,
                        signal_kind="лидер",
                    )
                    context = SimpleNamespace(
                        volume_ratio_5m=2, taker_buy_ratio_percent=60,
                        order_book_imbalance_percent=10, spread_bps=4,
                        flow_buy_5s_usdt=1000, flow_sell_5s_usdt=200,
                        flow_buy_15s_usdt=2000, flow_sell_15s_usdt=500,
                        flow_buy_60s_usdt=5000, flow_sell_60s_usdt=1000,
                        flow_cvd_60s_percent=50 if outcome == "target" else -10,
                        flow_trade_rate_acceleration=2,
                        flow_price_change_60s_percent=0.2 if outcome == "target" else -0.1,
                        flow_price_efficiency_per_10k=0.5 if outcome == "target" else -0.2,
                        flow_ask_depletion_percent=70,
                        flow_bid_support_percent=65,
                        flow_spread_bps=4, flow_spread_change_bps=-1,
                    )
                    event_id = log.record_confirmation_event(event, context)
                    log.connection.execute(
                        "UPDATE confirmation_events SET evaluated_at=1000,"
                        "immediate_success=?,immediate_stopped_first=? WHERE id=?",
                        (int(outcome == "target"), int(outcome == "stop"), event_id),
                    )
                log.connection.commit()
                report = log.order_flow_report_text(1000)
                self.assertIn("цель 1, стоп 1, нейтрально 1", report)
                self.assertIn("CVD 60 с", report)
                self.assertIn("эффективное продолжение", report)
            finally:
                log.close()

    def test_probability_report_keeps_validation_and_shadow_counts_separate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                samples = []
                for index in range(500):
                    features = {name: None for name in FEATURE_NAMES}
                    features["confirmation_progress_percent"] = index % 10
                    samples.append((int(index % 5 == 0), features))
                model = train_probability_model(samples)
                log._probability_cache = model
                log._probability_cache_at = 1000
                event = SimpleNamespace(
                    started_at=900, resolved_at=920, symbol="MODELUSDT",
                    trigger_price=100, resolution_price=100,
                    accepted=False, reason="shadow", progress_percent=0.1,
                    pullback_percent=0, change_5s_percent=0.1,
                    change_10s_percent=0.1,
                )
                event_id = log.record_confirmation_event(event)
                log.connection.execute(
                    "UPDATE confirmation_events SET evaluated_at=999,"
                    "delayed_success=1,delayed_stopped_first=0,"
                    "shadow_probability_percent=45,spread_bps=10 WHERE id=?",
                    (event_id,),
                )
                log.connection.commit()
                report = log.probability_shadow_report_text(1000)
                self.assertIn("только отложенная выборка", report)
                self.assertIn("отдельная выборка, не контрольная", report)
                self.assertIn("Прогноз 40%+: 1", report)
                bucket_total = sum(item[3] for item in model.validation_buckets)
                self.assertEqual(bucket_total, model.validation_examples)
            finally:
                log.close()

    def test_leader_report_summarizes_matured_signals(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                signal_id = log.record_signal(
                    100, "ROCKETUSDT", 1, "аномальный лидер", 3.5, 20,
                    2_000_000, 82, "продолжение", ai_decision="BUY",
                )
                log.connection.execute(
                    "INSERT INTO learning_examples("
                    "signal_id,matured_at,signal_timestamp,symbol,"
                    "success_before_stop,reached_second_target,"
                    "maximum_return_percent,minimum_return_percent,"
                    "setup_change_percent) VALUES(?,?,?,?,?,?,?,?,?)",
                    (signal_id, 1000, 100, "ROCKETUSDT", 1, 0, 0.8, -0.1, 3.5),
                )
                log.connection.commit()
                report = log.leader_report_text(1000)
                self.assertIn("аномальный лидер", report)
                self.assertIn("AI BUY 1", report)
                self.assertIn("100.0%", report)
            finally:
                log.close()

    def test_leader_funnel_shows_confirmation_ai_and_purchase_stages(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                event = SimpleNamespace(
                    started_at=90, resolved_at=100, symbol="ROCKETUSDT",
                    trigger_price=1, resolution_price=1.01, accepted=True,
                    reason="подтверждён", progress_percent=0.1,
                    pullback_percent=0, change_5s_percent=0.05,
                    change_10s_percent=0.1, signal_kind="аномальный лидер",
                )
                log.record_confirmation_event(event)
                log.record_signal(
                    100, "ROCKETUSDT", 1.01, "аномальный лидер", 3.1, 20,
                    1_000_000, None, "AI недоступен",
                )
                report = log.leader_funnel_report_text(200, 0)
                self.assertIn("После фильтра роста за 12 ч: 1", report)
                self.assertIn("Выдержали 20 секунд: 1", report)
                self.assertIn("Получили ответ AI: 0; без ответа: 1", report)
                self.assertIn("Тестовых покупок: 0", report)
            finally:
                log.close()

    def test_rescue_report_links_shadow_ai_decision_to_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                event = SimpleNamespace(
                    started_at=0, resolved_at=20, symbol="RESCUEUSDT",
                    trigger_price=100, resolution_price=100.1,
                    accepted=True, reason="повторное ускорение подтверждено",
                    progress_percent=0.1, pullback_percent=0,
                    change_5s_percent=0.06, change_10s_percent=0.11,
                )
                log.record_confirmation_event(event)
                log.record_signal(
                    20, "RESCUEUSDT", 100.1, "ранний", 0.6, 1,
                    1_000_000, 78, "теневой BUY", ai_decision="BUY",
                    analysis_version=2,
                )
                for timestamp in range(0, 901):
                    log.record_confirmation_prices(
                        {"RESCUEUSDT": 100.9 if timestamp >= 100 else 100.1},
                        timestamp,
                    )
                log.refresh_confirmation_outcomes(901, 0.7, 0.5)
                report = log.candidate_pattern_report_text(901)
                self.assertIn("Повторно ускорились: 1", report)
                self.assertIn("Дошли до AI: 1; AI BUY: 1", report)
                self.assertIn("цель +0,7% — 1", report)
            finally:
                log.close()

    def test_confirmation_audit_counts_missed_winner_and_prevented_stop(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                for symbol in ("WINUSDT", "STOPUSDT"):
                    event = SimpleNamespace(
                        started_at=0, resolved_at=20, symbol=symbol,
                        trigger_price=100, resolution_price=100,
                        accepted=False, reason="нет продолжения",
                        progress_percent=0.2 if symbol == "WINUSDT" else -0.1,
                        pullback_percent=-0.02 if symbol == "WINUSDT" else -0.2,
                        change_5s_percent=0.1 if symbol == "WINUSDT" else -0.1,
                        change_10s_percent=0.15 if symbol == "WINUSDT" else -0.15,
                    )
                    context = SimpleNamespace(
                        volume_ratio_5m=2 if symbol == "WINUSDT" else 1.2,
                        taker_buy_ratio_percent=65 if symbol == "WINUSDT" else 51,
                        order_book_imbalance_percent=(20 if symbol == "WINUSDT" else -15),
                        spread_bps=2 if symbol == "WINUSDT" else 8,
                    )
                    dynamics = SimpleNamespace(as_dict=lambda symbol=symbol: {
                        "change_15s_percent": 0.2 if symbol == "WINUSDT" else -0.1,
                        "change_60s_percent": 0.5 if symbol == "WINUSDT" else 0.1,
                        "pullback_from_5m_high_percent": (-0.02 if symbol == "WINUSDT" else -0.2),
                        "btc_change_300s_percent": 0.1,
                        "market_breadth_60s_percent": 55,
                    })
                    log.record_confirmation_event(event, context, dynamics)
                for timestamp in range(0, 901):
                    log.record_confirmation_prices({
                        "WINUSDT": 100.8 if timestamp >= 100 else 100,
                        "STOPUSDT": 99.4 if timestamp >= 100 else 100,
                    }, timestamp)
                self.assertEqual(
                    log.refresh_confirmation_outcomes(901, 0.7, 0.5), 2
                )
                report = log.build_confirmation_audit(901)
                self.assertEqual(report.missed_winners, 1)
                self.assertEqual(report.prevented_stops, 1)
                learning = log.build_learning_report(901)
                self.assertEqual(learning.examples, 2)
                behavior = log.build_symbol_behavior(
                    "WINUSDT", 901, 0.5, 0.7, 1.0, 0.5
                )
                self.assertEqual(len(behavior.impulses), 1)
                self.assertTrue(behavior.impulses[0].first_target_hit)
                pattern = log.candidate_pattern_report_text(901)
                self.assertIn("Расширенных снимков: 2", pattern)
                self.assertIn("taker-buy: успешно 65.00%", pattern)
                self.assertIn("n=1", pattern)
            finally:
                log.close()

    def test_rejected_candidate_gets_outcome_from_confirmation_price(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                event = SimpleNamespace(
                    started_at=0, resolved_at=20, symbol="REJECTEDUSDT",
                    trigger_price=100, resolution_price=100.2,
                    accepted=False, reason="нет продолжения",
                    progress_percent=0.2, pullback_percent=-0.05,
                    change_5s_percent=0.05, change_10s_percent=0.1,
                )
                event_id = log.record_confirmation_event(event)
                for timestamp in range(0, 901):
                    price = 100.2 if timestamp < 100 else 100.91
                    log.record_confirmation_prices(
                        {"REJECTEDUSDT": price}, timestamp
                    )
                self.assertEqual(
                    log.refresh_confirmation_outcomes(901, 0.7, 0.5), 1
                )
                row = log.connection.execute(
                    "SELECT delayed_success,delayed_stopped_first "
                    "FROM confirmation_events WHERE id=?", (event_id,),
                ).fetchone()
                self.assertEqual(row, (1, 0))
            finally:
                log.close()

    def test_observer_reports_ai_decisions_and_rejections(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                log.record_signal(
                    100, "WATCHUSDT", 1, "ранний", 1.1, 2, 1_000_000,
                    31, "слабый", ai_decision="SKIP",
                    ai_reason="Импульс затухает", analysis_version=2,
                )
                log.record_entry_rejection(
                    101, "WATCHUSDT", "AI решил SKIP", 1, 0.01
                )
                log.record_error("OpenAI: timeout", 102)
                log.record_error("Binance context unavailable", 103)
                decisions = log.recent_ai_decisions_text()
                observer = log.observer_report_text(200, 0)
                self.assertIn("WATCHUSDT: SKIP, 31/100", decisions)
                self.assertIn("Решения AI: SKIP 1", observer)
                self.assertIn("решение AI 1", observer)
                self.assertIn("OpenAI 1", observer)
                self.assertIn("Binance/данные 1", observer)
            finally:
                log.close()

    @staticmethod
    def _record_full_signal(log, timestamp, symbol="LEARNUSDT"):
        return log.record_signal(
            timestamp, symbol, 100, "ранний", 1.1, 3, 1_000_000,
            75, "подтверждён", 100_000, 1.8, 500, 58,
            2, 50_000, 40_000, 11, ai_decision="BUY",
            analysis_version=2,
            entry_dynamics={
                "change_60s_percent": 0.2,
                "pullback_from_5m_high_percent": -0.05,
            },
        )

    def test_matured_signal_becomes_learning_example(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                self._record_full_signal(log, 0)
                for timestamp in range(0, 901, 60):
                    price = 100.8 if timestamp == 300 else 100.1
                    log.record_prices({"LEARNUSDT": price}, timestamp)
                inserted = log.refresh_learning_examples(901, 0.7, 1.0, 0.5)
                self.assertEqual(inserted, 1)
                row = log.connection.execute(
                    "SELECT success_before_stop, maximum_return_percent "
                    "FROM learning_examples"
                ).fetchone()
                self.assertEqual(row[0], 1)
                self.assertAlmostEqual(row[1], 0.8)
            finally:
                log.close()

    def test_learning_profile_demands_high_score_after_repeated_failures(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                for index in range(4):
                    signal_id = self._record_full_signal(log, index * 2000)
                    log.connection.execute(
                        "INSERT INTO learning_examples("
                        "signal_id,matured_at,signal_timestamp,symbol,"
                        "success_before_stop,reached_second_target,"
                        "maximum_return_percent,minimum_return_percent,"
                        "setup_change_percent,volume_ratio_5m,"
                        "taker_buy_ratio_percent,order_book_imbalance_percent,"
                        "change_60s_percent,pullback_from_high_percent,"
                        "ai_score,ai_decision) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (signal_id, index * 2000 + 900, index * 2000,
                         "LEARNUSDT", 0, 0, 0.2, -0.6, 1.1, 1.8,
                         58, 11, 0.2, -0.05, 75, "BUY"),
                    )
                log.connection.commit()
                profile = log.build_learning_profile(
                    "LEARNUSDT", 9000,
                    {"volume_ratio_5m": 1.8,
                     "taker_buy_ratio_percent": 58,
                     "order_book_imbalance_percent": 11,
                     "change_60s_percent": 0.2,
                     "pullback_from_high_percent": -0.05},
                )
                self.assertFalse(profile.blocked)
                self.assertEqual(profile.status, "HIGH_CAUTION")
                self.assertEqual(profile.consecutive_failures, 4)
                self.assertEqual(profile.required_ai_score(70), 85)
            finally:
                log.close()

    def test_learning_profile_uses_actual_consecutive_trade_losses(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                log.connection.execute(
                    "CREATE TABLE paper_positions(symbol TEXT, status TEXT, "
                    "closed_at REAL, realized_pnl_usdt REAL)"
                )
                log.connection.executemany(
                    "INSERT INTO paper_positions VALUES(?, 'CLOSED', ?, ?)",
                    (("LEARNUSDT", 100, -0.3), ("LEARNUSDT", 200, -0.4)),
                )
                log.connection.commit()
                profile = log.build_learning_profile("LEARNUSDT", 300, {})
                self.assertEqual(profile.consecutive_trade_losses, 2)
                self.assertEqual(profile.status, "HIGH_CAUTION")
                self.assertEqual(profile.required_ai_score(70), 80)
            finally:
                log.close()

    def test_old_price_only_history_cannot_authorize_entry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                for timestamp in range(0, 7501, 60):
                    log.record_prices(
                        {"TESTUSDT": 101 if timestamp % 2400 == 300 else 100},
                        timestamp,
                    )
                behavior = log.build_symbol_behavior(
                    "TESTUSDT", 7500, 0.5, 0.7, 1, 0.5
                )
                self.assertEqual(behavior.impulses, ())
                self.assertFalse(behavior.favorable)
            finally:
                log.close()

    def test_symbol_behavior_requires_repeated_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                points = {}
                for base in (0, 2400, 4800):
                    points[base] = 100
                    points[base + 300] = 100.6
                    points[base + 360] = 101.7
                    points[base + 1200] = 101.0
                for timestamp in range(0, 7501, 60):
                    price = points.get(timestamp, 100)
                    log.record_prices({"TESTUSDT": price}, timestamp)
                for base in (0, 2400, 4800):
                    log.record_signal(
                        base + 300, "TESTUSDT", 100.6, "ранний", 0.6, 2,
                        1_000_000, 75, "подтверждён", 100_000, 2, 500, 60,
                        2, 50_000, 40_000, 11, ai_decision="BUY",
                        analysis_version=2,
                        entry_dynamics={
                            "change_60s_percent": 0.2,
                            "pullback_from_5m_high_percent": 0,
                        },
                    )
                behavior = log.build_symbol_behavior(
                    "TESTUSDT", 7500, 0.5, 0.7, 1, 0.5
                )
                self.assertGreaterEqual(len(behavior.impulses), 3)
                self.assertTrue(behavior.favorable)
                self.assertGreaterEqual(behavior.first_target_hits, 2)
            finally:
                log.close()

    def test_records_entry_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                log.record_entry_rejection(1, "BTTUSDT", "широкий спред", 300, 3)
                row = log.connection.execute(
                    "SELECT symbol, reason FROM paper_entry_rejections"
                ).fetchone()
                self.assertEqual(row, ("BTTUSDT", "широкий спред"))
            finally:
                log.close()

    def test_detects_and_groups_pumps(self) -> None:
        candles = [
            (0, 100, 100),
            (60, 100, 101),
            (120, 100, 104),
            (180, 103, 105),
            (2040, 100, 100),
            (2100, 100, 104),
        ]
        self.assertEqual(detect_pumps(candles, 300, 3, 1800), [120, 2100])

    def test_builds_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                log._set_metadata("period_started_at", "0")
                log.connection.commit()
                log.record_prices({"DOGEUSDT": 1.0}, 60)
                log.record_alert("DOGEUSDT", True, 120)
                summary = log.build_summary(
                    300, ("DOGEUSDT",), {"DOGEUSDT": [120]}, 60, 3
                )
                self.assertEqual(summary.expected_pumps, 1)
                self.assertEqual(summary.delivered_alerts, 1)
                self.assertEqual(summary.missed_pumps, 0)
            finally:
                log.close()

    def test_tracks_signal_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                started = log.period_started_at()
                log.record_signal(
                    started,
                    "TESTUSDT",
                    100.0,
                    "ранний",
                    1.2,
                    4.5,
                    1_000_000.0,
                    63,
                    "умеренный импульс",
                )
                inserted = log.record_due_outcomes(
                    {"TESTUSDT": 102.0}, started + 3600, 0.2
                )
                self.assertEqual(inserted, 3)
                performance = log.build_signal_performance(started + 3601)
                self.assertEqual(performance.signal_count, 1)
                self.assertEqual(performance.evaluated[15], 1)
                self.assertEqual(performance.positive_rate[30], 100.0)
                self.assertAlmostEqual(performance.average_net_return[60], 1.8)
            finally:
                log.close()

    def test_stores_order_book_context(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            log = AuditLog(str(Path(directory) / "audit.db"))
            try:
                signal_id = log.record_signal(
                    1,
                    "TESTUSDT",
                    100,
                    "ранний",
                    1.2,
                    4.5,
                    1_000_000,
                    63,
                    "умеренный импульс",
                    50_000,
                    2.5,
                    120,
                    61,
                    8,
                    20_000,
                    15_000,
                    14.2857,
                )
                row = log.connection.execute(
                    "SELECT spread_bps, bid_depth_usdt, ask_depth_usdt, "
                    "order_book_imbalance_percent FROM signal_events WHERE id = ?",
                    (signal_id,),
                ).fetchone()
                self.assertEqual(row[0], 8)
                self.assertEqual(row[1], 20_000)
                self.assertEqual(row[2], 15_000)
                self.assertAlmostEqual(row[3], 14.2857)
            finally:
                log.close()


if __name__ == "__main__":
    unittest.main()
