import tempfile
import unittest
from pathlib import Path

from bot.audit import AuditLog, detect_pumps


class AuditTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
