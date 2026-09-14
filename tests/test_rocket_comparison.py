import sqlite3
import unittest

from bot.rocket_comparison import RocketComparison


class RocketComparisonTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.model = RocketComparison(self.db)

    def tearDown(self):
        self.db.close()

    def candidate(self, decision="WAIT", reentry=False, now=0, spread=0):
        self.model.candidate("TEST", decision, reentry, now, spread, 0.5, 0.2)

    def leg(self, variant):
        columns = self.db.execute("SELECT * FROM rocket_ab_legs WHERE variant=?", (variant,))
        return dict(zip([c[0] for c in columns.description], columns.fetchone()))

    def test_only_future_quote_can_open_after_ai(self):
        self.candidate(now=10)
        self.model.tick({"TEST": 100}, 9)
        self.model.tick({"TEST": 100}, 10)
        self.assertEqual(self.leg("A")["status"], "READY")
        self.model.tick({"TEST": 101}, 11)
        self.assertEqual(self.leg("A")["entry"], 101)
        self.assertEqual(self.leg("B")["status"], "WAIT")

    def test_wait_b_requires_validated_reentry_and_uses_new_price(self):
        self.candidate()
        self.model.tick({"TEST": 100}, 1)
        self.model.tick({"TEST": 99}, 2)
        self.candidate(reentry=False, now=3)
        self.assertEqual(self.leg("B")["status"], "WAIT")
        self.candidate(reentry=True, now=4, spread=20)
        self.model.tick({"TEST": 99}, 5)
        self.assertAlmostEqual(self.leg("B")["entry"], 99.099)
        self.assertEqual(self.leg("A")["status"], "CLOSED")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM rocket_ab_episodes").fetchone()[0], 1)

    def test_buy_opens_both_with_same_price_and_costs(self):
        self.candidate("BUY", spread=20)
        self.model.tick({"TEST": 100}, 1)
        self.model.tick({"TEST": 99}, 2)
        a, b = self.leg("A"), self.leg("B")
        self.assertEqual(a["entry"], b["entry"])
        self.assertAlmostEqual(a["entry"], 100.1)
        self.assertAlmostEqual(a["exit_price"], 98.901)
        self.assertAlmostEqual(a["net"], (98.901 / 100.1 - 1) * 100 - 0.2)
        self.assertEqual(a["net"], b["net"])

    def test_trailing_follows_full_position_and_observed_exit(self):
        self.candidate("BUY")
        for now, price in ((1, 100), (2, 101.2), (3, 104), (4, 102.8)):
            self.model.tick({"TEST": price}, now)
        self.assertEqual(self.leg("A")["reason"], "трейлинг")
        self.assertAlmostEqual(self.leg("A")["net"], 2.6)

    def test_protection_one_percent_does_not_guarantee_fill(self):
        self.candidate("BUY")
        for now, price in ((1, 100), (2, 101.2), (3, 100.7)):
            self.model.tick({"TEST": price}, now)
        self.assertAlmostEqual(self.leg("A")["net"], 0.5)

    def test_wait_expires_without_forcing_entry(self):
        self.candidate()
        self.model.tick({"TEST": 100}, 1)
        self.model.tick({"TEST": 99}, 2)
        self.model.tick({"TEST": 110}, 3600)
        self.assertEqual(self.leg("B")["status"], "NO_ENTRY")
        self.assertEqual(self.leg("A")["status"], "CLOSED")
        self.assertIn("Завершено пар: 1", self.model.report())

    def test_unclosed_position_is_marked_not_a_fake_trade_exit(self):
        self.candidate("BUY")
        for now in range(1, 3600, 20):
            self.model.tick({"TEST": 100}, now)
        self.model.tick({"TEST": 100.1}, 3600)
        self.assertEqual(self.leg("A")["status"], "MARKED")
        self.assertIsNone(self.leg("A")["exited"])
        self.assertAlmostEqual(self.leg("A")["net"], -0.1)

    def test_gaps_exclude_entire_pair(self):
        self.candidate("BUY")
        self.model.tick({"TEST": 100}, 1)
        self.model.tick({"TEST": 110}, 200)
        self.model.tick({"TEST": 110}, 3600)
        self.assertEqual(self.leg("A")["status"], "INCOMPLETE")
        self.assertIn("Завершено пар: 0", self.model.report())
        self.assertIn("неполные: 1", self.model.report())

    def test_restart_retains_pair_and_duplicate_candidate_does_not_reset(self):
        self.candidate()
        self.model.tick({"TEST": 100}, 1)
        self.model = RocketComparison(self.db)
        self.candidate(now=2)
        self.assertEqual(self.leg("A")["entered"], 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM rocket_ab_episodes").fetchone()[0], 1)

    def test_missing_ai_or_spread_does_not_create_pairs(self):
        self.candidate(None)
        self.candidate(spread=None)
        self.candidate(spread=float("nan"))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM rocket_ab_episodes").fetchone()[0], 0)

    def test_reentry_seed_is_separate_phase_and_uses_same_policy(self):
        self.candidate(reentry=True)
        self.assertEqual(self.leg("B")["status"], "READY")
        self.assertEqual(self.db.execute("SELECT phase FROM rocket_ab_episodes").fetchone()[0], "повторный сигнал")

    def test_late_ready_quote_is_not_used(self):
        self.candidate()
        self.model.tick({"TEST": 105}, 31)
        self.assertEqual(self.leg("A")["status"], "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
