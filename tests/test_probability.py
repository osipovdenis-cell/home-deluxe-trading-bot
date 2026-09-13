import unittest

from bot.probability import FEATURE_NAMES, train_probability_model


class ProbabilityModelTests(unittest.TestCase):
    def test_chronological_model_separates_continuation(self) -> None:
        samples = []
        for index in range(600):
            success = index % 4 == 0 or index % 7 == 0
            features = {name: None for name in FEATURE_NAMES}
            features.update({
                "confirmation_progress_percent": 0.18 if success else -0.05,
                "confirmation_change_5s_percent": 0.10 if success else 0.01,
                "confirmation_change_10s_percent": 0.14 if success else 0.02,
                "change_60s_percent": 0.35 if success else 0.10,
                "trend_change_60m_percent": 4.0 if success else -1.0,
            })
            samples.append((int(success), features))
        model = train_probability_model(samples)
        self.assertIsNotNone(model)
        self.assertGreater(
            model.validation_top_quartile_rate_percent,
            model.validation_base_rate_percent,
        )
        self.assertEqual(
            sum(bucket[3] for bucket in model.validation_buckets),
            model.validation_examples,
        )
        winner = {name: None for name in FEATURE_NAMES}
        winner.update({
            "confirmation_progress_percent": 0.2,
            "confirmation_change_5s_percent": 0.1,
            "confirmation_change_10s_percent": 0.15,
            "change_60s_percent": 0.4,
            "trend_change_60m_percent": 5,
        })
        loser = {name: None for name in FEATURE_NAMES}
        loser.update({
            "confirmation_progress_percent": -0.1,
            "confirmation_change_5s_percent": 0,
            "confirmation_change_10s_percent": 0,
            "change_60s_percent": 0.05,
            "trend_change_60m_percent": -2,
        })
        self.assertGreater(model.predict_percent(winner), model.predict_percent(loser))


if __name__ == "__main__":
    unittest.main()
