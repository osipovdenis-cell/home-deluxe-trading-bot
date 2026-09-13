from dataclasses import dataclass
import math
from statistics import median


FEATURE_NAMES = (
    "confirmation_progress_percent",
    "confirmation_change_5s_percent",
    "confirmation_change_10s_percent",
    "volume_ratio_5m",
    "taker_buy_ratio_percent",
    "order_book_imbalance_percent",
    "spread_bps",
    "change_15s_percent",
    "change_60s_percent",
    "pullback_from_high_percent",
    "btc_change_300s_percent",
    "market_breadth_60s_percent",
    "large_buy_volume_15s_usdt",
    "large_sell_volume_15s_usdt",
    "large_buy_volume_60s_usdt",
    "large_sell_volume_60s_usdt",
    "large_trade_imbalance_60s_percent",
    "large_trade_count_60s",
    "trend_change_15m_percent",
    "trend_change_60m_percent",
    "trend_change_240m_percent",
    "trend_efficiency_15m_percent",
    "trend_efficiency_60m_percent",
    "trend_efficiency_240m_percent",
)


@dataclass(frozen=True)
class ProbabilityModel:
    feature_names: tuple[str, ...]
    medians: tuple[float, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]
    weights: tuple[float, ...]
    intercept: float
    examples: int
    validation_examples: int
    validation_base_rate_percent: float
    validation_top_quartile_rate_percent: float
    validation_brier_score: float

    def predict_percent(self, features: dict) -> float:
        values = _transform(features, self.medians, self.means, self.scales)
        score = self.intercept + sum(
            weight * value for weight, value in zip(self.weights, values)
        )
        return _sigmoid(score) * 100


def _sigmoid(value: float) -> float:
    value = max(-35.0, min(35.0, value))
    return 1.0 / (1.0 + math.exp(-value))


def _transform(
    features: dict,
    medians: tuple[float, ...],
    means: tuple[float, ...],
    scales: tuple[float, ...],
) -> tuple[float, ...]:
    transformed = []
    for index, name in enumerate(FEATURE_NAMES):
        raw = features.get(name)
        value = medians[index] if raw is None else float(raw)
        z_score = (value - means[index]) / scales[index]
        transformed.append(max(-6.0, min(6.0, z_score)))
    return tuple(transformed)


def _normalization(samples: list[tuple[int, dict]]):
    medians = []
    means = []
    scales = []
    for name in FEATURE_NAMES:
        available = [
            float(features[name]) for _label, features in samples
            if features.get(name) is not None
        ]
        center = median(available) if available else 0.0
        values = [
            center if features.get(name) is None else float(features[name])
            for _label, features in samples
        ]
        mean = center
        if len(available) < max(50, len(samples) // 10):
            medians.append(center)
            means.append(center)
            scales.append(1e12)
            continue
        absolute_deviations = [abs(value - center) for value in values]
        robust_scale = median(absolute_deviations) * 1.4826
        if robust_scale < 1e-6:
            arithmetic_mean = sum(values) / len(values)
            variance = sum(
                (value - arithmetic_mean) ** 2 for value in values
            ) / len(values)
            robust_scale = math.sqrt(variance)
        medians.append(center)
        means.append(mean)
        scales.append(max(robust_scale, 1e-6))
    return tuple(medians), tuple(means), tuple(scales)


def _fit(samples: list[tuple[int, dict]]):
    medians, means, scales = _normalization(samples)
    matrix = [
        _transform(features, medians, means, scales)
        for _label, features in samples
    ]
    labels = [int(label) for label, _features in samples]
    base_rate = min(0.999, max(0.001, sum(labels) / len(labels)))
    intercept = math.log(base_rate / (1.0 - base_rate))
    weights = [0.0] * len(FEATURE_NAMES)
    learning_rate = 0.08
    regularization = 0.03
    for _iteration in range(180):
        intercept_gradient = 0.0
        gradients = [0.0] * len(weights)
        for values, label in zip(matrix, labels):
            probability = _sigmoid(
                intercept + sum(w * value for w, value in zip(weights, values))
            )
            error = probability - label
            intercept_gradient += error
            for index, value in enumerate(values):
                gradients[index] += error * value
        count = len(samples)
        intercept -= learning_rate * intercept_gradient / count
        for index in range(len(weights)):
            gradient = gradients[index] / count + regularization * weights[index]
            weights[index] -= learning_rate * gradient
    return medians, means, scales, tuple(weights), intercept


def train_probability_model(
    samples: list[tuple[int, dict]], minimum_examples: int = 400
) -> ProbabilityModel | None:
    if len(samples) < minimum_examples:
        return None
    validation_count = max(80, len(samples) // 5)
    if len(samples) - validation_count < 250:
        return None
    training = samples[:-validation_count]
    validation = samples[-validation_count:]
    medians, means, scales, weights, intercept = _fit(training)

    validation_predictions = []
    for label, features in validation:
        values = _transform(features, medians, means, scales)
        probability = _sigmoid(
            intercept + sum(w * value for w, value in zip(weights, values))
        )
        validation_predictions.append((probability, int(label)))
    validation_predictions.sort(key=lambda item: item[0], reverse=True)
    top_count = max(1, len(validation_predictions) // 4)
    top_rate = sum(
        label for _probability, label in validation_predictions[:top_count]
    ) / top_count
    base_rate = sum(
        label for _probability, label in validation_predictions
    ) / len(validation_predictions)
    brier = sum(
        (probability - label) ** 2
        for probability, label in validation_predictions
    ) / len(validation_predictions)

    # The validation slice remains untouched for the measurements above.  The
    # production shadow prediction is then fitted on every already matured event.
    medians, means, scales, weights, intercept = _fit(samples)
    return ProbabilityModel(
        FEATURE_NAMES, medians, means, scales, weights, intercept,
        len(samples), len(validation_predictions), base_rate * 100,
        top_rate * 100, brier,
    )
