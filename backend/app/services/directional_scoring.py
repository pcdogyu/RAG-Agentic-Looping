from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DirectionScore:
    raw_score: int
    probability_score: int
    event_score: int | None
    factor_score: int | None


def normalize_probabilities(
    bull: float, base: float, bear: float
) -> tuple[float, float, float]:
    values = [max(0.0, bull), max(0.0, base), max(0.0, bear)]
    total = sum(values)
    if total <= 0:
        return (0.25, 0.5, 0.25)
    normalized = tuple(round(value / total, 8) for value in values)
    # Keep the Pydantic sum invariant exact enough after rounding.
    return (
        normalized[0],
        round(1.0 - normalized[0] - normalized[2], 8),
        normalized[2],
    )


def calibrate_probabilities(
    bull: float,
    base: float,
    bear: float,
    *,
    reliability: float,
) -> tuple[float, float, float]:
    """Contract model scenarios toward a neutral prior when support is weak.

    This is an explicit pre-empirical calibration policy.  Outcome-based Brier
    and ECE evaluation can later replace its versioned parameters without
    changing the scoring contract.
    """

    probabilities = normalize_probabilities(bull, base, bear)
    prior = (0.25, 0.5, 0.25)
    weight = max(0.0, min(1.0, reliability))
    calibrated = tuple(
        prior_value + weight * (value - prior_value)
        for value, prior_value in zip(probabilities, prior, strict=True)
    )
    return normalize_probabilities(*calibrated)


def probabilities_for_score(
    score: int,
    *,
    base_probability: float = 0.5,
) -> tuple[float, float, float]:
    """Return scenarios whose bull-bear edge exactly matches a published score.

    The deterministic score may combine scenario, event and measured-factor
    components.  Reconstructing the published scenarios prevents the UI from
    showing a bullish probability edge next to a bearish final score.
    """

    edge = max(-1.0, min(1.0, score / 100))
    base = max(0.0, min(float(base_probability), 1.0 - abs(edge)))
    directional_mass = 1.0 - base
    bull = (directional_mass + edge) / 2
    bear = (directional_mass - edge) / 2
    return normalize_probabilities(bull, base, bear)


def deterministic_direction_score(
    *,
    bull_probability: float,
    base_probability: float,
    bear_probability: float,
    event_direction: int | None = None,
    event_relevance: float = 0,
    factor_signal: float | None = None,
    factor_reliability: float = 0,
) -> DirectionScore:
    """Calculate direction from declared inputs using a fixed, auditable formula.

    The research model's self-reported score is deliberately absent.  Its
    scenario probabilities are one signal; explicit event mapping and measured
    market/fundamental factors are independent program inputs.  Missing inputs
    are omitted and the remaining weights are renormalized.
    """

    bull, _, bear = normalize_probabilities(
        bull_probability, base_probability, bear_probability
    )
    probability_score = round(100 * (bull - bear))
    components: list[tuple[float, float]] = [(float(probability_score), 0.45)]

    event_score: int | None = None
    if event_direction in {-1, 1} and event_relevance > 0:
        event_score = round(100 * event_direction * min(1.0, event_relevance))
        components.append((float(event_score), 0.25))

    measured_factor_score: int | None = None
    if factor_signal is not None and factor_reliability > 0:
        measured_factor_score = round(100 * max(-1.0, min(1.0, factor_signal)))
        components.append(
            (float(measured_factor_score), 0.30 * min(1.0, factor_reliability))
        )

    denominator = sum(weight for _, weight in components)
    raw = round(sum(value * weight for value, weight in components) / denominator)
    return DirectionScore(
        raw_score=max(-100, min(100, raw)),
        probability_score=probability_score,
        event_score=event_score,
        factor_score=measured_factor_score,
    )


def gated_score(raw_score: int, evidence_strength: float, mapping_confidence: float) -> int:
    strength = max(0.0, min(1.0, evidence_strength))
    mapping = max(0.0, min(1.0, mapping_confidence))
    return max(-100, min(100, round(raw_score * strength * mapping)))


def blocked_probabilities(*, technical_failure: bool = False) -> tuple[float, float, float]:
    """Return probabilities that do not visually imply a tradable direction."""

    return (0.2, 0.6, 0.2) if technical_failure else (0.25, 0.5, 0.25)
