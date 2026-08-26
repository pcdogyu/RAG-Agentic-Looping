from backend.app.services.directional_scoring import (
    blocked_probabilities,
    calibrate_probabilities,
    deterministic_direction_score,
    gated_score,
    normalize_probabilities,
    probabilities_for_score,
)


def test_model_reported_score_is_not_an_input_to_deterministic_score():
    result = deterministic_direction_score(
        bull_probability=0.60,
        base_probability=0.25,
        bear_probability=0.15,
        event_direction=1,
        event_relevance=0.8,
        factor_signal=-0.2,
        factor_reliability=0.5,
    )

    assert result.probability_score == 45
    assert result.event_score == 80
    assert result.factor_score == -20
    assert result.raw_score == 44


def test_missing_components_are_omitted_instead_of_assumed_neutral():
    result = deterministic_direction_score(
        bull_probability=0.2,
        base_probability=0.3,
        bear_probability=0.5,
    )

    assert result.raw_score == -30


def test_gate_and_blocked_probability_contract_are_deterministic():
    assert gated_score(80, 0.75, 0.8) == 48
    assert blocked_probabilities() == (0.25, 0.5, 0.25)
    assert blocked_probabilities(technical_failure=True) == (0.2, 0.6, 0.2)
    assert sum(normalize_probabilities(2, 1, 1)) == 1


def test_probability_calibration_contracts_weak_evidence_toward_neutral():
    assert calibrate_probabilities(0.8, 0.1, 0.1, reliability=0) == (0.25, 0.5, 0.25)
    assert calibrate_probabilities(0.8, 0.1, 0.1, reliability=1) == (0.8, 0.1, 0.1)
    calibrated = calibrate_probabilities(0.8, 0.1, 0.1, reliability=0.5)
    assert calibrated == (0.525, 0.3, 0.175)


def test_published_probabilities_are_consistent_with_program_score():
    bull, base, bear = probabilities_for_score(-35, base_probability=0.4)

    assert (bull, base, bear) == (0.125, 0.4, 0.475)
    assert round(100 * (bull - bear)) == -35
