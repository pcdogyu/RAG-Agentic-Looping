from backend.app.services.directional_scoring import (
    blocked_probabilities,
    calibrate_probabilities,
    deterministic_direction_score,
    directional_text_hint,
    gated_score,
    normalize_probabilities,
    probabilities_for_score,
)


def test_explicit_adverse_facts_override_an_incorrect_positive_fallback():
    assert directional_text_hint(
        "Securities Fraud Class Action Lawsuit Against Hertz",
        "negative",
        fallback=1,
    ) == -1
    assert directional_text_hint(
        "RBLX 的营收持续增长，尽管净利润仍为负值。",
        fallback=1,
    ) == -1
    assert directional_text_hint(
        "INVESTIGATION ALERT: law firm is investigating Innventure stockholders' claims",
        "可能对投资者情绪产生负面影响",
        fallback=0,
    ) == -1


def test_directional_text_hint_keeps_fallback_without_explicit_language():
    assert directional_text_hint("Quarterly business update", fallback=1) == 1
    assert directional_text_hint("Quarterly business update", fallback=0) is None
    assert directional_text_hint("Revenue growth", fallback=0, allow_beneficial=False) is None


def test_explicit_benefit_language_is_recognized_as_positive():
    assert directional_text_hint("Meta 可能从中受益") == 1
    assert directional_text_hint("该公司有望直接受益，并带来新增订单") == 1
    assert directional_text_hint("The company is poised to benefit from new AI orders") == 1


def test_negated_or_uncertain_benefit_language_blocks_positive_fallback():
    for text in (
        "Meta 未必受益",
        "能否受益仍不确定",
        "受益有限",
        "收益可能被成本投入抵消",
        "The company may not benefit from the spending cycle",
        "Benefits remain uncertain and could be offset by costs",
    ):
        assert directional_text_hint(text, fallback=1) is None
    assert directional_text_hint("Benefits remain uncertain", fallback=-1) == -1


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
