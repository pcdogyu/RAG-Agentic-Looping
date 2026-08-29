from backend.app.services.industry_taxonomy import (
    all_industries,
    industries_mentioned,
    normalize_industry,
)


def test_industry_taxonomy_maps_specific_bilingual_source_values():
    assert normalize_industry("Consumer Cyclical", "Auto - Parts") == (
        "sector:consumer_discretionary",
        "industry:auto_parts",
    )
    assert normalize_industry("金融业", "") == (
        "sector:financials",
        "industry:diversified_financials",
    )
    assert normalize_industry("", "药品及生物科技") == (
        "sector:health_care",
        "industry:biotechnology",
    )
    assert normalize_industry("Real Estate", "REIT - Mortgage") == (
        "sector:real_estate",
        "industry:reits",
    )


def test_industry_taxonomy_uses_specific_longest_alias_and_preserves_unknowns():
    assert normalize_industry("可选消费", "汽车零部件") == (
        "sector:consumer_discretionary",
        "industry:auto_parts",
    )
    assert normalize_industry("Financial Services", "Shell Companies") == (
        "sector:financials",
        "industry:special_purpose",
    )
    assert normalize_industry("", "无法识别的新行业") == ("", "")
    industry_ids = [item.industry_id for item in all_industries()]
    assert len(industry_ids) == len(set(industry_ids))
    assert industries_mentioned("半导体行业订单增长") == ["industry:semiconductors"]
