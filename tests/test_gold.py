import pytest

from off_pipeline.gold import build_gold
from off_pipeline.silver import transform


@pytest.fixture
def gold(bronze_df):
    silver, _ = transform(bronze_df)
    return build_gold(silver)


def test_fact_has_one_row_per_product(gold):
    fact = gold["fact_product"]
    assert fact.count() == 4
    assert fact.select("product_code").distinct().count() == 4


def test_nutrition_levels_and_healthy_flag(gold):
    rows = {r["product_code"]: r for r in gold["fact_product"].collect()}

    yogurt = rows["1001"]  # sugar 3.2 g, 406 kJ, protein 9 g
    assert (yogurt["sugar_level"], yogurt["energy_level"], yogurt["protein_level"]) == (0, 1, 1)
    assert yogurt["is_healthy"] is True

    gummies = rows["1009"]  # sugar 46 g, 1440 kJ, protein 6.9 g
    assert (gummies["sugar_level"], gummies["energy_level"]) == (2, 1)
    assert gummies["is_healthy"] is False

    chocolate = rows["1008"]  # 2340 kJ
    assert chocolate["energy_level"] == 2


def test_bridge_keeps_all_countries_of_a_product(gold):
    bridge = gold["bridge_product_country"]
    # 1001: germany + france, 1002: uk, 1008: switzerland + germany, 1009: germany
    assert bridge.count() == 6
    assert bridge.filter("product_code = '1001'").count() == 2


def test_fact_keys_match_dimensions(gold):
    fact = gold["fact_product"]
    orphan_brands = fact.join(gold["dim_brand"], "brand_key", "left_anti").count()
    orphan_categories = fact.join(gold["dim_category"], "category_key", "left_anti").count()
    orphan_countries = (
        gold["bridge_product_country"].join(gold["dim_country"], "country_key", "left_anti").count()
    )
    assert (orphan_brands, orphan_categories, orphan_countries) == (0, 0, 0)


def test_country_summary(gold):
    summary = {r["country_name"]: r for r in gold["agg_country_summary"].collect()}
    assert summary["germany"]["n_products"] == 3
    assert summary["france"]["n_products"] == 1
