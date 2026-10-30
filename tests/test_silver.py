from off_pipeline.silver import transform


def rows_by_code(df):
    return {row["code"]: row for row in df.collect()}


def test_only_valid_products_reach_silver(bronze_df):
    valid, _ = transform(bronze_df)
    assert set(rows_by_code(valid)) == {"1001", "1002", "1008", "1009"}


def test_rejected_rows_name_the_failed_rules(bronze_df):
    _, rejected = transform(bronze_df)
    failed = {row["code"]: set(row["_dq_failed_rules"]) for row in rejected.collect()}

    assert failed["1003"] == {"sugar_in_range"}  # sugar value missing
    assert failed["1004"] == {"is_food"}
    assert failed["1006"] == {"energy_in_range"}
    assert failed["1007"] == {"country_present"}
    assert failed[""] == {"code_present"}


def test_duplicate_barcode_keeps_newest_version(bronze_df):
    valid, _ = transform(bronze_df)
    yogurt = rows_by_code(valid)["1001"]
    assert yogurt["product_name"] == "Greek Yogurt Natural"
    assert valid.filter("code = '1001'").count() == 1


def test_tags_and_names_are_normalised(bronze_df):
    rows = rows_by_code(transform(bronze_df)[0])

    assert rows["1001"]["main_category"] == "dairies"
    assert rows["1001"]["countries"] == ["germany", "france"]
    assert rows["1002"]["brand"] == "coca cola"
    assert rows["1002"]["countries"] == ["united kingdom"]
    assert rows["1008"]["product_name"] == "Dark Chocolate 70%"


def test_nutrients_are_numeric(bronze_df):
    valid, _ = transform(bronze_df)
    types = dict(valid.dtypes)
    assert types["sugar_100g"] == "double"
    assert types["energy_100g_kj"] == "double"
    assert types["protein_100g"] == "double"
