from registry.transform.silver_to_gold import ensure_gold_table


def test_ensure_gold_table_is_idempotent(spark_session, table_suffix):
    gold_table = f"lakehouse.gold.sirene_{table_suffix}"

    ensure_gold_table(spark_session, gold_table)
    ensure_gold_table(spark_session, gold_table)

    columns = [field.name for field in spark_session.table(gold_table).schema]
    assert "is_current" in columns
    assert "valid_from" in columns
    assert "valid_to" in columns
