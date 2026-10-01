def test_spark_session_fixture_can_create_iceberg_table(spark_session, table_suffix):
    table = f"lakehouse.smoke.test_{table_suffix}"
    spark_session.sql(f"CREATE TABLE {table} (id INT) USING iceberg")
    spark_session.sql(f"INSERT INTO {table} VALUES (1), (2)")

    rows = spark_session.table(table).collect()

    assert sorted(row.id for row in rows) == [1, 2]
