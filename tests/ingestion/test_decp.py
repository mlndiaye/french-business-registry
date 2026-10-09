import datetime as dt

import pyarrow as pa
import pyarrow.parquet as pq

from registry.ingestion.decp import bronze_object_key, filter_decp_to_scope


def test_bronze_object_key_formats_ingestion_date():
    key = bronze_object_key(dt.date(2026, 10, 9))

    assert key == "bronze/decp/ingestion_date=2026-10-09/marches.parquet"


def _make_national_table() -> pa.Table:
    return pa.table(
        {
            "uid": ["M1", "M2", "M3", "M4"],
            "acheteur_departement_code": ["08", "51", "08", "08"],
            "datePublicationDonnees": pa.array(
                [
                    dt.date(2026, 5, 1),
                    dt.date(2026, 5, 1),
                    dt.date(2024, 1, 1),
                    dt.date(2026, 5, 1),
                ],
                type=pa.date32(),
            ),
            "donneesActuelles": [True, True, True, False],
        }
    )


def test_filter_decp_to_scope_keeps_only_matching_rows(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(national_path, department="08", since=dt.date(2025, 10, 9))

    assert result.column("uid").to_pylist() == ["M1"]


def test_filter_decp_to_scope_excludes_wrong_department(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(national_path, department="08", since=dt.date(2025, 10, 9))

    assert "M2" not in result.column("uid").to_pylist()


def test_filter_decp_to_scope_excludes_rows_before_since(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(national_path, department="08", since=dt.date(2025, 10, 9))

    assert "M3" not in result.column("uid").to_pylist()


def test_filter_decp_to_scope_excludes_superseded_modifications(tmp_path):
    national_path = tmp_path / "national.parquet"
    pq.write_table(_make_national_table(), national_path)

    result = filter_decp_to_scope(national_path, department="08", since=dt.date(2025, 10, 9))

    assert "M4" not in result.column("uid").to_pylist()
