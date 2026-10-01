from pathlib import Path

import pytest


@pytest.fixture
def fixture_csv_path() -> Path:
    return Path(__file__).parent / "fixtures" / "sirene_stock_sample.csv"
