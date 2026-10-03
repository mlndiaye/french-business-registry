from registry.ingestion.bodacc import extract_siren_from_registre


def test_extract_siren_from_registre_with_spaces():
    assert extract_siren_from_registre("334 393 806 RCS PARIS") == "334393806"


def test_extract_siren_from_registre_without_spaces():
    assert extract_siren_from_registre("334393806 RCS PARIS") == "334393806"


def test_extract_siren_from_registre_returns_none_when_absent():
    assert extract_siren_from_registre("RCS PARIS") is None


def test_extract_siren_from_registre_returns_none_for_short_number():
    assert extract_siren_from_registre("12 RCS PARIS 2024") is None


def test_extract_siren_from_registre_returns_none_for_none_input():
    assert extract_siren_from_registre(None) is None
