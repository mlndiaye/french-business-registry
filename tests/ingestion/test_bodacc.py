import datetime as dt

import responses

from registry.ingestion.bodacc import (
    BODACC_API_URL,
    extract_siren_from_registre,
    fetch_bodacc_announcements,
    normalize_announcement,
)


def _raw_announcement(id_: str, siren: str) -> dict:
    return {
        "id": id_,
        "dateparution": "2025-10-15",
        "numeroannonce": "1",
        "typeavis_lib": "Jugement",
        "familleavis_lib": "Procedures collectives",
        "tribunal": "Tribunal de commerce de Paris",
        "commercant": "DUPONT BATIMENT SARL",
        "siren": siren,
        "registre": None,
        "ville": "PARIS",
        "cp": "75002",
        "denomination": None,
    }


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


def test_normalize_announcement_falls_back_to_registre_parsing():
    raw = {
        "id": "BX202500012345",
        "dateparution": "2025-10-15",
        "numeroannonce": "12345",
        "typeavis_lib": "Jugement",
        "familleavis_lib": "Procedures collectives",
        "tribunal": "Tribunal de commerce de Paris",
        "commercant": "DUPONT BATIMENT SARL",
        "siren": None,
        "registre": "334 393 806 RCS PARIS",
        "ville": "PARIS",
        "cp": "75002",
    }

    result = normalize_announcement(raw)

    assert result["id"] == "BX202500012345"
    assert result["siren_declared"] == "334393806"
    assert result["denomination"] is None  # not set on the raw dict in this test


def test_normalize_announcement_prefers_direct_siren_field():
    raw = {
        "id": "BX202500012346",
        "dateparution": "2025-10-16",
        "numeroannonce": "12346",
        "typeavis_lib": "Jugement",
        "familleavis_lib": "Procedures collectives",
        "tribunal": "Tribunal de commerce de Lyon",
        "commercant": "MARTIN TRAVAUX SARL",
        "siren": "552032534",
        "registre": "999999999 RCS LYON",
        "ville": "LYON",
        "cp": "69001",
    }

    result = normalize_announcement(raw)

    assert result["siren_declared"] == "552032534"


@responses.activate
def test_fetch_bodacc_announcements_follows_offset_pagination():
    responses.add(
        responses.GET,
        BODACC_API_URL,
        json={"results": [_raw_announcement("A", "111111111")]},
        status=200,
    )
    responses.add(
        responses.GET,
        BODACC_API_URL,
        json={"results": [_raw_announcement("B", "222222222")]},
        status=200,
    )
    responses.add(
        responses.GET,
        BODACC_API_URL,
        json={"results": []},
        status=200,
    )

    records = fetch_bodacc_announcements(dt.date(2025, 10, 1), dt.date(2025, 10, 16), page_size=1)

    assert [r["id"] for r in records] == ["A", "B"]
