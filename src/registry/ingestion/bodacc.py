"""Ingestion of BODACC legal announcements (bootstrap and daily diffs)."""

from __future__ import annotations

import re

SIREN_IN_TEXT_PATTERN = re.compile(r"\b(\d[\d ]{0,11}\d)\b")


def extract_siren_from_registre(registre: str | None) -> str | None:
    if not registre:
        return None
    match = SIREN_IN_TEXT_PATTERN.search(registre)
    if not match:
        return None
    digits = match.group(1).replace(" ", "")
    if len(digits) != 9:
        return None
    return digits


BODACC_COLUMNS = [
    "id",
    "dateparution",
    "numeroannonce",
    "typeavis_lib",
    "familleavis_lib",
    "tribunal",
    "commercant",
    "siren_declared",
    "ville",
    "cp",
    "denomination",
]


def normalize_announcement(raw: dict) -> dict:
    siren = raw.get("siren") or extract_siren_from_registre(raw.get("registre"))
    return {
        "id": raw.get("id"),
        "dateparution": raw.get("dateparution"),
        "numeroannonce": raw.get("numeroannonce"),
        "typeavis_lib": raw.get("typeavis_lib"),
        "familleavis_lib": raw.get("familleavis_lib"),
        "tribunal": raw.get("tribunal"),
        "commercant": raw.get("commercant"),
        "siren_declared": siren,
        "ville": raw.get("ville"),
        "cp": raw.get("cp"),
        "denomination": raw.get("denomination"),
    }
