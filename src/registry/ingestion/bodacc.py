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
