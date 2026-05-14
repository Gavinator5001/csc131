from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

from civic_vote_scraper.minutes_db import MinutesDatabase


BAD_ENTITY_PATTERNS = [
    r"\bNAME\s+OF\s+BUSINESS\s+ENTITY\b",
    r"\bAME\s+OF\s+BUSINESS\s+ENTITY\b",
    r"\bNAME\s+OF\s+BUSINESS\s+ENTITY\s+OR\s+TRUST\b",
    r"\bNAME\s+OF\s+SOURCE\s+OF\s+INCOME\b",
    r"\bNAME\s+OF\s+LENDER\b",
    r"\bBUSINESS\s+ACTIVITY\b",
    r"\bIF\s+INVESTMENT\b",
    r"\bFILER.?S\s+VERIFICATION\b",
    r"\bVERIFICATION\b",
    r"\bSCHEDULE\b",
    r"\bFAIR\s+MARKET\s+VALUE\b",
    r"\bGROSS\s+INCOME\b",
    r"\bACQUIRED\b",
    r"\bDISPOSED\b",
    r"\bNATURE\s+OF\s+INVESTMENT\b",
    r"\bOFFICIAL\s+USE\s+ONLY\b",
    r"\bPUBLIC\s+DOCUMENT\b",
    r"\bCOUNTY\s+OF\s+DESCRIPTION\b",
    r"\bDATE\s+SIGNED\b",
    r"\bGENERAL\s+DESCRIPTION\b",
    r"\bPROPERTY\s+OWNERSHIP\s*/?\s*DEED\b",
    r"\bREPRESENTS\b",
    r"\bLEGISLATURE\b",
    r"\bFEDERAL\s+GOV\b",
    r"\bFOR\s+HOLDING\s+REAL\s+PROPERTY\b",
    r"\bINVESTMENT\s+HOLDING\s+COMPANY\b",
    r"\bSPOUSE\s+TITLE\b",
]

BAD_ENTITY_KEYS = {
    "",
    "arial narrow",
    "co owner",
    "commission",
    "general description",
    "non traded bdc",
    "of busine",
    "of real",
    "over",
    "personal",
    "president",
    "real",
    "signature",
}

BAD_CONTEXT_KEYS = {
    "",
    "s",
    "s name",
    "s.",
    "title",
    "agency",
    "position",
    "first name",
    "middle name",
    "last name",
    "filing type",
    "filing year",
    "due date",
    "filed date",
}


def norm_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


def norm_key(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", norm_text(value).lower())).strip()


def _normalize_date_field(value: Any) -> str:
    text = norm_text(value)
    if not text:
        return ""
    match = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4}(?:\s+\d{1,2}:\d{2}\s+[AP]M)?)\b", text, flags=re.I)
    if match:
        return match.group(1)
    year_match = re.search(r"\b(20\d{2}|19\d{2})\b", text)
    if year_match:
        return year_match.group(1)
    return text


def _sanitize_context_field(value: Any) -> str:
    text = norm_text(value)
    key = norm_key(text)
    if key in BAD_CONTEXT_KEYS:
        return ""
    if re.search(r"\b(do not use acronyms|public document|official use only)\b", text, flags=re.I):
        return ""
    return text


def _is_bad_entity_label(value: Any) -> bool:
    text = norm_text(value)
    key = norm_key(text)
    if not text or len(key) < 3:
        return True
    if key in BAD_ENTITY_KEYS:
        return True
    if len(text) > 180 or len(text.split()) > 18:
        return True
    if re.fullmatch(r"[\W\d_ ]+", text):
        return True
    for pattern in BAD_ENTITY_PATTERNS:
        if re.search(pattern, text, flags=re.I):
            return True
    return False


def is_valid_entity_name(value: Any) -> bool:
    return not _is_bad_entity_label(value)


def _is_valid_pdf_entity(value: Any) -> bool:
    return is_valid_entity_name(value)


def extract_form_700(input_path: str | Path) -> dict:
    try:
        from fppc700extract import extract_form_700 as package_extract_form_700
    except Exception as exc:  # pragma: no cover - depends on the app environment
        raise RuntimeError(f"fppc700extract is required for Form 700 parsing: {exc}") from exc
    return package_extract_form_700(str(input_path))


def _clean_entity(value: Any) -> str:
    text = norm_text(value).replace("\u25ba", "")
    text = re.sub(r"\{\s*0\s*\}", " ", text)
    text = re.sub(
        r"^(?:name of business entity or trust|name of business entity|name of lender|name of source of income)\s*[:\-]?\s*",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(r"\s+", " ", text).strip(" -:;,()[]{}")
    return text


def _require_coordinate_payload(payload: dict) -> None:
    """Fail loudly if the wrong extractor was imported or returned the wrong schema."""
    required_top_level = {"coverPage", "a1Investments", "a2Entities", "bProperties", "cIncomes"}
    missing = required_top_level - set(payload.keys())
    if missing:
        raise ValueError(
            "Form 700 parser expected the coordinate extractor payload. "
            f"Missing keys: {sorted(missing)}. "
            "Check that form700_parser.py imports `extract_form_700` from "
            "`fppc700extract.extract`, not from `fppc700extract` package root."
        )

    cover = payload.get("coverPage")
    if not isinstance(cover, dict):
        raise ValueError("Coordinate extractor returned coverPage, but it is not a dict.")

    if "agencyName" not in cover:
        raise ValueError(
            "Coordinate extractor returned coverPage without agencyName. "
            "This means the coordinate extractor schema is not being produced correctly; "
            "check parse_cover_page() and the cover-page layout JSON."
        )


def _owner_meta_from_coordinate_payload(payload: dict, filing_metadata: dict | None, source_pdf_path: str) -> dict:
    _require_coordinate_payload(payload)
    filing_metadata = filing_metadata or {}
    cover = payload["coverPage"]

    first = norm_text(cover["firstName"])
    middle = norm_text(cover["middleName"])
    last = norm_text(cover["lastName"])
    agency_name = norm_text(cover["agencyName"])
    position = norm_text(cover["position"])
    full = " ".join(part for part in [first, middle, last] if part).strip()

    # Agency name is the jurisdiction for this pipeline. The extractor's coverPage
    # currently has jurisdiction='State', so do not use that field.
    jurisdiction = agency_name

    return {
        "owner_last_name": last,
        "owner_first_name": first,
        "owner_middle_name": middle,
        "owner_full_name": full,
        "filer_position_title": position,
        "filer_agency_name": agency_name,
        "filer_entity_name": norm_text(filing_metadata.get("entity_name", "")),
        "filing_type": norm_text(filing_metadata.get("filing_type", "")),
        "filing_year": _normalize_date_field(filing_metadata.get("filing_year", "")),
        "due_date": _normalize_date_field(filing_metadata.get("due_date", "")),
        "filed_date": _normalize_date_field(filing_metadata.get("filed_date", "")),
        "jurisdiction": jurisdiction,
        "_source_pdf_path": source_pdf_path,
    }


def _base_record(schedule: str, record_type: str, entity_name: Any, owner_meta: dict, raw_value: Any = "") -> dict:
    entity = _clean_entity(entity_name)
    raw = norm_text(raw_value) or entity
    return {
        "_schedule": schedule,
        "_record_type": record_type,
        "entity_name": entity,
        "raw_value": raw,
        "owner_last_name": owner_meta["owner_last_name"],
        "owner_first_name": owner_meta["owner_first_name"],
        "owner_middle_name": owner_meta["owner_middle_name"],
        "owner_full_name": owner_meta["owner_full_name"],
        "filer_position_title": owner_meta.get("filer_position_title", ""),
        "filer_agency_name": owner_meta.get("filer_agency_name", ""),
        "filer_entity_name": owner_meta.get("filer_entity_name", ""),
        "jurisdiction": owner_meta.get("jurisdiction", ""),
        "_source_pdf_path": owner_meta.get("_source_pdf_path", ""),
    }


def _owner_name_keys(row: dict) -> set[str]:
    first = norm_text(row.get("owner_first_name", ""))
    middle = norm_text(row.get("owner_middle_name", ""))
    last = norm_text(row.get("owner_last_name", ""))
    full = norm_text(row.get("owner_full_name", ""))
    candidates = {
        full,
        " ".join(part for part in [first, middle, last] if part),
        " ".join(part for part in [first, last] if part),
        " ".join(part for part in [last, first, middle] if part),
        " ".join(part for part in [last, first] if part),
    }
    return {norm_key(candidate) for candidate in candidates if norm_key(candidate)}


def _entity_matches_owner_name(entity_name: str, row: dict) -> bool:
    entity_key = norm_key(entity_name)
    return bool(entity_key and entity_key in _owner_name_keys(row))


def _adapt_coordinate_records(payload: dict, owner_meta: dict) -> list[dict]:
    _require_coordinate_payload(payload)
    records: list[dict] = []

    for row in payload.get("a1Investments", []) or []:
        records.append(_base_record("A1", "investment", row.get("name", ""), owner_meta))

    for row in payload.get("a2Entities", []) or []:
        records.append(_base_record("A2", "business_entity_or_trust", row.get("name", ""), owner_meta))

        # A2 can also include a real property name/address. Keep it only if present and distinct.
        prop_name = norm_text(row.get("realPropertyNameOrAddress", "")) or norm_text(row.get("realPropertyDescription", ""))
        if prop_name:
            records.append(_base_record("A2", "real_property", prop_name, owner_meta))

        # Single source names are entity-like sources listed inside A2. Keep non-empty ones.
        for source_name in row.get("singleSourceNames", []) or []:
            if norm_text(source_name):
                records.append(_base_record("A2", "income_source", source_name, owner_meta))

    for row in payload.get("bProperties", []) or []:
        entity = row.get("parcelNumberOrAddress", "")
        records.append(_base_record("B", "real_property", entity, owner_meta))

    for row in payload.get("cIncomes", []) or []:
        records.append(_base_record("C", "income_source", row.get("sourceName", ""), owner_meta))

    # Intentionally exclude Schedule D and E from the flat entities file.
    return records


def sanitize_form700_records(records: list[dict]) -> list[dict]:
    cleaned_records = []
    seen = set()
    for row in records:
        schedule = norm_text(row.get("_schedule", ""))
        if schedule in {"D", "E"}:
            continue

        entity_name = _clean_entity(row.get("entity_name", ""))
        if not is_valid_entity_name(entity_name):
            continue
        if _entity_matches_owner_name(entity_name, row):
            continue

        dedupe_key = (
            norm_key(row.get("owner_full_name", "")),
            norm_key(row.get("_record_type", "")),
            norm_key(entity_name),
            norm_key(row.get("_source_pdf_path", "")),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        cleaned = dict(row)
        cleaned["_schedule"] = schedule
        cleaned["entity_name"] = entity_name
        cleaned["raw_value"] = norm_text(row.get("raw_value", "")) or entity_name
        cleaned["owner_first_name"] = norm_text(row.get("owner_first_name", ""))
        cleaned["owner_middle_name"] = norm_text(row.get("owner_middle_name", ""))
        cleaned["owner_last_name"] = norm_text(row.get("owner_last_name", ""))
        cleaned["owner_full_name"] = norm_text(row.get("owner_full_name", ""))
        cleaned["filer_position_title"] = _sanitize_context_field(row.get("filer_position_title", ""))
        cleaned["filer_agency_name"] = _sanitize_context_field(row.get("filer_agency_name", ""))
        cleaned["jurisdiction"] = norm_text(row.get("jurisdiction", ""))
        cleaned_records.append(cleaned)
    return cleaned_records


def extract_form700_metadata_from_pdf(input_path: str | Path, filing_metadata: dict | None = None) -> dict:
    input_path = Path(input_path)
    payload = extract_form_700(input_path)
    return _owner_meta_from_coordinate_payload(payload, filing_metadata, str(input_path))


def parse_form700_pdf(input_path: str | Path, filing_metadata: dict | None = None) -> list[dict]:
    input_path = Path(input_path)
    print(f"[info] opening Form 700 PDF: {input_path}")
    payload = extract_form_700(input_path)
    owner_meta = _owner_meta_from_coordinate_payload(payload, filing_metadata, str(input_path))
    records = _adapt_coordinate_records(payload, owner_meta)
    records = sanitize_form700_records(records)
    print(f"[info] Form 700 PDF parsing complete: {len(records)} total rows")
    return records


def write_outputs(records: list[dict], out_csv: str | Path, out_json: str | Path) -> None:
    out_csv = Path(out_csv)
    out_json = Path(out_json)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    print(f"[info] writing Form 700 CSV output: {out_csv}")
    print(f"[info] writing Form 700 JSON output: {out_json}")

    fieldnames: list[str] = []
    seen = set()
    for row in records:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    out_json.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print("[info] finished writing Form 700 outputs")


def export_form700_database(
    database_path: str | Path,
    out_csv: str | Path,
    out_json: str | Path,
    jurisdiction: str = "",
) -> list[dict]:
    database = MinutesDatabase(database_path)
    database.initialize()

    # For this pipeline the effective jurisdiction saved by the parser is the agency name.
    rows = database.fetch_form700_entity_rows(jurisdiction=jurisdiction)
    rows = sanitize_form700_records(rows)
    write_outputs(rows, out_csv, out_json)
    return rows
