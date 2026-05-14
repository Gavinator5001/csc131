from __future__ import annotations

import csv
import io
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

STOPWORDS = {
    "inc", "inc.", "corp", "corp.", "corporation", "co", "co.", "company", "companies",
    "llc", "lp", "llp", "plc", "ltd", "ltd.", "holdings", "holding", "group", "partners",
    "partner", "ventures", "capital", "fund", "trust", "series", "class", "common", "preferred"
}

FORM700_HINT_COLUMNS = [
    "entity_name", "raw_value", "issuer", "company", "business entity",
    "investment", "source of income", "source", "asset", "stock", "security", "lender"
]


@dataclass
class InvestmentEntity:
    raw_name: str
    normalized_name: str
    aliases: List[str]
    record_type: str = ""
    owner_full_name: str = ""


@dataclass
class MatterMatch:
    matter_id: Optional[str]
    meeting_date: Optional[str]
    body: Optional[str]
    matter_title: Optional[str]
    result: Optional[str]
    minutes_cache_key: str = ""
    source_url: str = ""
    matched_company: str = ""
    matched_alias: str = ""
    confidence: float = 0.0
    record_type: str = ""
    matched_form700_owner: str = ""


def normalize_company_name(name: str) -> str:
    text = (name or "").strip().lower()
    text = re.sub(r"&", " and ", text)
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    tokens = [t for t in text.split() if t and t not in STOPWORDS]
    return " ".join(tokens).strip()


def normalize_exact_entity_name(name: str) -> str:
    text = (name or "").strip().lower()
    text = re.sub(r"&", " and ", text)
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_search_text(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"&", " and ", text)
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_person_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip()).lower()


def normalize_person_key(name: str) -> str:
    text = (name or "").strip().lower()
    text = re.sub(r"\(cid:\s*\d+\)", " ", text)
    text = re.sub(r"\bcid\s*\d+\b", " ", text)
    text = re.sub(r"[^a-z\s]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def last_name(name: str) -> str:
    parts = [p for p in normalize_person_name(name).split() if p]
    return parts[-1] if parts else ""


def alias_candidates(raw_name: str) -> List[str]:
    base = normalize_exact_entity_name(raw_name)
    if not base:
        return []
    return [base] if len(base) >= 3 else []


def parse_form700_entities(path: str | Path) -> List[InvestmentEntity]:
    path = Path(path)
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = data if isinstance(data, list) else data.get("rows", [])
        return entities_from_rows(rows)

    text = path.read_text(encoding="utf-8", errors="ignore")
    sample = text[:4096]
    dialect = csv.Sniffer().sniff(sample) if sample.strip() else csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    return entities_from_rows(list(reader))


def _extract_entity_value(row: dict) -> str:
    skip_prefixes = ("owner_", "filer_", "matched_", "meeting_", "matter_", "politician_")
    ranked = []
    for key, value in row.items():
        if not value:
            continue
        key_lower = key.lower()
        if key_lower.startswith(skip_prefixes):
            continue
        score = max((len(hint) for hint in FORM700_HINT_COLUMNS if hint in key_lower), default=0)
        if score:
            ranked.append((score, key_lower == "entity_name", str(value)))
    if not ranked:
        return ""
    ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return ranked[0][2]


def _owner_name_keys(row: dict) -> set[str]:
    first = str(row.get("owner_first_name", "")).strip()
    middle = str(row.get("owner_middle_name", "")).strip()
    last = str(row.get("owner_last_name", "")).strip()
    full = str(row.get("owner_full_name", "")).strip()
    if not full:
        full = " ".join(part for part in [first, middle, last] if part).strip()

    candidates = {
        full,
        " ".join(part for part in [first, middle, last] if part),
        " ".join(part for part in [first, last] if part),
        " ".join(part for part in [last, first, middle] if part),
        " ".join(part for part in [last, first] if part),
    }
    return {normalize_person_key(candidate) for candidate in candidates if normalize_person_key(candidate)}


def _entity_is_owner_name(raw_entity: str, row: dict) -> bool:
    entity_key = normalize_person_key(raw_entity)
    return bool(entity_key and entity_key in _owner_name_keys(row))


def entities_from_rows(rows: Sequence[dict]) -> List[InvestmentEntity]:
    out = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw = _extract_entity_value(row)
        if not raw:
            continue
        if _entity_is_owner_name(raw, row):
            continue
        norm = normalize_exact_entity_name(raw)
        if not norm:
            continue

        owner = str(row.get("owner_full_name", "")).strip()
        unique_key = (normalize_person_name(owner), norm)
        if unique_key not in out:
            out[unique_key] = InvestmentEntity(
                raw_name=raw.strip(),
                normalized_name=norm,
                aliases=alias_candidates(raw),
                record_type=str(row.get("_record_type", "")),
                owner_full_name=owner,
            )
    return list(out.values())


def matter_text_blob(matter: dict) -> str:
    parts = [
        matter.get("matter_title"),
        matter.get("matter_name"),
        matter.get("matter_id"),
        matter.get("result"),
        matter.get("action_text"),
        matter.get("event_item_title"),
        matter.get("minutes_text"),
    ]
    return " ".join(p for p in parts if p).lower()


def score_match(blob: str, entity: InvestmentEntity):
    best = None
    normalized_blob = normalize_search_text(blob)
    for alias in entity.aliases:
        pattern = r"\b" + re.escape(alias.lower()) + r"\b"
        normalized_pattern = r"\b" + re.escape(normalize_search_text(alias)) + r"\b"
        if re.search(pattern, blob.lower()) or re.search(normalized_pattern, normalized_blob):
            score = 0.72
            if alias == entity.normalized_name:
                score = 0.92
            elif len(alias.split()) >= 2:
                score = 0.82
            if best is None or score > best[0]:
                best = (score, alias)
    return best


def match_matters_to_investments(
    matters: Iterable[dict],
    entities: Sequence[InvestmentEntity],
    min_confidence: float = 0.75,
    matched_form700_owner: str = "",
) -> List[MatterMatch]:
    matches: List[MatterMatch] = []
    for matter in matters:
        blob = matter_text_blob(matter)
        if not blob.strip():
            continue
        for entity in entities:
            scored = score_match(blob, entity)
            if not scored:
                continue
            confidence, alias = scored
            if confidence < min_confidence:
                continue
            matches.append(
                MatterMatch(
                    matter_id=matter.get("matter_id"),
                    meeting_date=matter.get("meeting_date"),
                    body=matter.get("body"),
                    matter_title=matter.get("matter_title") or matter.get("matter_name"),
                    result=matter.get("result"),
                    minutes_cache_key=matter.get("minutes_cache_key", ""),
                    source_url=matter.get("source_url", "") or matter.get("minutes_url", ""),
                    matched_company=entity.raw_name,
                    matched_alias=alias,
                    confidence=confidence,
                    record_type=entity.record_type,
                    matched_form700_owner=matched_form700_owner or entity.owner_full_name,
                )
            )
    return matches


def _group_entities_by_owner(rows: Sequence[dict]) -> Dict[str, List[InvestmentEntity]]:
    grouped: Dict[str, List[InvestmentEntity]] = {}
    for entity in entities_from_rows(rows):
        owner_full = normalize_person_name(entity.owner_full_name)
        owner_last = last_name(owner_full)
        for owner_key in {owner_full, owner_last}:
            if not owner_key:
                continue
            grouped.setdefault(owner_key, []).append(entity)
    return grouped


def match_vote_rows_against_form700_rows(
    vote_rows: Iterable[dict],
    form700_rows: Sequence[dict],
    min_confidence: float = 0.75,
    allowed_names: set[str] | None = None,
):
    rows = list(vote_rows)
    print(f"[info] starting database-backed Form 700 matching for {len(rows)} vote rows")

    entities_by_owner = _group_entities_by_owner(form700_rows)
    all_matches: List[MatterMatch] = []

    for i, row in enumerate(rows, start=1):
        if i % 1000 == 0:
            print(f"[info] Form 700 match progress: {i} vote rows checked")

        politician = row.get("politician_name", "")
        owner_full = normalize_person_name(politician)
        owner_last = last_name(owner_full)

        if allowed_names is not None and owner_full not in allowed_names and owner_last not in allowed_names:
            continue

        entities = []
        seen = set()
        for owner_key in [owner_full, owner_last]:
            for entity in entities_by_owner.get(owner_key, []):
                dedupe_key = (normalize_person_name(entity.owner_full_name), entity.normalized_name)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                entities.append(entity)

        if not entities:
            continue

        matches = match_matters_to_investments(
            [row],
            entities,
            min_confidence=min_confidence,
        )
        if matches:
            print(f"[info] matched {len(matches)} Form 700 entities for {politician}")
        all_matches.extend(matches)

    print(f"[info] Form 700 matching complete: {len(all_matches)} total matches")
    return all_matches


def _read_minutes_text(minutes_row: dict) -> str:
    inline = minutes_row.get("minutes_text", "")
    if inline:
        return str(inline)

    text_path = minutes_row.get("text_path", "")
    if not text_path:
        return ""
    path = Path(text_path)
    if not path.exists():
        print(f"[warn] minutes text file missing for matching: {path}")
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def minutes_file_to_matter(minutes_row: dict) -> dict:
    text = _read_minutes_text(minutes_row)
    title = (
        minutes_row.get("meeting_title")
        or f"{minutes_row.get('body', '')} {minutes_row.get('meeting_date', '')}".strip()
        or "Minutes file"
    )
    return {
        "matter_id": "",
        "meeting_date": minutes_row.get("meeting_date", ""),
        "body": minutes_row.get("body", ""),
        "matter_title": title,
        "result": "",
        "minutes_text": text,
        "minutes_cache_key": minutes_row.get("minutes_cache_key", ""),
        "source_url": minutes_row.get("minutes_url", "") or minutes_row.get("source_url", ""),
    }


def match_minutes_files_against_form700_rows(
    minutes_rows: Iterable[dict],
    form700_rows: Sequence[dict],
    min_confidence: float = 0.75,
):
    rows = list(minutes_rows)
    entities = entities_from_rows(form700_rows)
    print(
        f"[info] scanning {len(rows)} full minutes files against "
        f"{len(entities)} Form 700 entities"
    )

    all_matches: List[MatterMatch] = []
    seen = set()
    for i, row in enumerate(rows, start=1):
        if i % 100 == 0:
            print(f"[info] full-minutes Form 700 match progress: {i} minutes files checked")

        matter = minutes_file_to_matter(row)
        if not matter.get("minutes_text", "").strip():
            continue

        matches = match_matters_to_investments(
            [matter],
            entities,
            min_confidence=min_confidence,
        )
        for match in matches:
            key = (
                match.minutes_cache_key,
                normalize_person_name(match.matched_form700_owner),
                normalize_exact_entity_name(match.matched_company),
                match.matched_alias,
            )
            if key in seen:
                continue
            seen.add(key)
            all_matches.append(match)

    print(f"[info] full-minutes Form 700 matching complete: {len(all_matches)} total matches")
    return all_matches


def enrich_vote_rows_with_form700_rows(
    vote_rows: Iterable[dict],
    form700_rows: Sequence[dict],
    min_confidence: float = 0.75,
    allowed_names: set[str] | None = None,
):
    rows = list(vote_rows)
    matches = match_vote_rows_against_form700_rows(
        rows,
        form700_rows,
        min_confidence=min_confidence,
        allowed_names=allowed_names,
    )

    by_key = {}
    for match in matches:
        owner_full = normalize_person_name(match.matched_form700_owner)
        owner_last = last_name(owner_full)
        for owner_key in {owner_full, owner_last}:
            if not owner_key:
                continue
            key = (match.matter_id, match.meeting_date, match.matter_title, owner_key)
            by_key.setdefault(key, []).append(match)

    enriched = []
    for row in rows:
        owner_full = normalize_person_name(row.get("politician_name", ""))
        owner_last = last_name(owner_full)
        row_matches = []
        for owner_key in [owner_full, owner_last]:
            key = (
                row.get("matter_id"),
                row.get("meeting_date"),
                row.get("matter_title") or row.get("matter_name"),
                owner_key,
            )
            row_matches.extend(by_key.get(key, []))

        deduped = []
        seen = set()
        for match in row_matches:
            key = (
                match.matched_form700_owner,
                match.matched_company,
                match.matched_alias,
                match.record_type,
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(match)

        row = dict(row)
        row["matched_form700_owner"] = "; ".join(sorted({m.matched_form700_owner for m in deduped if m.matched_form700_owner}))
        row["form700_company_match"] = "; ".join(m.matched_company for m in deduped)
        row["form700_alias_match"] = "; ".join(m.matched_alias for m in deduped)
        row["form700_match_confidence"] = max((m.confidence for m in deduped), default="")
        row["form700_match_count"] = len(deduped)
        row["form700_record_types"] = "; ".join(sorted({m.record_type for m in deduped if m.record_type}))
        enriched.append(row)

    return enriched


def write_matches_csv(matches, path: str | Path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(asdict(matches[0]).keys()) if matches else [
        "matter_id", "meeting_date", "body", "matter_title", "result",
        "minutes_cache_key", "source_url", "matched_company", "matched_alias",
        "confidence", "record_type", "matched_form700_owner"
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for match in matches:
            writer.writerow(asdict(match))


def write_matches_json(matches, path: str | Path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(match) for match in matches]
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
