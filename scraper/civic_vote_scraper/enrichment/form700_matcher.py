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

ALIAS_MAP = {
    "alphabet": ["google", "youtube", "waymo"],
    "meta": ["facebook", "instagram", "whatsapp"],
    "amazon": ["aws", "whole foods"],
    "berkshire hathaway": ["berkshire"],
    "exxon mobil": ["exxon", "xom"],
}

FORM700_HINT_COLUMNS = [
    "entity_name", "name", "issuer", "company", "business entity",
    "investment", "source", "asset", "stock", "security", "lender"
]


@dataclass
class InvestmentEntity:
    raw_name: str
    normalized_name: str
    aliases: List[str]
    record_type: str = ""


@dataclass
class MatterMatch:
    matter_id: Optional[str]
    meeting_date: Optional[str]
    body: Optional[str]
    matter_title: Optional[str]
    result: Optional[str]
    matched_company: str
    matched_alias: str
    confidence: float
    record_type: str = ""
    matched_form700_owner: str = ""


def normalize_company_name(name: str) -> str:
    text = (name or "").strip().lower()
    text = re.sub(r"&", " and ", text)
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    tokens = [t for t in text.split() if t and t not in STOPWORDS]
    return " ".join(tokens).strip()


def normalize_person_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip()).lower()


def last_name(name: str) -> str:
    parts = [p for p in normalize_person_name(name).split() if p]
    return parts[-1] if parts else ""


def alias_candidates(raw_name: str) -> List[str]:
    base = normalize_company_name(raw_name)
    if not base:
        return []
    aliases = {base}
    parts = base.split()
    if len(parts) >= 2:
        aliases.add(" ".join(parts[:2]))
    if parts:
        aliases.add(parts[0])
    aliases.update(ALIAS_MAP.get(base, []))
    return sorted(a for a in aliases if len(a) >= 3)


def parse_form700_entities(path: str | Path) -> List[InvestmentEntity]:
    path = Path(path)
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = data if isinstance(data, list) else data.get("rows", [])
        return _entities_from_rows(rows)

    text = path.read_text(encoding="utf-8", errors="ignore")
    sample = text[:4096]
    dialect = csv.Sniffer().sniff(sample) if sample.strip() else csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    return _entities_from_rows(list(reader))


def _entities_from_rows(rows):
    out = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw = ""
        for key, value in row.items():
            if value and any(hint in key.lower() for hint in FORM700_HINT_COLUMNS):
                raw = str(value)
                break
        if not raw:
            continue
        norm = normalize_company_name(raw)
        if not norm:
            continue
        if norm not in out:
            out[norm] = InvestmentEntity(
                raw_name=raw.strip(),
                normalized_name=norm,
                aliases=alias_candidates(raw),
                record_type=str(row.get("_record_type", "")),
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
    for alias in entity.aliases:
        pattern = r"\b" + re.escape(alias.lower()) + r"\b"
        if re.search(pattern, blob):
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
                    matched_company=entity.raw_name,
                    matched_alias=alias,
                    confidence=confidence,
                    record_type=entity.record_type,
                    matched_form700_owner=matched_form700_owner,
                )
            )
    return matches


def match_vote_rows_against_form700_registry(
    vote_rows: Iterable[dict],
    form700_registry: Dict[str, str | Path],
    min_confidence: float = 0.75,
    allowed_names: set[str] | None = None,
):
    rows = list(vote_rows)
    print(f"[info] starting registry-based Form 700 matching for {len(rows)} vote rows")

    entity_cache: Dict[str, List[InvestmentEntity]] = {}
    all_matches: List[MatterMatch] = []

    for i, row in enumerate(rows, start=1):
        if i % 1000 == 0:
            print(f"[info] Form 700 match progress: {i} vote rows checked")

        politician = row.get("politician_name", "")
        owner_key = normalize_person_name(politician)

        if allowed_names is not None and owner_key not in allowed_names:
            continue
        lookup = owner_key if owner_key in form700_registry else last_name(owner_key)
        registry_entry = form700_registry.get(lookup)

        if not registry_entry:
            continue

        if isinstance(registry_entry, str):
            form700_path = registry_entry
            matched_owner = lookup
            site = ""
        else:
            form700_path = registry_entry.get("form700_path", "")
            matched_owner = registry_entry.get("politician_name", lookup)
            site = registry_entry.get("site", "")

        if not form700_path:
            print(f"[info] registry entry missing form700_path for {lookup}")
            continue

        form700_path_obj = Path(form700_path)
        if not form700_path_obj.exists():
            print(f"[info] missing Form 700 file for {matched_owner} ({site}): {form700_path_obj}")
            continue

        cache_key = str(form700_path_obj)
        if cache_key not in entity_cache:
            print(f"[info] loading Form 700 entities for {matched_owner} from {form700_path_obj}")
            entity_cache[cache_key] = parse_form700_entities(form700_path_obj)

        matches = match_matters_to_investments(
            [row],
            entity_cache[cache_key],
            min_confidence=min_confidence,
            matched_form700_owner=matched_owner,
        )
        if matches:
            print(f"[info] matched {len(matches)} Form 700 entities for {matched_owner} ({site})")
        all_matches.extend(matches)

    print(f"[info] Form 700 matching complete: {len(all_matches)} total matches")
    return all_matches


def enrich_vote_rows_with_registry_matches(
    vote_rows: Iterable[dict],
    form700_registry: Dict[str, str | Path],
    min_confidence: float = 0.75,
    allowed_names: set[str] | None = None,
):
    rows = list(vote_rows)
    matches = match_vote_rows_against_form700_registry(rows, form700_registry, min_confidence=min_confidence, allowed_names=allowed_names)

    by_key = {}
    for m in matches:
        key = (m.matter_id, m.meeting_date, m.matter_title, m.matched_form700_owner)
        by_key.setdefault(key, []).append(m)

    enriched = []
    for row in rows:
        owner_key = normalize_person_name(row.get("politician_name", ""))
        lookup = owner_key if owner_key in form700_registry else last_name(owner_key)
        key = (row.get("matter_id"), row.get("meeting_date"), row.get("matter_title") or row.get("matter_name"), lookup)
        row_matches = by_key.get(key, [])

        row = dict(row)
        row["matched_form700_owner"] = lookup if row_matches else ""
        row["form700_company_match"] = "; ".join(m.matched_company for m in row_matches)
        row["form700_alias_match"] = "; ".join(m.matched_alias for m in row_matches)
        row["form700_match_confidence"] = max((m.confidence for m in row_matches), default="")
        row["form700_match_count"] = len(row_matches)
        row["form700_record_types"] = "; ".join(sorted({m.record_type for m in row_matches if m.record_type}))
        enriched.append(row)

    return enriched


def write_matches_csv(matches, path: str | Path):
    path = Path(path)
    fieldnames = list(asdict(matches[0]).keys()) if matches else [
        "matter_id", "meeting_date", "body", "matter_title", "result",
        "matched_company", "matched_alias", "confidence", "record_type", "matched_form700_owner"
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in matches:
            writer.writerow(asdict(m))
