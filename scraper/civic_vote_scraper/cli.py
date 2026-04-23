from __future__ import annotations

import json
import argparse
import csv
from pathlib import Path
from urllib.parse import urlparse

from civic_vote_scraper.adapters.legistar_playwright import LegistarPlaywrightDiscovery
from civic_vote_scraper.vote_extract import (
    scrape_votes_for_meetings,
    build_allowed_politician_names,
)
from civic_vote_scraper.form700_parser import parse_form700_workbook, write_outputs
from civic_vote_scraper.enrichment.form700_matcher import (
    enrich_vote_rows_with_registry_matches,
    match_vote_rows_against_form700_registry,
    write_matches_csv,
)


def write_csv(path: str | Path, rows):
    path = Path(path)
    fieldnames = []
    seen = set()
    for row in rows:
        for k in row.keys():
            if k == "minutes_text":
                continue
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            cleaned = dict(row)
            cleaned.pop("minutes_text", None)
            writer.writerow({k: cleaned.get(k, "") for k in fieldnames})


def normalize_person_name(name: str) -> str:
    return " ".join((name or "").strip().lower().split())


def last_name(name: str) -> str:
    parts = [p for p in normalize_person_name(name).split() if p]
    return parts[-1] if parts else ""


def build_registry_template(vote_rows, source_url: str, allowed_names: set[str] | None = None):
    print("[info] building starter Form 700 registry")
    site = urlparse(source_url).netloc
    print(f"[info] source site detected: {site}")

    template = {}
    seen = set()
    scanned = 0
    added = 0

    for row in vote_rows:
        scanned += 1
        if scanned % 1000 == 0:
            print(f"[info] registry scan progress: {scanned} vote rows scanned")

        person_raw = row.get("politician_name", "")
        person = normalize_person_name(person_raw)
        if not person or person in seen:
            continue

        if allowed_names is not None and person not in allowed_names:
            continue

        seen.add(person)
        surname = person.split()[-1] if person.split() else ""
        if not surname:
            continue

        template[surname] = {
            "politician_name": person_raw.strip(),
            "site": site,
            "form700_path": f"form700s/{surname}_entities.csv",
        }
        added += 1
        print(f"[info] added registry entry: {person_raw.strip()} -> form700s/{surname}_entities.csv")

    print(f"[info] registry build complete: {added} politicians added")
    return template


def main():
    ap = argparse.ArgumentParser(
        description="Civic vote scraper with SF Playwright discovery and Form 700 registry matching."
    )
    ap.add_argument("--url", default="https://sfgov.legistar.com/Calendar.aspx")
    ap.add_argument("--jurisdiction", default="San Francisco")
    ap.add_argument("--body-filter", default="")
    ap.add_argument("--page-limit", type=int, default=0, help="Max pages for discovery; ignored if meeting-limit is set")
    ap.add_argument("--meeting-limit", type=int, default=0, help="Max discovered meetings to process")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--out", default="votes.csv")
    ap.add_argument("--minutes-cache-dir", default="minutes_cache")
    ap.add_argument("--minutes-text-index", default="minutes_text_index.json")
    ap.add_argument("--form700-xlsx", default="")
    ap.add_argument("--form700-registry", default="")
    ap.add_argument("--form700-csv-out", default="form700_entities.csv")
    ap.add_argument("--form700-json-out", default="form700_entities.json")
    ap.add_argument("--form700-matches-out", default="form700_matches.csv")
    ap.add_argument("--min-confidence", type=float, default=0.75)
    args = ap.parse_args()

    discovery = LegistarPlaywrightDiscovery(
        url=args.url,
        jurisdiction=args.jurisdiction,
        body_filter=args.body_filter,
        headless=args.headless,
    )

    max_pages = 0 if args.meeting_limit > 0 else args.page_limit
    meeting_limit = args.meeting_limit if args.meeting_limit > 0 else 0

    meetings = discovery.discover_meetings(
        max_pages=max_pages,
        meeting_limit=meeting_limit,
    )
    print(f"Discovered {len(meetings)} meetings")
    allowed_names = None
    votes = scrape_votes_for_meetings(
        meetings,
        politician=None,
        allowed_names=allowed_names,
        cache_dir=args.minutes_cache_dir,
        text_artifacts_path=args.minutes_text_index,
    )
    print(f"Extracted {len(votes)} vote rows across all politicians")


    if args.form700_xlsx:
        print(f"[info] reading Form 700 workbook: {args.form700_xlsx}")
        records = parse_form700_workbook(args.form700_xlsx)
        print(f"[info] extracted {len(records)} Form 700 rows from workbook")
        write_outputs(records, args.form700_csv_out, args.form700_json_out)
        print(f"[info] wrote Form 700 CSV: {args.form700_csv_out}")
        print(f"[info] wrote Form 700 JSON: {args.form700_json_out}")

        owner_rows = []
        seen = set()
        for row in records:
            key = (
                row.get("owner_full_name", ""),
                row.get("owner_last_name", ""),
            )
            if key in seen:
                continue
            seen.add(key)
            owner_rows.append(row)

        allowed_names = build_allowed_politician_names(owner_rows)
        print(f"[info] built allowed politician-name set from Form 700: {len(allowed_names)} names")

    if args.form700_registry:
        registry_path = Path(args.form700_registry)

        if not registry_path.exists():
            print(f"[info] registry file not found: {registry_path}")
            template = build_registry_template(votes, args.url, allowed_names=allowed_names)
            registry_path.write_text(
                json.dumps(template, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"[info] starter registry written to: {registry_path}")

        print(f"[info] loading Form 700 registry: {registry_path}")
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
        print(f"[info] registry entries loaded: {len(registry)}")

        votes = enrich_vote_rows_with_registry_matches(
            votes,
            registry,
            min_confidence=args.min_confidence,
            allowed_names=allowed_names,
        )

        matches = match_vote_rows_against_form700_registry(
            votes,
            registry,
            min_confidence=args.min_confidence,
            allowed_names=allowed_names,
        )
        write_matches_csv(matches, args.form700_matches_out)
        print(f"[info] wrote Form 700 matches to {args.form700_matches_out}")

    write_csv(args.out, votes)
    print(f"Wrote vote output to {args.out}")


if __name__ == "__main__":
    main()
