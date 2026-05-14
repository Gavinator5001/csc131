from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Any

from civic_vote_scraper.adapters.form700_fppc_scraper import (
    Form700FPPCSync,
    Form700JurisdictionNotFound,
)
from civic_vote_scraper.adapters.legistar_playwright import LegistarPlaywrightDiscovery
from civic_vote_scraper.minutes_db import MinutesDatabase
from civic_vote_scraper.vote_extract import (
    build_allowed_politician_names,
    scrape_votes_for_meetings,
)
from civic_vote_scraper.enrichment.form700_matcher import (
    enrich_vote_rows_with_form700_rows,
    match_minutes_files_against_form700_rows,
    write_matches_csv,
    write_matches_json,
)


FORM700_ENTITY_NAME_FIELDS = (
    "entity_name",
    "raw_value",
    "name",
    "source_name",
    "sourceName",
    "business_name",
    "businessName",
    "investment_name",
    "investmentName",
    "property_name",
    "propertyName",
    "parcel_number_or_address",
    "parcelNumberOrAddress",
    "real_property_name_or_address",
    "realPropertyNameOrAddress",
    "lender_name",
    "lenderName",
    "gift_source_name",
    "giftSourceName",
    "travel_payment_source",
    "travelPaymentSource",
)

FORM700_SCHEDULE_FIELDS = ("_schedule", "schedule", "schedule_name", "scheduleName")
FORM700_RECORD_TYPE_FIELDS = ("_record_type", "record_type", "recordType", "type")

OWNER_FIELD_ALIASES = {
    "owner_first_name": ("owner_first_name", "first_name", "firstName", "filer_first_name", "filerFirstName"),
    "owner_middle_name": ("owner_middle_name", "middle_name", "middleName", "filer_middle_name", "filerMiddleName"),
    "owner_last_name": ("owner_last_name", "last_name", "lastName", "filer_last_name", "filerLastName"),
    "owner_full_name": ("owner_full_name", "full_name", "fullName", "filer_full_name", "filerFullName"),
    "filer_position_title": ("filer_position_title", "position_title", "position", "title"),
    "filer_agency_name": ("filer_agency_name", "agency_name", "agencyName", "agency"),
    "filer_entity_name": ("filer_entity_name", "entity_filer_name", "filerEntityName"),
    "jurisdiction": ("jurisdiction",),
    "_source_pdf_path": ("_source_pdf_path", "source_pdf_path", "pdf_path", "pdfPath", "file_path", "filePath"),
}

AGENCY_FIELD_ALIASES = (
    "filer_agency_name",
    "agency_name",
    "agencyName",
    "agency",
)


def norm_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()


def norm_key(value: Any) -> str:
    return norm_text(value).casefold()


def first_nonblank(row: dict, field_names: tuple[str, ...] | list[str]) -> str:
    for field_name in field_names:
        value = norm_text(row.get(field_name, ""))
        if value:
            return value
    return ""


def write_csv(path: str | Path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key == "minutes_text":
                continue
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            cleaned = dict(row)
            cleaned.pop("minutes_text", None)
            writer.writerow({key: cleaned.get(key, "") for key in fieldnames})


def write_json(path: str | Path, rows) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(list(rows), indent=2, ensure_ascii=False), encoding="utf-8")


def normalize_form700_db_row(row: dict) -> dict:
    normalized = dict(row)

    entity_name = first_nonblank(normalized, FORM700_ENTITY_NAME_FIELDS)
    normalized["entity_name"] = entity_name
    normalized["raw_value"] = first_nonblank(normalized, ("raw_value", "rawValue")) or entity_name
    normalized["_schedule"] = first_nonblank(normalized, FORM700_SCHEDULE_FIELDS)
    normalized["_record_type"] = first_nonblank(normalized, FORM700_RECORD_TYPE_FIELDS)

    for output_field, aliases in OWNER_FIELD_ALIASES.items():
        normalized[output_field] = first_nonblank(normalized, aliases)

    if not normalized["owner_full_name"]:
        normalized["owner_full_name"] = " ".join(
            part
            for part in [
                normalized.get("owner_first_name", ""),
                normalized.get("owner_middle_name", ""),
                normalized.get("owner_last_name", ""),
            ]
            if part
        ).strip()

    return normalized


def row_matches_agency(row: dict, agency_name: str) -> bool:
    if not norm_text(agency_name):
        return True
    target = norm_key(agency_name)
    agency = norm_key(first_nonblank(row, AGENCY_FIELD_ALIASES))
    return agency == target


def export_form700_database_for_cli(
    database: MinutesDatabase,
    out_csv: str | Path,
    out_json: str | Path,
    agency_name: str = "",
) -> list[dict]:
    """Export Form 700 rows by agencyName/filer_agency_name, not jurisdiction.

    The coordinate extractor may save the DB jurisdiction as "State". The actual
    local agency is stored on the row as agencyName/filer_agency_name, so this
    fetches all entity rows and applies the filter client-side using agency name.
    """
    raw_rows = database.fetch_form700_entity_rows(jurisdiction="")
    print(f"[info] fetched {len(raw_rows)} total Form 700 entity rows from database")

    rows = [normalize_form700_db_row(dict(row)) for row in raw_rows]
    if norm_text(agency_name):
        rows = [row for row in rows if row_matches_agency(row, agency_name)]
        print(f"[info] kept {len(rows)} Form 700 entity rows for agencyName '{agency_name}'")

    rows_with_entity = [row for row in rows if norm_text(row.get("entity_name", ""))]

    if rows and not rows_with_entity:
        sample_keys = sorted(str(key) for key in dict(rows[0]).keys())
        print(
            "[warn] Form 700 DB rows matched the agency filter, but none had a recognizable "
            f"entity-name column. First-row keys: {sample_keys}"
        )
        rows_to_write = rows
    else:
        rows_to_write = rows_with_entity

    print(f"[info] writing Form 700 CSV output: {out_csv}")
    write_csv(out_csv, rows_to_write)
    print(f"[info] writing Form 700 JSON output: {out_json}")
    write_json(out_json, rows_to_write)
    print(f"[info] exported {len(rows_to_write)} Form 700 entity rows to files")

    return rows_with_entity


def sync_form700s(args, database: MinutesDatabase) -> tuple[list[dict], set[str]]:
    if not args.skip_form700_sync:
        sync = Form700FPPCSync(
            search_url=args.form700_search_url,
            jurisdiction=args.jurisdiction,
            headless=args.headless,
        )
        try:
            stats = sync.sync(
                database_path=args.minutes_db,
                download_dir=args.form700_folder,
                reparse_existing_form700s=args.reparse_existing_form700s,
            )
        except Form700JurisdictionNotFound as exc:
            print(f"[warn] {exc}")
            stats = {
                "filers_seen": 0,
                "filings_seen": 0,
                "downloaded_filings": 0,
                "parsed_filings": 0,
            }
        print(
            f"[info] Form 700 sync stats: {stats['filers_seen']} filers, "
            f"{stats['filings_seen']} filings, {stats['downloaded_filings']} downloads, "
            f"{stats['parsed_filings']} parses"
        )

    form700_agency_name = args.form700_agency_name or args.jurisdiction

    # Owner rows are still read through the existing jurisdiction API because
    # that worked in your logs. If it returns 0, fall back to all owners so
    # matching can still proceed.
    owner_rows = database.fetch_form700_owner_rows(jurisdiction=args.jurisdiction)
    if not owner_rows:
        owner_rows = database.fetch_form700_owner_rows(jurisdiction="")
        owner_rows = [
            row for row in owner_rows
            if row_matches_agency(dict(row), form700_agency_name)
        ]
        print(
            f"[warn] no owner rows for jurisdiction '{args.jurisdiction}'; "
            f"kept {len(owner_rows)} owner rows by agencyName '{form700_agency_name}'"
        )

    allowed_names = build_allowed_politician_names(owner_rows)
    print(
        f"[info] loaded {len(owner_rows)} Form 700 owners for jurisdiction '{args.jurisdiction}'"
    )
    print(
        f"[info] built allowed politician-name set from Form 700 PDFs: {len(allowed_names)} names"
    )

    form700_rows = export_form700_database_for_cli(
        database=database,
        out_csv=args.form700_csv_out,
        out_json=args.form700_json_out,
        agency_name=form700_agency_name,
    )
    print(f"[info] loaded {len(form700_rows)} Form 700 entity rows for matching")
    return form700_rows, allowed_names


def run_once(args):
    database = MinutesDatabase(args.minutes_db)
    database.initialize()

    form700_rows, allowed_names = sync_form700s(args, database)

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

    votes = scrape_votes_for_meetings(
        meetings,
        politician=None,
        allowed_names=None,
        cache_dir=args.minutes_cache_dir,
        text_artifacts_path=args.minutes_text_index,
        database_path=args.minutes_db or None,
        reparse_existing_minutes=args.reparse_existing_minutes,
    )
    print(f"Extracted {len(votes)} new vote rows across all politicians")

    if args.minutes_db:
        votes = database.fetch_vote_rows()
        print(f"[info] loaded {len(votes)} total vote rows from minutes database")

    if form700_rows:
        votes = enrich_vote_rows_with_form700_rows(
            votes,
            form700_rows,
            min_confidence=args.min_confidence,
            allowed_names=allowed_names,
        )

        minutes_rows = database.fetch_minutes_text_rows()
        matches = match_minutes_files_against_form700_rows(
            minutes_rows,
            form700_rows,
            min_confidence=args.min_confidence,
        )
        write_matches_csv(matches, args.form700_matches_out)
        write_matches_json(matches, args.form700_matches_json_out)
        print(
            f"[info] wrote {len(matches)} full-minutes Form 700 matches "
            f"to {args.form700_matches_out} and {args.form700_matches_json_out}"
        )

    print(
        f"[info] database totals: {database.count_minutes()} minutes files, "
        f"{database.count_vote_rows()} vote rows, "
        f"{database.count_form700_filings()} Form 700 filings, "
        f"{database.count_form700_entities()} Form 700 entities"
    )

    write_csv(args.out, votes)
    print(f"Wrote vote output to {args.out}")


def build_parser():
    ap = argparse.ArgumentParser(
        description="Civic vote scraper with live minutes discovery, FPPC Form 700 sync, PDF parsing, and database-backed matching."
    )
    ap.add_argument("--url", default="https://sonoma-county.legistar.com/Calendar.aspx")
    ap.add_argument("--jurisdiction", default="County of Sonoma")
    ap.add_argument(
        "--form700-agency-name",
        default="",
        help="Agency name to use when exporting/matching Form 700 entities. Defaults to --jurisdiction. This filters agencyName/filer_agency_name, not jurisdiction.",
    )
    ap.add_argument("--body-filter", default="")
    ap.add_argument(
        "--page-limit",
        type=int,
        default=0,
        help="Max pages for discovery; ignored if meeting-limit is set",
    )
    ap.add_argument(
        "--meeting-limit",
        type=int,
        default=0,
        help="Max discovered meetings to process",
    )
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--out", default="votes.csv")
    ap.add_argument("--minutes-cache-dir", default="minutes_cache")
    ap.add_argument("--minutes-text-index", default="minutes_text_index.json")
    ap.add_argument("--minutes-db", default="minutes.db")
    ap.add_argument(
        "--reparse-existing-minutes",
        action="store_true",
        help="Re-parse minutes files that are already marked parsed in the database.",
    )
    ap.add_argument(
        "--reparse-existing-form700s",
        action="store_true",
        help="Re-parse downloaded Form 700 PDFs that are already marked parsed in the database.",
    )
    ap.add_argument(
        "--live",
        action="store_true",
        help="Keep searching for newly posted minutes on an interval.",
    )
    ap.add_argument(
        "--live-interval-minutes",
        type=float,
        default=60.0,
        help="Minutes to wait between live searches.",
    )
    ap.add_argument(
        "--form700-search-url",
        default="https://form700search.fppc.ca.gov/Search/SearchFilerForms.aspx",
    )
    ap.add_argument("--form700-folder", default="form700")
    ap.add_argument("--skip-form700-sync", action="store_true")
    ap.add_argument("--form700-csv-out", default="form700_entities.csv")
    ap.add_argument("--form700-json-out", default="form700_entities.json")
    ap.add_argument("--form700-matches-out", default="form700_matches.csv")
    ap.add_argument("--form700-matches-json-out", default="form700_matches.json")
    ap.add_argument("--min-confidence", type=float, default=0.75)
    return ap


def main():
    args = build_parser().parse_args()

    while True:
        try:
            print(
                "[info] live search cycle starting"
                if args.live
                else "[info] scraper run starting"
            )
            run_once(args)
            print(
                "[info] live search cycle complete"
                if args.live
                else "[info] scraper run complete"
            )
        except KeyboardInterrupt:
            print("[info] stop requested")
            raise
        except Exception as exc:
            print(f"[error] scraper run failed: {exc}")
            if not args.live:
                raise

        if not args.live:
            return

        interval_seconds = max(args.live_interval_minutes * 60, 1)
        print(f"[info] next live search in {interval_seconds / 60:.2f} minutes")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
