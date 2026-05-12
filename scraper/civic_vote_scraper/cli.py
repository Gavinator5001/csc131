from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

from civic_vote_scraper.adapters.form700_fppc_scraper import (
    Form700FPPCSync,
    Form700JurisdictionNotFound,
)
from civic_vote_scraper.adapters.legistar_playwright import LegistarPlaywrightDiscovery
from civic_vote_scraper.form700_parser import export_form700_database
from civic_vote_scraper.minutes_db import MinutesDatabase
from civic_vote_scraper.vote_extract import (
    build_allowed_politician_names,
    scrape_votes_for_meetings,
)
from civic_vote_scraper.enrichment.form700_matcher import (
    enrich_vote_rows_with_form700_rows,
    match_vote_rows_against_form700_rows,
    write_matches_csv,
)


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
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            cleaned = dict(row)
            cleaned.pop("minutes_text", None)
            writer.writerow({key: cleaned.get(key, "") for key in fieldnames})


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

    owner_rows = database.fetch_form700_owner_rows(jurisdiction=args.jurisdiction)
    allowed_names = build_allowed_politician_names(owner_rows)
    print(
        f"[info] loaded {len(owner_rows)} Form 700 owners for jurisdiction '{args.jurisdiction}'"
    )
    print(
        f"[info] built allowed politician-name set from Form 700 PDFs: {len(allowed_names)} names"
    )

    form700_rows = export_form700_database(
        database_path=args.minutes_db,
        out_csv=args.form700_csv_out,
        out_json=args.form700_json_out,
        jurisdiction=args.jurisdiction,
    )
    print(f"[info] exported {len(form700_rows)} Form 700 entity rows from database")
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

        matches = match_vote_rows_against_form700_rows(
            votes,
            form700_rows,
            min_confidence=args.min_confidence,
            allowed_names=allowed_names,
        )
        write_matches_csv(matches, args.form700_matches_out)
        print(f"[info] wrote Form 700 matches to {args.form700_matches_out}")

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
    ap.add_argument("--url", default="https://sfgov.legistar.com/Calendar.aspx")
    ap.add_argument("--jurisdiction", default="San Francisco")
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
