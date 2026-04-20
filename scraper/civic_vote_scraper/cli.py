from __future__ import annotations

import argparse
import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from civic_vote_scraper.detector import choose_adapter
from civic_vote_scraper.models import VoteRecord


def write_csv(path: str, rows: List[VoteRecord]) -> None:
    dict_rows = [r.to_dict() for r in rows]
    if not dict_rows:
        with open(path, "w", newline="", encoding="utf-8") as handle:
            handle.write("")
        return
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(dict_rows[0].keys()))
        writer.writeheader()
        writer.writerows(dict_rows)


def write_json(path: str, rows: List[VoteRecord]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump([r.to_dict() for r in rows], handle, indent=2, ensure_ascii=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape civic meeting sites for a politician's voting history")
    parser.add_argument("--url", required=True, help="Base public URL or archive page")
    parser.add_argument("--jurisdiction", required=True, help="Jurisdiction label")
    parser.add_argument("--politician", required=True, help="Politician name to search for")
    parser.add_argument("--limit", type=int, default=20, help="Maximum meetings to inspect")
    parser.add_argument("--out", default="votes.csv", help="Output CSV or JSON path")
    parser.add_argument("--list-meetings", action="store_true", help="Only list discovered meeting URLs")
    parser.add_argument("--workers", type=int, default=6, help="Number of concurrent meeting fetches")
    parser.add_argument("--html-only", action="store_true", help="Skip linked PDFs and linked HTML documents for faster scans")
    parser.add_argument("--body-filter", default="", help="Optional calendar body filter, e.g. 'Board of Supervisors'")
    parser.add_argument("--use-playwright-discovery", action="store_true", help="Use Playwright to drive meeting discovery on dynamic calendars")
    parser.add_argument("--playwright-headed", action="store_true", help="Run Playwright discovery in a visible browser window")
    args = parser.parse_args()

    adapter_cls = choose_adapter(args.url)
    adapter = adapter_cls(
        base_url=args.url,
        jurisdiction=args.jurisdiction,
        body_filter=args.body_filter,
        use_playwright_discovery=args.use_playwright_discovery,
        playwright_headless=not args.playwright_headed,
    )

    meetings = adapter.discover_meetings(limit=args.limit)
    if args.list_meetings:
        for meeting in meetings:
            print(f"{meeting.meeting_date or ''}	{meeting.body}	{meeting.meeting_url}")
        return 0

    all_rows: List[VoteRecord] = []
    workers = max(1, args.workers)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(adapter.extract_votes, meeting, args.politician, args.html_only): meeting
            for meeting in meetings
        }
        for future in as_completed(future_map):
            meeting = future_map[future]
            print(f"[info] scanning {meeting.meeting_url}", file=sys.stderr)
            try:
                all_rows.extend(future.result())
            except Exception as exc:
                print(f"[warn] failed on {meeting.meeting_url}: {exc}", file=sys.stderr)

    if args.out.lower().endswith(".json"):
        write_json(args.out, all_rows)
    else:
        write_csv(args.out, all_rows)

    print(f"Wrote {len(all_rows)} vote rows to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
