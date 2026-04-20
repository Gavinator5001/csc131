# civic_vote_scraper

Starter project for scraping voting history across Legistar and non-Legistar public meeting sites.

## What it does

- Detects Legistar vs generic archive pages from the URL.
- Discovers meeting detail links.
- Extracts vote-like lines for a named politician from HTML pages and linked PDFs.
- Writes a unified CSV or JSON output.
- Speeds up scans with HTTP caching, parallel meeting fetches, and cheap HTML prefilters before linked-document downloads.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

List meetings:

```bash
python -m civic_vote_scraper.cli \
  --url "https://sfgov.legistar.com/Calendar.aspx" \
  --jurisdiction "San Francisco" \
  --politician "Stephen Sherrill" \
  --list-meetings
```

Scrape votes:

```bash
python -m civic_vote_scraper.cli \
  --url "https://sfgov.legistar.com/Calendar.aspx" \
  --jurisdiction "San Francisco" \
  --politician "Stephen Sherrill" \
  --limit 15 \
  --workers 6 \
  --out stephen_sherrill_votes.csv
```

Fast HTML-only pass:

```bash
python -m civic_vote_scraper.cli \
  --url "https://sfgov.legistar.com/Calendar.aspx" \
  --jurisdiction "San Francisco" \
  --politician "Stephen Sherrill" \
  --limit 15 \
  --html-only \
  --out stephen_sherrill_votes.json
```

Generic archive page:

```bash
python -m civic_vote_scraper.cli \
  --url "https://example.gov/meetings" \
  --jurisdiction "Example City" \
  --politician "Jane Doe" \
  --out jane_doe_votes.json
```

## Optimization notes

The optimized version adds four simple speedups:

- `--workers` parallelizes meeting-level scans.
- `--html-only` skips linked documents when you want a quick pass.
- The HTTP client caches repeated text and PDF fetches by URL.
- The adapters only fetch linked documents when the meeting HTML already looks relevant or when the meeting page itself already produced vote-like rows.

On Legistar pages, linked PDFs are also narrowed to likely vote-bearing documents such as minutes, action minutes, results, and vote pages, instead of downloading every PDF.

## Notes

This is still a starter project, not a perfect named-vote extractor. Public meeting sites vary a lot, so the design is adapter-based:

- `adapters/legistar.py` handles public Legistar meeting detail pages and linked minutes/packets.
- `adapters/generic_archive.py` provides a fallback for ordinary archive pages.
- `extractors/html_votes.py` and `extractors/pdf_votes.py` look for vote-like lines containing the politician's name.

## Next upgrades

- Add PrimeGov, CivicClerk, BoardDocs, NovusAGENDA, and eScribe adapters.
- Add Playwright support for JavaScript-heavy sites.
- Add date-aware filtering and paginated calendar crawling.
- Add matter-level parsing, roster normalization, and stronger confidence scoring from structured vote tables.


## San Francisco Playwright discovery

For sfgov.legistar.com, the most reliable discovery path is browser-driven calendar search. Install Playwright browsers first:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

Example:

```bash
python -m civic_vote_scraper.cli --url "https://sfgov.legistar.com/Calendar.aspx" --jurisdiction "San Francisco" --politician "Stephen Sherrill" --body-filter "Board of Supervisors" --use-playwright-discovery --limit 300 --out stephen_sherrill_votes.csv
```
