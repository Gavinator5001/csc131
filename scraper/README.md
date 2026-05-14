
## Install

```bash
python -m venv .venv
source .venv/scripts/activate
pip install -r requirements.txt
python -m playwright install chromium
```

## Live minutes and Form 700 sync

The scraper now keeps a single SQLite database for:

- discovered minutes files
- parsed vote rows
- FPPC Form 700 filings for the selected jurisdiction
- parsed Form 700 entity rows extracted from downloaded PDFs

On each run it:

1. searches the FPPC Form 700 portal for the current jurisdiction
2. records every discovered filing in SQLite
3. downloads only Form 700 PDFs that are not already in the database
4. parses those PDFs into entity rows used for vote matching
5. discovers minutes, downloads only new minutes files, parses vote rows, and writes the outputs

One-time run:

```bash
python -m civic_vote_scraper.cli --url "https://sonoma-county.legistar.com/Calendar.aspx" --jurisdiction "County of Sonoma" --body-filter "Board of Supervisors" --meeting-limit 200 --headless --minutes-db minutes.db --form700-folder form700 --out votes.csv
```

Live interval run:

```bash
python -m civic_vote_scraper.cli --url "https://sonoma-county.legistar.com/Calendar.aspx" --jurisdiction "County of Sonoma" --body-filter "Board of Supervisors" --meeting-limit 200 --headless --minutes-db minutes.db --form700-folder form700 --live --live-interval-minutes 60 --out votes.csv
```

Useful options:

```bash
--form700-search-url "https://form700search.fppc.ca.gov/Search/SearchFilerForms.aspx"
--reparse-existing-form700s
--reparse-existing-minutes
--skip-form700-sync
```

Form 700 outputs are written from the database-backed PDF parse:

- `form700_entities.csv`
- `form700_entities.json`
- `form700_matches.csv`
- `form700_matches.json`

The desktop app exposes the same flow with:

- `Minutes database`: where discovered minutes and parsed vote rows are stored.
- `Form 700 search URL`: the FPPC portal entrypoint.
- `Form 700 folder`: where downloaded filing PDFs are stored.
- `Search interval (minutes)`: how often live search checks for new minutes.
- `Start live search`: starts the repeating scrape.
- `Run once`: performs one database-backed scrape cycle.
- `Re-parse known minutes`: forces already parsed database records to be parsed again.
- `Re-parse known Form 700 PDFs`: re-runs PDF parsing for saved filings.

## (RECOMMENDED)  
Run the PyQt5 desktop app:

```bash
python civic_vote_scraper_desktop_app_registry.py
```
