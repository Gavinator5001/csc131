
## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

Example:

```bash
python -m civic_vote_scraper.cli --url "https://sfgov.legistar.com/Calendar.aspx" --jurisdiction "San Francisco" --body-filter "Board of Supervisors" --meeting-limit 10 --headless --out votes.csv
```
Takes a while to run just give it time

## Live minutes search

The scraper now keeps a SQLite database of discovered minutes files. On the first run it registers every discovered minutes file, downloads/cache-parses each unparsed file, stores the parsed vote rows, and writes the full vote CSV. On later runs it skips minutes that are already marked parsed and only downloads/parses newly discovered minutes.

One-time run:

```bash
python -m civic_vote_scraper.cli --url "https://sfgov.legistar.com/Calendar.aspx" --jurisdiction "San Francisco" --body-filter "Board of Supervisors" --meeting-limit 200 --headless --minutes-db minutes.db --out votes.csv
```

Live interval run:

```bash
python -m civic_vote_scraper.cli --url "https://sfgov.legistar.com/Calendar.aspx" --jurisdiction "San Francisco" --body-filter "Board of Supervisors" --meeting-limit 200 --headless --minutes-db minutes.db --live --live-interval-minutes 60 --out votes.csv
```

The desktop app exposes the same flow with:

- `Minutes database`: where discovered minutes and parsed vote rows are stored.
- `Search interval (minutes)`: how often live search checks for new minutes.
- `Start live search`: starts the repeating scrape.
- `Run once`: performs one database-backed scrape cycle.
- `Re-parse known minutes`: forces already parsed database records to be parsed again.

Run the PyQt5 desktop app:

```bash
python civic_vote_scraper_desktop_app_registry.py
```
