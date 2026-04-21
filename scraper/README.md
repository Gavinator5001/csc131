
## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

Example:

```bash
python -m civic_vote_scraper.cli --url "https://sfgov.legistar.com/Calendar.aspx" --jurisdiction "San Francisco" --politician "Stephen Sherrill" --body-filter "Board of Supervisors" --use-playwright-discovery --limit 10 --out stephen_sherrill_votes.csv
```
Takes a while to run just give it time