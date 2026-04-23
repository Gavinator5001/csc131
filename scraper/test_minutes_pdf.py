from pathlib import Path
from pypdf import PdfReader

from civic_vote_scraper.vote_extract import (
    extract_vote_rows_from_minutes_text,
    _politician_name_patterns,
    _matches_politician_name,
)

pdf_path = Path("Minutes-8.pdf")
politician = "David Rabbitt"

reader = PdfReader(str(pdf_path))
text = "\n".join(page.extract_text() or "" for page in reader.pages)

rows = extract_vote_rows_from_minutes_text(
    text,
    meeting_date="2025-08-26",
    body="Board of Supervisors",
    minutes_url=str(pdf_path),
)

full_pat, last_pat = _politician_name_patterns(politician)
matched = [
    row for row in rows
    if _matches_politician_name(row.get("politician_name", ""), full_pat, last_pat)
]

print(f"all parsed vote rows: {len(rows)}")
print(f"matched rows for {politician}: {len(matched)}")

for row in matched[:20]:
    print(row)