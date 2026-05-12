import csv
from civic_vote_scraper.enrichment.form700_matcher import (
    enrich_vote_rows_with_form700_rows,
    match_vote_rows_against_form700_rows,
)

votes_path = "votes.csv"
form700_path = "form700_entities.csv"

with open(votes_path, newline="", encoding="utf-8") as f:
    votes = list(csv.DictReader(f))

with open(form700_path, newline="", encoding="utf-8") as f:
    form700_rows = list(csv.DictReader(f))

matches = match_vote_rows_against_form700_rows(votes, form700_rows, min_confidence=0.75)
enriched = enrich_vote_rows_with_form700_rows(votes, form700_rows, min_confidence=0.75)

print(f"form700 rows: {len(form700_rows)}")
print(f"matches: {len(matches)}")

for m in matches[:20]:
    print(m)

with open("test_votes_enriched.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=enriched[0].keys() if enriched else votes[0].keys())
    writer.writeheader()
    writer.writerows(enriched)
