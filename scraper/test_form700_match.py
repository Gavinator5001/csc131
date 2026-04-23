import csv
from civic_vote_scraper.enrichment.form700_matcher import (
    parse_form700_entities,
    match_matters_to_investments,
    enrich_vote_rows_with_form700,
)

votes_path = "votes.csv"
form700_path = "form700_entities.csv"

with open(votes_path, newline="", encoding="utf-8") as f:
    votes = list(csv.DictReader(f))

entities = parse_form700_entities(form700_path)
matches = match_matters_to_investments(votes, entities, min_confidence=0.75)
enriched = enrich_vote_rows_with_form700(votes, form700_path, min_confidence=0.75)

print(f"entities: {len(entities)}")
print(f"matches: {len(matches)}")

for m in matches[:20]:
    print(m)

with open("test_votes_enriched.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=enriched[0].keys() if enriched else votes[0].keys())
    writer.writeheader()
    writer.writerows(enriched)