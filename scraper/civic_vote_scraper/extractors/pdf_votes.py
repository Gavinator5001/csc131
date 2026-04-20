from __future__ import annotations

import io
from typing import List

import pdfplumber

from civic_vote_scraper.models import VoteRecord
from civic_vote_scraper.utils.text import clean_whitespace, contains_name, likely_vote_line, normalize_vote_label, split_candidate_lines


def extract_votes_from_pdf_bytes(
    pdf_bytes: bytes,
    politician: str,
    *,
    jurisdiction: str,
    platform: str,
    body: str,
    meeting_title: str,
    meeting_date: str | None,
    source_url: str,
) -> List[VoteRecord]:
    text_chunks: List[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            if page_text:
                text_chunks.append(page_text)

    results: List[VoteRecord] = []
    for line in split_candidate_lines("\n".join(text_chunks)):
        if not likely_vote_line(line) or not contains_name(line, politician):
            continue
        lowered = line.lower()
        vote = "Unknown"
        for marker in ["aye", "yes", "no", "nay", "abstain", "absent", "recused"]:
            if marker in lowered:
                vote = normalize_vote_label(marker)
                break
        results.append(
            VoteRecord(
                jurisdiction=jurisdiction,
                platform=platform,
                body=body,
                meeting_date=meeting_date,
                meeting_title=meeting_title,
                item_number="",
                matter_id="",
                matter_title="",
                motion_text="",
                result="",
                member_name=politician,
                vote=vote,
                source_url=source_url,
                source_type="pdf",
                confidence=0.40,
                snippet=clean_whitespace(line),
            )
        )
    return results
