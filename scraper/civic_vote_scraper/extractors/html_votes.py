from __future__ import annotations

from typing import Iterable, List

from bs4 import BeautifulSoup

from civic_vote_scraper.models import VoteRecord
from civic_vote_scraper.utils.text import clean_whitespace, contains_name, likely_vote_line, normalize_vote_label


def extract_votes_from_html(
    html: str,
    politician: str,
    *,
    jurisdiction: str,
    platform: str,
    body: str,
    meeting_title: str,
    meeting_date: str | None,
    source_url: str,
) -> List[VoteRecord]:
    soup = BeautifulSoup(html, "html.parser")
    text_lines = [clean_whitespace(x.get_text(" ", strip=True)) for x in soup.find_all(["tr", "p", "li", "div"])]
    return _parse_lines(
        text_lines,
        politician=politician,
        jurisdiction=jurisdiction,
        platform=platform,
        body=body,
        meeting_title=meeting_title,
        meeting_date=meeting_date,
        source_url=source_url,
        source_type="html",
    )


def _parse_lines(
    lines: Iterable[str],
    *,
    politician: str,
    jurisdiction: str,
    platform: str,
    body: str,
    meeting_title: str,
    meeting_date: str | None,
    source_url: str,
    source_type: str,
) -> List[VoteRecord]:
    results: List[VoteRecord] = []
    for line in lines:
        if not line or not likely_vote_line(line) or not contains_name(line, politician):
            continue
        lowered = line.lower()
        detected_vote = ""
        for marker in ["aye", "yes", "no", "nay", "abstain", "absent", "recused"]:
            if marker in lowered:
                detected_vote = normalize_vote_label(marker)
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
                vote=detected_vote or "Unknown",
                source_url=source_url,
                source_type=source_type,
                confidence=0.45,
                snippet=line,
            )
        )
    return results
