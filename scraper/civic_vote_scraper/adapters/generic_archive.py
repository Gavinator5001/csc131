from __future__ import annotations

from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from civic_vote_scraper.adapters.base import BaseAdapter
from civic_vote_scraper.extractors.html_votes import extract_votes_from_html
from civic_vote_scraper.extractors.pdf_votes import extract_votes_from_pdf_bytes
from civic_vote_scraper.models import MeetingLink, MeetingRecord, VoteRecord
from civic_vote_scraper.utils.text import clean_whitespace, contains_name


class GenericArchiveAdapter(BaseAdapter):
    platform_name = "generic"

    def discover_meetings(self, limit: int = 25) -> List[MeetingRecord]:
        html = self.http.get_text(self.base_url)
        soup = BeautifulSoup(html, "html.parser")
        meetings: List[MeetingRecord] = []
        for anchor in soup.select("a"):
            href = anchor.get("href") or ""
            label = clean_whitespace(anchor.get_text(" ", strip=True))
            if not href:
                continue
            absolute = urljoin(self.base_url + "/", href)
            lower = absolute.lower()
            if not any(token in lower for token in ["meeting", "agenda", "minutes", ".pdf"]):
                continue
            meetings.append(
                MeetingRecord(
                    jurisdiction=self.jurisdiction,
                    platform=self.platform_name,
                    body="",
                    meeting_title=label or absolute.rsplit("/", 1)[-1],
                    meeting_date=None,
                    meeting_url=absolute,
                    links=[MeetingLink(label=label or "source", url=absolute, kind="pdf" if lower.endswith(".pdf") else "html")],
                )
            )
            if len(meetings) >= limit:
                break
        return _dedupe_meetings(meetings)

    def extract_votes(self, meeting: MeetingRecord, politician: str, html_only: bool = False) -> List[VoteRecord]:
        url = meeting.meeting_url
        if url.lower().endswith(".pdf"):
            return extract_votes_from_pdf_bytes(
                self.http.get_bytes(url),
                politician,
                jurisdiction=self.jurisdiction,
                platform=self.platform_name,
                body=meeting.body,
                meeting_title=meeting.meeting_title,
                meeting_date=meeting.meeting_date,
                source_url=url,
            )
        html = self.http.get_text(url)
        records = extract_votes_from_html(
            html,
            politician,
            jurisdiction=self.jurisdiction,
            platform=self.platform_name,
            body=meeting.body,
            meeting_title=meeting.meeting_title,
            meeting_date=meeting.meeting_date,
            source_url=url,
        )
        if html_only or records or not contains_name(html, politician):
            return records

        soup = BeautifulSoup(html, "html.parser")
        links = _collect_document_links(url, soup)
        for link in links:
            try:
                if link.kind == "pdf":
                    records.extend(
                        extract_votes_from_pdf_bytes(
                            self.http.get_bytes(link.url),
                            politician,
                            jurisdiction=self.jurisdiction,
                            platform=self.platform_name,
                            body=meeting.body,
                            meeting_title=meeting.meeting_title,
                            meeting_date=meeting.meeting_date,
                            source_url=link.url,
                        )
                    )
                else:
                    records.extend(
                        extract_votes_from_html(
                            self.http.get_text(link.url),
                            politician,
                            jurisdiction=self.jurisdiction,
                            platform=self.platform_name,
                            body=meeting.body,
                            meeting_title=meeting.meeting_title,
                            meeting_date=meeting.meeting_date,
                            source_url=link.url,
                        )
                    )
            except Exception:
                continue
        return records


def _collect_document_links(base_url: str, soup: BeautifulSoup) -> List[MeetingLink]:
    links: List[MeetingLink] = []
    for anchor in soup.select("a"):
        href = anchor.get("href") or ""
        label = clean_whitespace(anchor.get_text(" ", strip=True))
        if not href:
            continue
        absolute = urljoin(base_url, href)
        lower = absolute.lower()
        lower_label = label.lower()
        if any(token in lower_label for token in ["minutes", "summary minutes", "action minutes", "results", "vote"]):
            links.append(MeetingLink(label=label or href, url=absolute, kind="pdf" if lower.endswith(".pdf") else "html"))
    return _dedupe_links(links)


def _dedupe_links(links: List[MeetingLink]) -> List[MeetingLink]:
    seen = set()
    output: List[MeetingLink] = []
    for link in links:
        if link.url in seen:
            continue
        seen.add(link.url)
        output.append(link)
    return output


def _dedupe_meetings(meetings: List[MeetingRecord]) -> List[MeetingRecord]:
    seen = set()
    output: List[MeetingRecord] = []
    for meeting in meetings:
        if meeting.meeting_url in seen:
            continue
        seen.add(meeting.meeting_url)
        output.append(meeting)
    return output
