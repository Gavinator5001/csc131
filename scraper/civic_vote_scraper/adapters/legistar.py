from __future__ import annotations

import asyncio
from typing import List
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from civic_vote_scraper.adapters.base import BaseAdapter
from civic_vote_scraper.extractors.html_votes import extract_votes_from_html
from civic_vote_scraper.extractors.pdf_votes import extract_votes_from_pdf_bytes
from civic_vote_scraper.models import MeetingLink, MeetingRecord, VoteRecord
from civic_vote_scraper.utils.text import clean_whitespace, politician_name_variants


class LegistarAdapter(BaseAdapter):
    platform_name = "legistar"

    def discover_meetings(self, limit: int = 25) -> List[MeetingRecord]:
        host = urlparse(self.base_url).netloc.lower()
        if self.use_playwright_discovery or "sfgov.legistar.com" in host:
            try:
                meetings = asyncio.run(self._discover_meetings_playwright(limit=limit))
                if meetings:
                    return _dedupe_meetings(meetings[:limit])
            except Exception:
                pass

        html = self.http.get_text(self.base_url)
        soup = BeautifulSoup(html, "html.parser")
        meetings: List[MeetingRecord] = []

        for link in soup.select("a"):
            href = link.get("href") or ""
            text = clean_whitespace(link.get_text(" ", strip=True))
            if "MeetingDetail" not in href and "MeetingDetail.aspx" not in href:
                continue
            meetings.append(
                MeetingRecord(
                    jurisdiction=self.jurisdiction,
                    platform=self.platform_name,
                    body="",
                    meeting_title=text or "Meeting Detail",
                    meeting_date=None,
                    meeting_url=urljoin(self.base_url + "/", href),
                    links=[],
                )
            )
            if len(meetings) >= limit:
                break
        return _dedupe_meetings(meetings)

    async def _discover_meetings_playwright(self, limit: int = 25) -> List[MeetingRecord]:
        from playwright.async_api import async_playwright

        async def select_dropdown_item(page, dropdown_index: int, item_text: str):
            dropdown = page.locator("input.rcbInput").nth(dropdown_index)
            await dropdown.click()
            item = page.locator(f"li.rcbItem:has-text('{item_text}')").first
            await item.wait_for()
            await item.click()

        async def wait_for_page_number(page, page_num: int):
            await page.locator(
                f"tr.rgPager a.rgCurrentPage:has-text('{page_num}'), tr.rgPager .rgCurrentPage:has-text('{page_num}')"
            ).first.wait_for()

        meetings: List[MeetingRecord] = []
        seen = set()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.playwright_headless)
            page = await browser.new_page()
            await page.goto(self.base_url, wait_until="networkidle")
            await page.locator("#ctl00_ContentPlaceHolder1_gridCalendar").wait_for()

            await select_dropdown_item(page, 0, "All Years")
            if self.body_filter:
                await select_dropdown_item(page, 1, self.body_filter)

            async with page.expect_response(lambda response: "Calendar.aspx" in response.url):
                await page.locator("#ctl00_ContentPlaceHolder1_btnSearch").click()
            await page.locator("#ctl00_ContentPlaceHolder1_gridCalendar").wait_for()

            current_page = 1
            while True:
                grid_html = await page.locator("#ctl00_ContentPlaceHolder1_gridCalendar").inner_html()
                page_meetings = _parse_playwright_grid(
                    grid_html,
                    jurisdiction=self.jurisdiction,
                    platform=self.platform_name,
                    base_url=self.base_url,
                )
                for meeting in page_meetings:
                    key = meeting.meeting_url
                    if key in seen:
                        continue
                    seen.add(key)
                    meetings.append(meeting)
                if len(meetings) >= limit:
                    break

                next_page_num = current_page + 1
                next_page_link = page.locator(f"tr.rgPager a:has-text('{next_page_num}')").first
                if await next_page_link.count() == 0:
                    break
                async with page.expect_response(lambda response: "Calendar.aspx" in response.url):
                    await next_page_link.click()
                await wait_for_page_number(page, next_page_num)
                current_page = next_page_num

            await browser.close()
        return meetings

    def extract_votes(self, meeting: MeetingRecord, politician: str, html_only: bool = False) -> List[VoteRecord]:
        html = self.http.get_text(meeting.meeting_url)
        soup = BeautifulSoup(html, "html.parser")
        records = extract_votes_from_html(
            html,
            politician,
            jurisdiction=self.jurisdiction,
            platform=self.platform_name,
            body=meeting.body,
            meeting_title=meeting.meeting_title,
            meeting_date=meeting.meeting_date,
            source_url=meeting.meeting_url,
        )

        links = _collect_vote_bearing_links(meeting, soup)
        if html_only:
            return records

        if not _html_suggests_relevance(html, politician) and not records and not links:
            return records

        for link in links:
            try:
                if link.url.lower().endswith(".pdf") or link.kind == "pdf":
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


def _parse_playwright_grid(grid_html: str, *, jurisdiction: str, platform: str, base_url: str) -> List[MeetingRecord]:
    soup = BeautifulSoup(grid_html, "html.parser")
    meetings: List[MeetingRecord] = []
    for tr in soup.select("tr.rgRow, tr.rgAltRow"):
        tds = tr.find_all("td")
        if len(tds) < 8:
            continue
        body = clean_whitespace(tds[0].get_text(" ", strip=True))
        meeting_date = clean_whitespace(tds[1].get_text(" ", strip=True)) or None
        meeting_time = clean_whitespace(tds[3].get_text(" ", strip=True))
        location = clean_whitespace(tds[4].get_text(" ", strip=True))
        detail_a = tds[5].find("a", href=True)
        agenda_a = tds[6].find("a", href=True)
        minutes_a = tds[7].find("a", href=True)
        if not detail_a:
            continue
        meeting_url = urljoin(base_url + "/", detail_a["href"])
        links: List[MeetingLink] = []
        if agenda_a:
            links.append(MeetingLink(label="Agenda", url=urljoin(base_url + "/", agenda_a["href"]), kind="pdf"))
        if minutes_a:
            links.append(MeetingLink(label="Minutes", url=urljoin(base_url + "/", minutes_a["href"]), kind="pdf"))
        title = body
        if meeting_time:
            title = f"{body} {meeting_time}".strip()
        meetings.append(
            MeetingRecord(
                jurisdiction=jurisdiction,
                platform=platform,
                body=body,
                meeting_title=title,
                meeting_date=meeting_date,
                meeting_url=meeting_url,
                links=links,
            )
        )
    return meetings


def _collect_vote_bearing_links(meeting: MeetingRecord, soup: BeautifulSoup) -> List[MeetingLink]:
    links = list(meeting.links)
    for anchor in soup.select("a"):
        href = anchor.get("href") or ""
        text = clean_whitespace(anchor.get_text(" ", strip=True))
        label = text.lower()
        if not href:
            continue
        absolute = urljoin(meeting.meeting_url, href)
        lower_url = absolute.lower()
        if any(token in label for token in ["minutes", "summary minutes", "action minutes", "results", "votes"]):
            kind = "pdf" if lower_url.endswith(".pdf") or "view.ashx" in lower_url else "html"
            links.append(MeetingLink(label=text or href, url=absolute, kind=kind))
        elif "agenda packet" in label:
            links.append(MeetingLink(label=text or href, url=absolute, kind="pdf" if lower_url.endswith(".pdf") else "html"))
    return _dedupe_links(links)


def _html_suggests_relevance(html: str, politician: str) -> bool:
    html_lower = html.casefold()
    return any(variant.casefold() in html_lower for variant in politician_name_variants(politician))


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
    output = []
    for meeting in meetings:
        if meeting.meeting_url in seen:
            continue
        seen.add(meeting.meeting_url)
        output.append(meeting)
    return output
