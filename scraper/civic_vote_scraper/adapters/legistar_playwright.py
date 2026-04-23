from __future__ import annotations

import asyncio
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from civic_vote_scraper.models import MeetingLink, MeetingRecord


def normalize_header(text: str) -> str:
    return " ".join((text or "").replace("\xa0", " ").split()).strip().lower()


def build_column_index(table_soup):
    for row in table_soup.select("thead tr"):
        cells = row.find_all(["th", "td"])
        names = [normalize_header(c.get_text(" ", strip=True)) for c in cells]
        if "minutes" in names and "meeting details" in names:
            return {name: idx for idx, name in enumerate(names) if name}
    return {}


def cell_text(tds, idx):
    if idx is None or idx >= len(tds):
        return ""
    return tds[idx].get_text(" ", strip=True)


def cell_link(tds, idx):
    if idx is None or idx >= len(tds):
        return None
    return tds[idx].find("a", href=True)


def parse_rows(grid_html: str, base_url: str):
    soup = BeautifulSoup(grid_html, "html.parser")
    rows = []

    table = soup.select_one("table")
    if table is None:
        return rows

    col_idx = build_column_index(table)

    body_idx = col_idx.get("name")
    date_idx = col_idx.get("meeting date")
    time_idx = col_idx.get("meeting time")
    location_idx = col_idx.get("meeting location")
    meeting_details_idx = col_idx.get("meeting details")
    agenda_idx = col_idx.get("agenda")
    minutes_idx = col_idx.get("minutes")

    for tr in soup.select("tr.rgRow, tr.rgAltRow"):
        tds = tr.find_all("td")
        if not tds:
            continue

        body = cell_text(tds, body_idx)
        meeting_date = cell_text(tds, date_idx)
        meeting_time = cell_text(tds, time_idx)
        location = cell_text(tds, location_idx)

        meeting_a = cell_link(tds, meeting_details_idx)
        agenda_a = cell_link(tds, agenda_idx)
        minutes_a = cell_link(tds, minutes_idx)

        rows.append(
            {
                "body": body,
                "meeting_date": meeting_date,
                "meeting_time": meeting_time,
                "location": location,
                "meeting_url": urljoin(base_url, meeting_a["href"]) if meeting_a else None,
                "agenda_url": urljoin(base_url, agenda_a["href"]) if agenda_a else None,
                "minutes_url": urljoin(base_url, minutes_a["href"]) if minutes_a else None,
            }
        )
    return rows


async def select_dropdown_item(page, dropdown_index: int, item_text: str):
    print(f"[info] opening dropdown {dropdown_index}")
    dropdown = page.locator("input.rcbInput").nth(dropdown_index)
    await dropdown.click()
    item = page.locator(f"li.rcbItem:has-text('{item_text}')").first
    await item.wait_for()
    await item.click()
    print(f"[info] selected '{item_text}'")


async def wait_for_page_change(page, previous_html: str, page_num: int):
    print(f"[info] waiting for page {page_num} to load")
    grid = page.locator("#ctl00_ContentPlaceHolder1_gridCalendar")

    for _ in range(60):
        await page.wait_for_timeout(500)
        current_html = await grid.inner_html()
        if current_html != previous_html:
            print(f"[info] page {page_num} loaded")
            return

    raise TimeoutError(f"calendar grid did not change while waiting for page {page_num}")


class LegistarPlaywrightDiscovery:
    def __init__(self, url: str, jurisdiction: str, body_filter: str = "", headless: bool = True):
        self.url = url
        self.jurisdiction = jurisdiction
        self.headless = headless

    async def _discover_async(self, max_pages: int = 0, meeting_limit: int = 0):
        all_meetings = []
        seen = set()

        print("[info] starting Playwright")
        async with async_playwright() as p:
            print("[info] launching browser")
            browser = await p.chromium.launch(headless=self.headless)
            page = await browser.new_page()

            print(f"[info] site loading: {self.url}")
            await page.goto(self.url, wait_until="networkidle")
            print("[info] site loaded")

            print("[info] waiting for calendar grid")
            await page.locator("#ctl00_ContentPlaceHolder1_gridCalendar").wait_for()
            print("[info] calendar grid found")

            print("[info] applying year filter")
            await select_dropdown_item(page, 0, "All Years")


            print("[info] clicking Search Calendar")
            await page.locator("#ctl00_ContentPlaceHolder1_btnSearch").click()
            print("[info] waiting for filtered grid")
            await page.locator("#ctl00_ContentPlaceHolder1_gridCalendar").wait_for()
            print("[info] filtered grid ready")

            current_page = 1
            while True:
                print(f"[info] scraping calendar page {current_page}")
                grid_html = await page.locator("#ctl00_ContentPlaceHolder1_gridCalendar").inner_html()
                page_rows = parse_rows(grid_html, self.url)
                print(f"[info] parsed {len(page_rows)} rows from page {current_page}")

                new_on_page = 0
                for row in page_rows:
                    key = row["meeting_url"] or f'{row["meeting_date"]}|{row["body"]}|{row["meeting_time"]}'
                    if key in seen:
                        continue
                    seen.add(key)
                    new_on_page += 1

                    links = []
                    if row.get("agenda_url"):
                        links.append(MeetingLink(label="Agenda", url=row["agenda_url"], kind="pdf"))
                    if row.get("minutes_url"):
                        links.append(MeetingLink(label="Minutes", url=row["minutes_url"], kind="pdf"))

                    all_meetings.append(
                        MeetingRecord(
                            jurisdiction=self.jurisdiction,
                            platform="legistar-playwright",
                            body=row.get("body") or "",
                            meeting_title=f'{row.get("body") or ""} {row.get("meeting_date") or ""}'.strip(),
                            meeting_date=row.get("meeting_date"),
                            meeting_url=row.get("meeting_url") or "",
                            links=links,
                        )
                    )

                    if meeting_limit and len(all_meetings) >= meeting_limit:
                        print(f"[info] meeting limit reached: {meeting_limit}")
                        await browser.close()
                        print("[info] browser closed")
                        return all_meetings

                print(f"[info] added {new_on_page} new meetings from page {current_page}")
                print(f"[info] total meetings discovered so far: {len(all_meetings)}")

                if max_pages and current_page >= max_pages:
                    print(f"[info] page limit reached: {max_pages}")
                    break

                next_page_num = current_page + 1
                next_page_link = page.locator(f"tr.rgPager a:has-text('{next_page_num}')").first
                if await next_page_link.count() == 0:
                    print("[info] no more calendar pages found")
                    break

                print(f"[info] moving to page {next_page_num}")
                previous_html = await page.locator("#ctl00_ContentPlaceHolder1_gridCalendar").inner_html()
                await next_page_link.scroll_into_view_if_needed()
                await next_page_link.click(force=True)
                await wait_for_page_change(page, previous_html, next_page_num)
                current_page = next_page_num

            await browser.close()
            print("[info] browser closed")

        print(f"[info] discovery complete: {len(all_meetings)} meetings")
        return all_meetings

    def discover_meetings(self, max_pages: int = 0, meeting_limit: int = 0):
        return asyncio.run(self._discover_async(max_pages=max_pages, meeting_limit=meeting_limit))
