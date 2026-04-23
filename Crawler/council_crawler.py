from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html
import http.client
import json
import mimetypes
import re
import tempfile
import time
from collections import deque
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen


DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_DELAY_SECONDS = 0.25
DEFAULT_MAX_PAGES_PER_SITE = 2000
DEFAULT_MAX_DOCUMENTS_PER_SITE = 2000
DEFAULT_MAX_AGE_YEARS = 5
DEFAULT_OUTPUT_ROOT = Path(tempfile.gettempdir()) / "council_crawler"

HTML_CONTENT_TYPES = ("text/html", "application/xhtml+xml")
DOCUMENT_EXTENSIONS = {
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".csv",
    ".txt",
    ".rtf",
}
SKIP_SCHEMES = ("mailto:", "tel:", "javascript:", "#")
SKIP_URL_KEYWORDS = ("cancelled",)


@dataclass(frozen=True)
class CrawlTarget:
    name: str
    start_urls: tuple[str, ...]
    allowed_hosts: tuple[str, ...]
    mode: str = "generic"
    api_base_url: str | None = None
    meeting_body_name: str | None = None
    browser_root_url: str | None = None


@dataclass
class ManifestEntry:
    site: str
    url: str
    kind: str
    status: str
    content_type: str | None = None
    local_path: str | None = None
    referrer: str | None = None
    depth: int | None = None
    error: str | None = None


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return

        attributes = dict(attrs)
        href = attributes.get("href")
        if href:
            self.links.append(href)


class CouncilCrawler:
    def __init__(
        self,
        output_root: Path,
        save_html: bool = False,
        max_pages_per_site: int = DEFAULT_MAX_PAGES_PER_SITE,
        max_documents_per_site: int = DEFAULT_MAX_DOCUMENTS_PER_SITE,
        max_age_years: int = DEFAULT_MAX_AGE_YEARS,
        delay_seconds: float = DEFAULT_DELAY_SECONDS,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        years: Iterable[int] | None = None,
    ) -> None:
        self.output_root = output_root
        self.save_html = save_html
        self.max_pages_per_site = max_pages_per_site
        self.max_documents_per_site = max_documents_per_site
        self.max_age_years = max_age_years
        self.min_year = dt.date.today().year - max_age_years + 1
        self.delay_seconds = delay_seconds
        self.timeout_seconds = timeout_seconds
        self.years = tuple(sorted({int(year) for year in years}, reverse=True)) if years else ()
        self.output_root.mkdir(parents=True, exist_ok=True)

    def crawl(self, target: CrawlTarget) -> dict[str, int | str]:
        site_root = self.output_root / self._safe_segment(target.name)
        site_root.mkdir(parents=True, exist_ok=True)

        manifest_path = site_root / "manifest.jsonl"
        existing_document_urls = self._load_existing_document_urls(manifest_path)
        if target.mode == "primegov":
            return self._crawl_primegov_target(
                target=target,
                manifest_path=manifest_path,
                existing_document_urls=existing_document_urls,
            )
        if target.mode == "legistar":
            return self._crawl_legistar_target(
                target=target,
                manifest_path=manifest_path,
                existing_document_urls=existing_document_urls,
            )
        seen_urls: set[str] = set()
        pages_visited = 0
        documents_saved = 0
        queue: deque[tuple[str, int, str | None]] = deque(
            (self._normalize_url(url), 0, None) for url in target.start_urls
        )

        with manifest_path.open("a", encoding="utf-8") as manifest_file:
            while queue:
                url, depth, referrer = queue.popleft()
                if url in seen_urls:
                    continue
                if self._should_skip_url(url):
                    continue
                if not self._is_recent_enough(url):
                    continue

                seen_urls.add(url)

                if self._is_document_url(url):
                    if url in existing_document_urls:
                        continue
                    if documents_saved >= self.max_documents_per_site:
                        continue
                    saved_count = self._fetch_document(
                        target=target,
                        url=url,
                        depth=depth,
                        referrer=referrer,
                        manifest_file=manifest_file,
                    )
                    documents_saved += saved_count
                    if saved_count:
                        existing_document_urls.add(url)
                    continue

                if pages_visited >= self.max_pages_per_site:
                    continue

                result = self._fetch_page(
                    target=target,
                    url=url,
                    depth=depth,
                    referrer=referrer,
                    manifest_file=manifest_file,
                )
                if result is None:
                    continue

                pages_visited += 1
                if pages_visited % 25 == 0:
                    print(
                        f"[{target.name}] pages_visited={pages_visited} "
                        f"documents_saved={documents_saved} queue_size={len(queue)}",
                        flush=True,
                    )
                content_type, html_text = result
                if not self._is_html_content_type(content_type):
                    continue

                extractor = LinkExtractor()
                extractor.feed(html_text)

                for link in extractor.links:
                    normalized = self._normalize_link(base_url=url, href=link)
                    if not normalized:
                        continue
                    if self._should_skip_url(normalized):
                        continue
                    if not self._is_allowed_url(normalized, target.allowed_hosts):
                        continue
                    if not self._is_recent_enough(normalized):
                        continue
                    if normalized in seen_urls:
                        continue
                    queue.append((normalized, depth + 1, url))

                time.sleep(self.delay_seconds)

        return {
            "site": target.name,
            "output_dir": str(site_root),
            "pages_visited": pages_visited,
            "documents_saved": documents_saved,
            "manifest_path": str(manifest_path),
            "previously_seen_documents": len(existing_document_urls) - documents_saved,
        }

    def _crawl_primegov_target(
        self,
        target: CrawlTarget,
        manifest_path: Path,
        existing_document_urls: set[str],
    ) -> dict[str, int | str]:
        pages_visited = 0
        documents_saved = 0
        meetings_processed = 0
        years_to_scrape = set(self.years) if self.years else None
        portal_url = target.browser_root_url or (target.start_urls[0] if target.start_urls else "")

        with manifest_path.open("a", encoding="utf-8") as manifest_file:
            self._write_manifest(
                manifest_file,
                ManifestEntry(
                    site=target.name,
                    url=portal_url,
                    kind="page",
                    status="fetched",
                    content_type="text/html",
                    depth=0,
                ),
            )

            discovered_meetings = self._fetch_primegov_meetings(target, years_to_scrape)

            if years_to_scrape:
                discovered_years = {int(meeting["year"]) for meeting in discovered_meetings}
                for year in sorted(years_to_scrape - discovered_years, reverse=True):
                    self._write_manifest(
                        manifest_file,
                        ManifestEntry(
                            site=target.name,
                            url=portal_url,
                            kind="page",
                            status="error",
                            depth=0,
                            error=f"PrimeGov API did not expose meetings for year: {year}",
                        ),
                    )

            for meeting in discovered_meetings:
                if pages_visited >= self.max_pages_per_site or documents_saved >= self.max_documents_per_site:
                    break

                meeting_url = str(meeting["meeting_url"])
                compiled_document_url = str(meeting["compiled_document_url"])

                page_result = self._fetch_page(
                    target=target,
                    url=meeting_url,
                    depth=1,
                    referrer=portal_url,
                    manifest_file=manifest_file,
                )
                if page_result is None:
                    continue

                pages_visited += 1
                meetings_processed += 1
                self._print_progress(target.name, pages_visited, documents_saved, 0)

                if compiled_document_url in existing_document_urls:
                    continue

                saved_count = self._fetch_document(
                    target=target,
                    url=compiled_document_url,
                    depth=2,
                    referrer=meeting_url,
                    manifest_file=manifest_file,
                )
                documents_saved += saved_count
                if saved_count:
                    existing_document_urls.add(compiled_document_url)

                time.sleep(self.delay_seconds)

        return {
            "site": target.name,
            "output_dir": str(manifest_path.parent),
            "pages_visited": pages_visited,
            "documents_saved": documents_saved,
            "manifest_path": str(manifest_path),
            "previously_seen_documents": len(existing_document_urls) - documents_saved,
            "meetings_processed": meetings_processed,
        }

    def _crawl_legistar_target(
        self,
        target: CrawlTarget,
        manifest_path: Path,
        existing_document_urls: set[str],
    ) -> dict[str, int | str]:
        pages_visited = 0
        documents_saved = 0
        meetings_processed = 0
        agenda_documents_found = 0
        current_year = dt.date.today().year
        years_to_scrape = self.years or tuple(range(current_year, self.min_year - 1, -1))

        with manifest_path.open("a", encoding="utf-8") as manifest_file:
            for year in years_to_scrape:
                try:
                    events = self._fetch_legistar_events(target, year)
                except (HTTPError, URLError, TimeoutError, ValueError, http.client.InvalidURL) as exc:
                    self._write_manifest(
                        manifest_file,
                        ManifestEntry(
                            site=target.name,
                            url=f"{target.api_base_url}/Events?year={year}",
                            kind="page",
                            status="error",
                            depth=0,
                            error=str(exc),
                        ),
                    )
                    continue

                for event in events:
                    if pages_visited >= self.max_pages_per_site:
                        break

                    meeting_url = self._normalize_url(event["EventInSiteURL"])
                    page_result = self._fetch_page(
                        target=target,
                        url=meeting_url,
                        depth=0,
                        referrer=target.start_urls[0] if target.start_urls else None,
                        manifest_file=manifest_file,
                    )
                    if page_result is None:
                        continue

                    pages_visited += 1
                    meetings_processed += 1
                    self._print_progress(target.name, pages_visited, documents_saved, 0)
                    agenda_url = self._normalize_url(str(event.get("EventAgendaFile") or "").strip()) if event.get("EventAgendaFile") else None
                    if not agenda_url:
                        continue

                    agenda_documents_found += 1
                    if agenda_url in existing_document_urls or documents_saved >= self.max_documents_per_site:
                        continue

                    saved_count = self._fetch_document(
                        target=target,
                        url=agenda_url,
                        depth=1,
                        referrer=meeting_url,
                        manifest_file=manifest_file,
                    )
                    documents_saved += saved_count
                    if saved_count:
                        existing_document_urls.add(agenda_url)

                    time.sleep(self.delay_seconds)

                if pages_visited >= self.max_pages_per_site or documents_saved >= self.max_documents_per_site:
                    break

        return {
            "site": target.name,
            "output_dir": str(manifest_path.parent),
            "pages_visited": pages_visited,
            "documents_saved": documents_saved,
            "manifest_path": str(manifest_path),
            "previously_seen_documents": len(existing_document_urls) - documents_saved,
            "meetings_processed": meetings_processed,
            "agenda_documents_found": agenda_documents_found,
        }

    def _fetch_page(
        self,
        target: CrawlTarget,
        url: str,
        depth: int,
        referrer: str | None,
        manifest_file,
    ) -> tuple[str | None, str] | None:
        try:
            response = self._open_url(url)
            body = response.read()
            content_type = response.headers.get_content_type()
            charset = response.headers.get_content_charset() or "utf-8"
            text = body.decode(charset, errors="replace")
            local_path = None

            if self.save_html:
                local_path = self._write_binary(
                    site_name=target.name,
                    url=url,
                    body=body,
                    content_type=content_type,
                )

            self._write_manifest(
                manifest_file,
                ManifestEntry(
                    site=target.name,
                    url=url,
                    kind="page",
                    status="saved" if local_path else "fetched",
                    content_type=content_type,
                    local_path=str(local_path) if local_path else None,
                    referrer=referrer,
                    depth=depth,
                ),
            )
            return content_type, text
        except (HTTPError, URLError, TimeoutError, ValueError, http.client.InvalidURL) as exc:
            self._write_manifest(
                manifest_file,
                ManifestEntry(
                    site=target.name,
                    url=url,
                    kind="page",
                    status="error",
                    referrer=referrer,
                    depth=depth,
                    error=str(exc),
                ),
            )
            return None

    def _fetch_document(
        self,
        target: CrawlTarget,
        url: str,
        depth: int,
        referrer: str | None,
        manifest_file,
    ) -> int:
        try:
            response = self._open_url(url)
            body = response.read()
            content_type = response.headers.get_content_type()
            if self._is_html_content_type(content_type):
                self._write_manifest(
                    manifest_file,
                    ManifestEntry(
                        site=target.name,
                        url=url,
                        kind="document",
                        status="skipped_html",
                        content_type=content_type,
                        referrer=referrer,
                        depth=depth,
                    ),
                )
                return 0
            local_path = self._write_binary(
                site_name=target.name,
                url=url,
                body=body,
                content_type=content_type,
            )
            self._write_manifest(
                manifest_file,
                ManifestEntry(
                    site=target.name,
                    url=url,
                    kind="document",
                    status="saved",
                    content_type=content_type,
                    local_path=str(local_path),
                    referrer=referrer,
                    depth=depth,
                ),
            )
            time.sleep(self.delay_seconds)
            return 1
        except (HTTPError, URLError, TimeoutError, ValueError, http.client.InvalidURL) as exc:
            self._write_manifest(
                manifest_file,
                ManifestEntry(
                    site=target.name,
                    url=url,
                    kind="document",
                    status="error",
                    referrer=referrer,
                    depth=depth,
                    error=str(exc),
                ),
            )
            return 0

    def _open_url(self, url: str):
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; CouncilCrawler/1.0; +https://example.invalid)",
                "Accept": "*/*",
            },
        )
        return urlopen(request, timeout=self.timeout_seconds)

    def _open_json_url(self, url: str):
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; CouncilCrawler/1.0; +https://example.invalid)",
                "Accept": "application/json",
            },
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))

    def _write_binary(self, site_name: str, url: str, body: bytes, content_type: str | None) -> Path:
        target_dir = self.output_root / self._safe_segment(site_name) / "downloads"
        target_dir.mkdir(parents=True, exist_ok=True)

        parsed = urlparse(url)
        candidate_name = self._filename_from_url(parsed)
        extension = Path(candidate_name).suffix
        if not extension:
            extension = mimetypes.guess_extension(content_type or "") or ".bin"

        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
        final_name = f"{Path(candidate_name).stem}_{digest}{extension}"
        output_path = target_dir / self._safe_filename(final_name)
        output_path.write_bytes(body)
        return output_path

    def _load_existing_document_urls(self, manifest_path: Path) -> set[str]:
        if not manifest_path.exists():
            return set()

        existing_urls: set[str] = set()
        with manifest_path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                url = entry.get("url")
                if entry.get("kind") == "document" and entry.get("status") == "saved" and url:
                    existing_urls.add(url)

        return existing_urls

    def _filename_from_url(self, parsed) -> str:
        query = parse_qs(parsed.query)
        for key in ("id", "file", "filename", "title"):
            values = query.get(key)
            if values:
                return self._safe_filename(unquote(values[0]))

        if parsed.path:
            basename = Path(unquote(parsed.path)).name
            if basename:
                return self._safe_filename(basename)

        return "download"

    def _is_document_url(self, url: str) -> bool:
        parsed = urlparse(url)
        path = parsed.path.lower()

        if any(path.endswith(extension) for extension in DOCUMENT_EXTENSIONS):
            return True

        if any(fragment in path for fragment in ("/document/", "/edoc/")):
            return True

        if "docview.aspx" in path:
            return True

        return False

    def _is_html_content_type(self, content_type: str | None) -> bool:
        if not content_type:
            return False
        return any(content_type.startswith(prefix) for prefix in HTML_CONTENT_TYPES)

    def _fetch_legistar_events(self, target: CrawlTarget, year: int) -> list[dict]:
        if not target.api_base_url or not target.meeting_body_name:
            return []

        start = f"{year}-01-01T00:00:00"
        end = f"{year + 1}-01-01T00:00:00"
        query = urlencode(
            {
                "$filter": (
                    f"EventBodyName eq '{target.meeting_body_name}' "
                    f"and EventDate ge datetime'{start}' "
                    f"and EventDate lt datetime'{end}'"
                ),
                "$orderby": "EventDate desc",
                "$top": "500",
            }
        )
        url = f"{target.api_base_url}/Events?{query}"
        response = self._open_json_url(url)
        return response if isinstance(response, list) else []

    def _fetch_legistar_attachment_links(self, target: CrawlTarget, matter_file: str | None) -> list[str]:
        if not target.api_base_url or not matter_file:
            return []

        query = urlencode(
            {
                "$filter": f"MatterFile eq '{matter_file}'",
                "$top": "2",
            }
        )
        matters_url = f"{target.api_base_url}/Matters?{query}"
        matters = self._open_json_url(matters_url)
        if not isinstance(matters, list) or not matters:
            return []

        matter_id = matters[0].get("MatterId")
        if not matter_id:
            return []

        attachments_url = f"{target.api_base_url}/Matters/{matter_id}/Attachments"
        attachments = self._open_json_url(attachments_url)
        if not isinstance(attachments, list):
            return []

        links: list[str] = []
        for attachment in attachments:
            hyperlink = attachment.get("MatterAttachmentHyperlink")
            if not hyperlink:
                continue
            links.append(self._normalize_url(hyperlink))
        return list(dict.fromkeys(links))

    def _extract_legistar_consent_items(self, base_url: str, html_text: str) -> list[tuple[str, str | None]]:
        items: list[tuple[str, str | None]] = []
        for row_match in re.finditer(r"<tr[^>]*>(.*?)</tr>", html_text, re.IGNORECASE | re.DOTALL):
            row_html = row_match.group(1)
            if "Consent Calendar Item" not in row_html:
                continue
            link_match = re.search(
                r'href="([^"]*LegislationDetail\.aspx[^"]*)"[^>]*>([^<]+)</a>',
                row_html,
                re.IGNORECASE,
            )
            if not link_match:
                continue
            absolute_url = self._normalize_url(urljoin(base_url, html.unescape(link_match.group(1))))
            matter_file = html.unescape(link_match.group(2)).strip() or None
            items.append((absolute_url, matter_file))
        return list(dict.fromkeys(items))

    def _extract_legistar_attachment_links(self, base_url: str, html_text: str) -> list[str]:
        attachment_section_match = re.search(
            r'<table[^>]+id="ctl00_ContentPlaceHolder1_tblAttachments"[^>]*>(.*?)</table>',
            html_text,
            re.IGNORECASE | re.DOTALL,
        )
        if not attachment_section_match:
            return []

        links: list[str] = []
        for href in re.findall(r'href="([^"]+)"', attachment_section_match.group(1), re.IGNORECASE):
            absolute_url = self._normalize_url(urljoin(base_url, html.unescape(href)))
            links.append(absolute_url)
        return list(dict.fromkeys(links))

    def _fetch_primegov_meetings(
        self,
        target: CrawlTarget,
        years_to_scrape: set[int] | None,
    ) -> list[dict[str, str | int]]:
        if not target.api_base_url:
            return []

        meetings: dict[str, dict[str, str | int]] = {}
        desired_years = set(years_to_scrape or ())

        if desired_years:
            years = sorted(desired_years, reverse=True)
        else:
            archived_years = self._open_json_url(f"{target.api_base_url}/GetArchivedMeetingYears")
            years = [int(year) for year in archived_years if int(year) >= self.min_year] if isinstance(archived_years, list) else []

        for year in years:
            archived = self._open_json_url(f"{target.api_base_url}/ListArchivedMeetings?year={year}")
            if not isinstance(archived, list):
                continue
            for meeting in archived:
                normalized = self._normalize_primegov_meeting(meeting)
                if not normalized:
                    continue
                meetings[str(normalized["meeting_template_id"])] = normalized

        upcoming = self._open_json_url(f"{target.api_base_url}/ListUpcomingMeetings")
        if isinstance(upcoming, list):
            for meeting in upcoming:
                normalized = self._normalize_primegov_meeting(meeting)
                if not normalized:
                    continue
                year = int(normalized["year"])
                if desired_years and year not in desired_years:
                    continue
                if not desired_years and year < self.min_year:
                    continue
                meetings[str(normalized["meeting_template_id"])] = normalized

        return sorted(
            meetings.values(),
            key=lambda meeting: (int(meeting["year"]), str(meeting["label"])),
            reverse=True,
        )

    def _normalize_primegov_meeting(self, meeting: dict) -> dict[str, str | int] | None:
        title = self._strip_control_characters(str(meeting.get("title") or "")).strip()
        date_text = self._strip_control_characters(str(meeting.get("date") or meeting.get("dateTime") or "")).strip()
        years = self._extract_years(date_text)
        if not years:
            return None

        document_list = meeting.get("documentList")
        if not isinstance(document_list, list):
            return None

        compiled_documents = [
            document for document in document_list if int(document.get("compileOutputType") or 0) == 1 and document.get("templateId")
        ]
        if not compiled_documents:
            return None

        preferred_document = next(
            (
                document
                for document in compiled_documents
                if re.search(r"agenda|notice", str(document.get("templateName") or ""), re.IGNORECASE)
            ),
            compiled_documents[0],
        )

        meeting_template_id = str(preferred_document["templateId"])
        compiled_output_type = int(preferred_document.get("compileOutputType") or 1)
        meeting_url = self._normalize_url(
            f"https://santa-ana.primegov.com/Portal/Meeting?meetingTemplateId={meeting_template_id}"
        )
        compiled_document_url = self._normalize_url(
            f"https://santa-ana.primegov.com/Public/CompiledDocument?meetingTemplateId={meeting_template_id}&compileOutputType={compiled_output_type}"
        )
        label = " ".join(part for part in (title, date_text) if part).strip() or meeting_template_id

        return {
            "meeting_template_id": meeting_template_id,
            "meeting_url": meeting_url,
            "compiled_document_url": compiled_document_url,
            "label": label,
            "year": max(years),
        }

    def _normalize_link(self, base_url: str, href: str) -> str | None:
        href = self._strip_control_characters(href).strip()
        if not href or href.lower().startswith(SKIP_SCHEMES):
            return None
        return self._normalize_url(urljoin(base_url, href))

    def _normalize_url(self, url: str) -> str:
        url = self._strip_control_characters(url).strip()
        parsed = urlparse(url)
        cleaned_path = re.sub(r"/+", "/", parsed.path or "/")
        encoded_path = quote(cleaned_path, safe="/%:@!$&'()*+,;=-._~")
        encoded_query = quote(parsed.query, safe="=&%:@!$'()*+,;/?-._~")
        normalized = parsed._replace(fragment="", path=encoded_path, query=encoded_query)
        return normalized.geturl()

    def _strip_control_characters(self, value: str) -> str:
        return "".join(
            character
            for character in value
            if character.isprintable() and character not in {"\u200e", "\u200f", "\u202a", "\u202b", "\u202c"}
        )

    def _print_progress(self, site_name: str, pages_visited: int, documents_saved: int, queue_size: int) -> None:
        if pages_visited % 25 != 0:
            return
        print(
            f"[{site_name}] pages_visited={pages_visited} "
            f"documents_saved={documents_saved} queue_size={queue_size}",
            flush=True,
        )

    def _is_allowed_url(self, url: str, allowed_hosts: Iterable[str]) -> bool:
        host = (urlparse(url).netloc or "").lower()
        return host in {allowed_host.lower() for allowed_host in allowed_hosts}

    def _should_skip_url(self, url: str) -> bool:
        lowered = url.lower()
        return any(keyword in lowered for keyword in SKIP_URL_KEYWORDS)

    def _is_recent_enough(self, url: str) -> bool:
        years = self._extract_years(url)
        if not years:
            return True
        return max(years) >= self.min_year

    def _extract_years(self, value: str) -> list[int]:
        years: list[int] = []
        for match in re.finditer(r"(?<!\d)(20\d{2})(?!\d)", value):
            year = int(match.group(1))
            if 2000 <= year <= dt.date.today().year + 1:
                years.append(year)
        return years

    def _safe_segment(self, value: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
        return cleaned.strip("._") or "site"

    def _safe_filename(self, value: str) -> str:
        cleaned = re.sub(r'[<>:"/\\\\|?*]+', "_", value.strip())
        return cleaned[:180] or "download.bin"

    def _write_manifest(self, manifest_file, entry: ManifestEntry) -> None:
        manifest_file.write(json.dumps(asdict(entry), ensure_ascii=True) + "\n")
        manifest_file.flush()


TARGETS = {
    "santa_ana": CrawlTarget(
        name="santa_ana",
        start_urls=(
            "https://www.santa-ana.org/agendas-and-minutes/",
            "https://santa-ana.primegov.com/public/portal",
        ),
        allowed_hosts=(
            "www.santa-ana.org",
            "santa-ana.primegov.com",
            "pgwest.blob.core.windows.net",
        ),
        mode="primegov",
        api_base_url="https://santa-ana.primegov.com/api/v2/PublicPortal",
        browser_root_url="https://santa-ana.primegov.com/public/portal",
    ),
    "sonoma": CrawlTarget(
        name="sonoma",
        start_urls=(
            "https://sonoma-county.legistar.com/Calendar.aspx",
        ),
        allowed_hosts=(
            "sonoma-county.legistar.com",
            "sonoma-county.legistar1.com",
            "webapi.legistar.com",
        ),
        mode="legistar",
        api_base_url="https://webapi.legistar.com/v1/sonoma-county",
        meeting_body_name="Board of Supervisors",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl council and county meeting portals and store fetched documents in a temp folder."
    )
    parser.add_argument(
        "--sites",
        nargs="+",
        choices=sorted(TARGETS.keys()),
        default=sorted(TARGETS.keys()),
        help="Which site adapters to run.",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help=f"Directory for downloads and manifests. Default: {DEFAULT_OUTPUT_ROOT}",
    )
    parser.add_argument(
        "--save-html",
        action="store_true",
        help="Persist HTML pages in addition to document files.",
    )
    parser.add_argument(
        "--max-pages-per-site",
        type=int,
        default=DEFAULT_MAX_PAGES_PER_SITE,
        help="Maximum number of HTML pages to visit per site.",
    )
    parser.add_argument(
        "--max-documents-per-site",
        type=int,
        default=DEFAULT_MAX_DOCUMENTS_PER_SITE,
        help="Maximum number of documents to save per site.",
    )
    parser.add_argument(
        "--max-age-years",
        type=int,
        default=DEFAULT_MAX_AGE_YEARS,
        help="Only crawl records from this many years back, based on years found in URLs. Default: 5",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=None,
        help="Optional explicit years to scrape for Legistar-backed sites, for example: --years 2026 2025",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help="Sleep interval between requests.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Per-request timeout.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    crawler = CouncilCrawler(
        output_root=Path(args.output_root),
        save_html=args.save_html,
        max_pages_per_site=args.max_pages_per_site,
        max_documents_per_site=args.max_documents_per_site,
        max_age_years=args.max_age_years,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
        years=args.years,
    )

    summaries = []
    for site_name in args.sites:
        summary = crawler.crawl(TARGETS[site_name])
        summaries.append(summary)

    print(json.dumps(summaries, indent=2))


if __name__ == "__main__":
    main()
