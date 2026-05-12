from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import HTTPCookieProcessor, Request, build_opener
import http.cookiejar

from civic_vote_scraper.form700_parser import extract_form700_metadata_from_pdf, parse_form700_pdf
from civic_vote_scraper.minutes_db import MinutesDatabase


CANONICAL_FORM700_SEARCH_URL = "https://form700search.fppc.ca.gov/"
FORM700_SEARCH_ENDPOINT = "https://form700search.fppc.ca.gov/Home/SearchDocuments"
FORM700_DOWNLOAD_ENDPOINT = "https://form700search.fppc.ca.gov/Home/GetRedactedFormPdf"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
AJAX_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Content-Type": "application/json; charset=UTF-8",
    "Origin": "https://form700search.fppc.ca.gov",
    "Referer": CANONICAL_FORM700_SEARCH_URL,
}


class Form700JurisdictionNotFound(RuntimeError):
    pass


@dataclass
class HttpResponse:
    url: str
    status: int
    content_type: str
    text: str

    @property
    def snippet(self) -> str:
        return _norm((self.text or "")[:400])


@dataclass
class BinaryResponse:
    url: str
    status: int
    content_type: str
    body: bytes

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="ignore")

    @property
    def snippet(self) -> str:
        return _norm(self.text[:400])


@dataclass
class DownloadCandidate:
    url: str
    description: str
    context: str
    index_id: str = ""
    alternate_urls: tuple[str, ...] = ()
    filing_metadata: dict = field(default_factory=dict)
    download_request: dict = field(default_factory=dict)

    @property
    def key(self) -> str:
        return _norm_key(self.index_id or self.url or self.description or self.context)


class _AnchorCollector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._current_href = ""
        self._parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        attrs_map = dict(attrs)
        if tag.lower() == "a":
            self._current_href = attrs_map.get("href", "")
            self._parts = []

    def handle_data(self, data):
        if self._current_href:
            self._parts.append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._current_href:
            text = _norm(" ".join(self._parts))
            self.links.append((self._current_href, text))
            self._current_href = ""
            self._parts = []


def _norm(text: str) -> str:
    return " ".join((text or "").replace("\xa0", " ").split()).strip()


def _norm_key(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", _norm(text).lower())).strip()


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", _norm(text).lower()).strip("_")
    return cleaned[:80] or "form700"


def _title_case_words(*parts: str) -> str:
    return _norm(" ".join(part for part in parts if _norm(part)))


def _format_portal_date(value) -> str:
    text = _norm(str(value or ""))
    if not text:
        return ""
    match = re.match(r"^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2}):(\d{2})(?::(\d{2}))?)?", text)
    if not match:
        return text
    year, month, day, hour, minute, _second = match.groups()
    if hour is None or minute is None:
        return f"{int(month):02d}/{int(day):02d}/{year}"
    hour_int = int(hour)
    suffix = "AM" if hour_int < 12 else "PM"
    hour_12 = hour_int % 12 or 12
    return f"{int(month):02d}/{int(day):02d}/{year} {hour_12:02d}:{int(minute):02d} {suffix}"


def _normalize_form700_search_url(url: str) -> str:
    parsed = urlparse((url or "").strip())
    host = (parsed.netloc or "").lower()
    if host != "form700search.fppc.ca.gov":
        return CANONICAL_FORM700_SEARCH_URL
    return CANONICAL_FORM700_SEARCH_URL


def _build_form700_cache_key(filing: dict) -> str:
    raw = "|".join(
        [
            filing.get("jurisdiction", "").lower(),
            filing.get("download_form_url", "").lower(),
            filing.get("form_description", "").lower(),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


def _build_temp_download_filename(filing: dict) -> str:
    stem = filing.get("form700_cache_key", "") or hashlib.sha1(
        filing.get("download_form_url", "").encode("utf-8")
    ).hexdigest()[:24]
    return f"tmp_{_slug(stem)}.pdf"


def _filename_date_token(pdf_metadata: dict) -> str:
    for key in ["filed_date", "due_date", "filing_year"]:
        value = _norm(pdf_metadata.get(key, ""))
        if not value:
            continue
        match = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", value)
        if match:
            month, day, year = match.groups()
            return f"{year}_{int(month):02d}_{int(day):02d}"
        year_match = re.search(r"\b(20\d{2}|19\d{2})\b", value)
        if year_match:
            return year_match.group(1)
        return _slug(value)
    return ""


def _build_pdf_named_filename(pdf_metadata: dict, form700_cache_key: str) -> str:
    date_token = _filename_date_token(pdf_metadata)
    pieces = [
        pdf_metadata.get("owner_last_name", ""),
        pdf_metadata.get("owner_first_name", ""),
        date_token,
        pdf_metadata.get("filing_type", ""),
    ]
    stem = "_".join(_slug(piece) for piece in pieces if piece)
    if not stem:
        stem = f"form700_{form700_cache_key}"
    return f"{stem}.pdf"


def _search_payload(jurisdiction: str) -> dict:
    return {
        "queryGenerationInfo": None,
        "searchFieldQueryInfos": [
            {
                "queryField": "FilerAgency",
                "filterValue": jurisdiction,
            }
        ],
    }


def _extract_pdf_urls_from_text(text: str, base_url: str) -> list[DownloadCandidate]:
    candidates: list[DownloadCandidate] = []
    seen = set()

    parser = _AnchorCollector()
    try:
        parser.feed(text)
    except Exception:
        pass

    for href, label in parser.links:
        absolute = urljoin(base_url, href)
        if ".pdf" not in absolute.lower() and "download" not in label.lower() and "pdf" not in label.lower():
            continue
        key = absolute.lower()
        if key in seen:
            continue
        seen.add(key)
        candidates.append(DownloadCandidate(url=absolute, description=label, context=label))

    for match in re.finditer(r"https?://[^\s\"'<>]+\.pdf(?:\?[^\s\"'<>]*)?", text, flags=re.I):
        absolute = match.group(0)
        key = absolute.lower()
        if key in seen:
            continue
        seen.add(key)
        candidates.append(DownloadCandidate(url=absolute, description=absolute.rsplit("/", 1)[-1], context=""))

    for match in re.finditer(r"(?:href|src)\s*=\s*['\"]([^'\"]+\.pdf(?:\?[^'\"]*)?)['\"]", text, flags=re.I):
        absolute = urljoin(base_url, match.group(1))
        key = absolute.lower()
        if key in seen:
            continue
        seen.add(key)
        candidates.append(DownloadCandidate(url=absolute, description=absolute.rsplit("/", 1)[-1], context=""))

    return candidates


def _collect_urls_from_object(value, *, base_url: str, key_name: str = "") -> list[str]:
    urls: list[str] = []
    lowered_key = (key_name or "").lower()
    if isinstance(value, dict):
        for child_key, child_value in value.items():
            urls.extend(_collect_urls_from_object(child_value, base_url=base_url, key_name=str(child_key)))
        return urls
    if isinstance(value, list):
        for item in value:
            urls.extend(_collect_urls_from_object(item, base_url=base_url, key_name=key_name))
        return urls
    if not isinstance(value, str):
        return urls

    text = value.strip()
    if not text:
        return urls
    if text.lower().startswith(("http://", "https://", "/")):
        if any(marker in lowered_key for marker in ["url", "uri", "download", "view", "link", "path"]):
            urls.append(urljoin(base_url, text))
        elif "form700search.fppc.ca.gov" in text.lower() or "/home/" in text.lower() or "/search/" in text.lower():
            urls.append(urljoin(base_url, text))
    return urls


def _extract_urls_from_json_text(text: str, *, base_url: str) -> list[str]:
    try:
        payload = json.loads(text)
    except Exception:
        return []
    payload = _unwrap_json_payload(payload)
    seen = set()
    urls: list[str] = []
    for url in _collect_urls_from_object(payload, base_url=base_url):
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _extract_index_id(document: dict) -> str:
    for key in ["indexID", "indexId", "documentID", "documentId", "id"]:
        value = _norm(str(document.get(key, "")))
        if value:
            return value
    return ""


def _build_download_urls_from_index_id(index_id: str) -> list[str]:
    if not index_id:
        return []
    patterns = [
        "/Home/DownloadDocument?indexID={id}",
        "/Home/DownloadDocument?id={id}",
        "/Home/DownloadDocument/{id}",
        "/Home/GetDocument?indexID={id}",
        "/Home/GetDocument?id={id}",
        "/Home/GetDocument/{id}",
        "/Home/ViewDocument?indexID={id}",
        "/Home/ViewDocument?id={id}",
        "/Home/DownloadForm?indexID={id}",
        "/Home/ViewForm?indexID={id}",
    ]
    urls: list[str] = []
    for pattern in patterns:
        urls.append(urljoin(CANONICAL_FORM700_SEARCH_URL, pattern.format(id=index_id)))
    return urls


def _build_download_request(index_id: str, *, filer: dict, filing_position: dict) -> dict:
    request = {
        "indexID": index_id,
        "formInfo": {
            "LastName": _norm(filer.get("lastName", "")),
            "FirstName": _norm(filer.get("firstName", "")),
            "FilingYear": filing_position.get("filingYear", "") or "",
            "Agency": _norm(filing_position.get("agency", "")),
            "Position": _norm(filing_position.get("position", "")),
            "FilingType": _norm(filing_position.get("filingType", "")),
        },
    }
    middle_name = _norm(filer.get("middleName", ""))
    if middle_name:
        request["formInfo"]["MiddleName"] = middle_name
    return request


def _document_to_candidate(document: dict, *, base_url: str) -> DownloadCandidate | None:
    if not isinstance(document, dict):
        return None

    filer = document.get("filer") if isinstance(document.get("filer"), dict) else {}
    positions = document.get("filingPositions") if isinstance(document.get("filingPositions"), list) else []
    first_position = positions[0] if positions and isinstance(positions[0], dict) else {}
    filing_info = document.get("filingInfo") if isinstance(document.get("filingInfo"), dict) else {}
    index_id = _extract_index_id(document)

    urls = []
    seen_urls = set()
    for url in _collect_urls_from_object(document, base_url=base_url):
        if url in seen_urls:
            continue
        seen_urls.add(url)
        urls.append(url)
    for url in _build_download_urls_from_index_id(index_id):
        if url in seen_urls:
            continue
        seen_urls.add(url)
        urls.append(url)
    if not urls and not index_id:
        return None

    first_name = _norm(filer.get("firstName", ""))
    middle_name = _norm(filer.get("middleName", ""))
    last_name = _norm(filer.get("lastName", ""))
    owner_full_name = _title_case_words(first_name, middle_name, last_name)
    agency = _norm(first_position.get("agency", ""))
    position = _norm(first_position.get("position", ""))
    filing_type = _norm(first_position.get("filingType", ""))
    filing_year = str(first_position.get("filingYear", "") or "")
    filed_date = _format_portal_date(filing_info.get("filedDate", ""))
    due_date = _format_portal_date(first_position.get("dueDate", ""))

    description = _title_case_words(owner_full_name, filing_year, filing_type, position or agency)
    filing_metadata = {
        "filer_last_name": last_name,
        "filer_first_name": first_name,
        "filer_middle_name": middle_name,
        "filer_full_name": owner_full_name,
        "position_title": position,
        "agency_name": agency,
        "entity_name": _norm(first_position.get("entity", "")),
        "form_description": description,
        "filing_year": filing_year,
        "due_date": due_date,
        "filed_date": filed_date,
        "filing_type": filing_type,
    }
    download_request = _build_download_request(index_id, filer=filer, filing_position=first_position)
    primary_url = urls[0] if urls else FORM700_DOWNLOAD_ENDPOINT
    return DownloadCandidate(
        url=primary_url,
        description=description or owner_full_name or index_id,
        context=index_id or agency or position,
        index_id=index_id,
        alternate_urls=tuple(urls[1:]),
        filing_metadata=filing_metadata,
        download_request=download_request,
    )


def _collect_strings(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        out: list[str] = []
        for item in value.values():
            out.extend(_collect_strings(item))
        return out
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            out.extend(_collect_strings(item))
        return out
    return []


def _unwrap_json_payload(value):
    seen = set()
    current = value
    while isinstance(current, str):
        text = current.strip()
        if not text or text in seen:
            break
        seen.add(text)
        if not (text.startswith("{") or text.startswith("[") or text.startswith('"')):
            break
        try:
            current = json.loads(text)
        except Exception:
            break

    if isinstance(current, dict):
        for key in ["d", "result", "data", "value"]:
            nested = current.get(key)
            if isinstance(nested, str):
                nested_text = nested.strip()
                if nested_text.startswith("{") or nested_text.startswith("[") or nested_text.startswith('"'):
                    try:
                        return _unwrap_json_payload(nested)
                    except Exception:
                        return current
            if isinstance(nested, (dict, list)):
                return _unwrap_json_payload(nested)
    return current


def _extract_pdf_candidates_from_response(response: HttpResponse) -> list[DownloadCandidate]:
    body = response.text or ""
    candidates = _extract_pdf_urls_from_text(body, response.url)
    if candidates:
        return candidates

    try:
        payload = json.loads(body)
    except Exception:
        return candidates
    payload = _unwrap_json_payload(payload)

    if isinstance(payload, dict) and isinstance(payload.get("documents"), list):
        seen = set()
        for document in payload.get("documents", []):
            candidate = _document_to_candidate(document, base_url=response.url)
            if candidate is None or candidate.key in seen:
                continue
            seen.add(candidate.key)
            candidates.append(candidate)
        if candidates:
            return candidates

    strings = _collect_strings(payload)
    seen = set()
    for text in strings:
        for candidate in _extract_pdf_urls_from_text(text, response.url):
            if candidate.key in seen:
                continue
            seen.add(candidate.key)
            candidates.append(candidate)
    return candidates


class _HttpSession:
    def __init__(self):
        self.cookies = http.cookiejar.CookieJar()
        self.opener = build_opener(HTTPCookieProcessor(self.cookies))

    def get(self, url: str, *, headers: dict[str, str] | None = None) -> HttpResponse:
        request = Request(url, headers=headers or DEFAULT_HEADERS, method="GET")
        try:
            with self.opener.open(request, timeout=30) as response:
                body = response.read().decode("utf-8", errors="ignore")
                return HttpResponse(
                    url=response.geturl(),
                    status=response.status,
                    content_type=response.headers.get("Content-Type", ""),
                    text=body,
                )
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            return HttpResponse(
                url=exc.geturl(),
                status=exc.code,
                content_type=exc.headers.get("Content-Type", "") if exc.headers else "",
                text=body,
            )
        except URLError as exc:
            raise RuntimeError(f"Form 700 HTTP GET failed for {url}: {exc}") from exc

    def post_bytes(self, url: str, body: bytes, *, content_type: str) -> HttpResponse:
        headers = dict(AJAX_HEADERS)
        headers["Content-Type"] = content_type
        request = Request(url, data=body, headers=headers, method="POST")
        try:
            with self.opener.open(request, timeout=30) as response:
                text = response.read().decode("utf-8", errors="ignore")
                return HttpResponse(
                    url=response.geturl(),
                    status=response.status,
                    content_type=response.headers.get("Content-Type", ""),
                    text=text,
                )
        except HTTPError as exc:
            text = exc.read().decode("utf-8", errors="ignore")
            return HttpResponse(
                url=exc.geturl(),
                status=exc.code,
                content_type=exc.headers.get("Content-Type", "") if exc.headers else "",
                text=text,
            )
        except URLError as exc:
            raise RuntimeError(f"Form 700 HTTP POST failed for {url}: {exc}") from exc

    def post_binary(self, url: str, body: bytes, *, content_type: str) -> BinaryResponse:
        headers = dict(AJAX_HEADERS)
        headers["Content-Type"] = content_type
        request = Request(url, data=body, headers=headers, method="POST")
        try:
            with self.opener.open(request, timeout=60) as response:
                return BinaryResponse(
                    url=response.geturl(),
                    status=response.status,
                    content_type=response.headers.get("Content-Type", ""),
                    body=response.read(),
                )
        except HTTPError as exc:
            return BinaryResponse(
                url=exc.geturl(),
                status=exc.code,
                content_type=exc.headers.get("Content-Type", "") if exc.headers else "",
                body=exc.read(),
            )
        except URLError as exc:
            raise RuntimeError(f"Form 700 HTTP POST failed for {url}: {exc}") from exc

    def download_bytes(self, url: str) -> bytes:
        return self.download(url).body

    def download(self, url: str) -> BinaryResponse:
        request = Request(url, headers=DEFAULT_HEADERS, method="GET")
        try:
            with self.opener.open(request, timeout=60) as response:
                return BinaryResponse(
                    url=response.geturl(),
                    status=response.status,
                    content_type=response.headers.get("Content-Type", ""),
                    body=response.read(),
                )
        except HTTPError as exc:
            return BinaryResponse(
                url=exc.geturl(),
                status=exc.code,
                content_type=exc.headers.get("Content-Type", "") if exc.headers else "",
                body=exc.read(),
            )
        except URLError as exc:
            raise RuntimeError(f"Form 700 PDF download failed for {url}: {exc}") from exc


class Form700FPPCSync:
    def __init__(self, *, search_url: str, jurisdiction: str, headless: bool = False):
        self.search_url = _normalize_form700_search_url(search_url)
        self.jurisdiction = jurisdiction
        self.headless = headless

    def sync(
        self,
        *,
        database_path: str | Path,
        download_dir: str | Path,
        reparse_existing_form700s: bool = False,
    ) -> dict:
        stats = {
            "filers_seen": 0,
            "filings_seen": 0,
            "downloaded_filings": 0,
            "parsed_filings": 0,
            "new_filings": 0,
        }

        database = MinutesDatabase(database_path)
        database.initialize()
        download_dir = Path(download_dir)
        download_dir.mkdir(parents=True, exist_ok=True)

        print(f"[info] starting Form 700 sync for jurisdiction: {self.jurisdiction}")

        session = _HttpSession()
        landing = session.get(self.search_url)
        if (urlparse(landing.url).netloc or "").lower() != "form700search.fppc.ca.gov":
            raise RuntimeError(
                f"Unexpected Form 700 landing page '{landing.url}'. Expected the portal at {CANONICAL_FORM700_SEARCH_URL}"
            )

        payload = _search_payload(self.jurisdiction)
        payload_bytes = json.dumps(payload).encode("utf-8")
        print(f"[info] Form 700 request search endpoint: {FORM700_SEARCH_ENDPOINT}")
        search_response = session.post_bytes(
            FORM700_SEARCH_ENDPOINT,
            payload_bytes,
            content_type="application/json",
        )
        print(
            "[info] Form 700 request attempt: "
            f"status={search_response.status} "
            f"content_type={search_response.content_type or '?'} "
            f"snippet={search_response.snippet[:200]}"
        )
        if search_response.status >= 400:
            raise RuntimeError(
                "Form 700 search request failed: "
                f"status={search_response.status} snippet={search_response.snippet[:400]}"
            )

        pdf_candidates = _extract_pdf_candidates_from_response(search_response)

        if not pdf_candidates:
            lowered = (search_response.text or "").lower()
            if "no data to display" in lowered or "no data found" in lowered or '"total":0' in lowered:
                raise Form700JurisdictionNotFound(
                    f"Could not find Form 700 search results for jurisdiction '{self.jurisdiction}'"
                )
            snippet = _norm(search_response.text)[:1200]
            raise RuntimeError(f"Form 700 search response did not expose any filing candidates. Snippet: {snippet}")

        print(f"[info] Form 700 request search returned {len(pdf_candidates)} filing candidates")

        seen_owners = set()
        seen_downloads = set()
        for candidate in pdf_candidates:
            if candidate.key in seen_downloads:
                continue
            seen_downloads.add(candidate.key)
            try:
                self._sync_single_pdf_candidate(
                    session=session,
                    database=database,
                    download_dir=download_dir,
                    candidate=candidate,
                    reparse_existing_form700s=reparse_existing_form700s,
                    stats=stats,
                    seen_owners=seen_owners,
                )
            except Exception as exc:
                print(
                    f"[warn] failed to sync Form 700 candidate "
                    f"'{candidate.description or candidate.context or candidate.index_id}': {exc}"
                )

        stats["filers_seen"] = len(seen_owners)
        print(
            f"[info] Form 700 sync complete: {stats['filers_seen']} filers, "
            f"{stats['filings_seen']} filings, {stats['downloaded_filings']} downloaded, "
            f"{stats['parsed_filings']} parsed"
        )
        return stats

    def _sync_single_pdf_candidate(
        self,
        *,
        session: _HttpSession,
        database: MinutesDatabase,
        download_dir: Path,
        candidate: DownloadCandidate,
        reparse_existing_form700s: bool,
        stats: dict,
        seen_owners: set[str],
    ) -> None:
        filing_seed = dict(candidate.filing_metadata or {})
        download_identity = candidate.url
        if candidate.index_id:
            download_identity = f"fppc:index:{candidate.index_id}"
        filing = {
            "portal": "FPPC",
            "jurisdiction": self.jurisdiction,
            "filer_last_name": filing_seed.get("filer_last_name", ""),
            "filer_first_name": filing_seed.get("filer_first_name", ""),
            "filer_middle_name": filing_seed.get("filer_middle_name", ""),
            "filer_full_name": filing_seed.get("filer_full_name", ""),
            "position_title": filing_seed.get("position_title", ""),
            "agency_name": filing_seed.get("agency_name", ""),
            "entity_name": filing_seed.get("entity_name", ""),
            "form_description": (filing_seed.get("form_description") or candidate.description or candidate.context or candidate.url)[:500],
            "filing_year": filing_seed.get("filing_year", ""),
            "due_date": filing_seed.get("due_date", ""),
            "filed_date": filing_seed.get("filed_date", ""),
            "filing_type": filing_seed.get("filing_type", ""),
            "page_count": "",
            "forms_page_url": self.search_url,
            "view_form_url": candidate.url,
            "download_form_url": download_identity,
        }
        filing["form700_cache_key"] = _build_form700_cache_key(filing)

        row_state, is_new = database.upsert_form700_filing(filing)
        stats["filings_seen"] += 1
        if is_new:
            stats["new_filings"] += 1

        current_path = Path(row_state.get("pdf_path", "")) if row_state.get("pdf_path") else None
        has_local_pdf = bool(current_path and current_path.exists())
        needs_download = is_new or not has_local_pdf
        needs_parse = bool(reparse_existing_form700s or is_new or not row_state.get("parsed_at"))

        if needs_download:
            body = self._download_pdf_candidate(session, candidate)
            pdf_path = download_dir / _build_temp_download_filename(filing)
            pdf_path.write_bytes(body)
            database.record_form700_download(
                filing["form700_cache_key"],
                pdf_path=pdf_path,
                content_sha1=hashlib.sha1(body).hexdigest(),
            )
            print(f"[info] downloaded Form 700 PDF: {pdf_path}")
            row_state = database.get_form700_filing(filing["form700_cache_key"]) or row_state
            current_path = Path(row_state.get("pdf_path", ""))
            has_local_pdf = bool(current_path.exists())
            stats["downloaded_filings"] += 1

        if not has_local_pdf or current_path is None or not current_path.exists():
            return

        if not needs_parse:
            return

        try:
            pdf_metadata = extract_form700_metadata_from_pdf(
                current_path,
                filing_metadata={"jurisdiction": self.jurisdiction},
            )
            owner_name = _norm(pdf_metadata.get("owner_full_name", ""))
            if owner_name:
                seen_owners.add(owner_name)

            final_path = self._rename_pdf_from_metadata(
                current_path,
                pdf_metadata=pdf_metadata,
                form700_cache_key=filing["form700_cache_key"],
            )
            if final_path != current_path:
                current_path = final_path
                database.record_form700_download(
                    filing["form700_cache_key"],
                    pdf_path=current_path,
                    content_sha1=row_state.get("content_sha1", ""),
                )

            database.update_form700_filing_metadata(
                filing["form700_cache_key"],
                {
                    "filer_last_name": pdf_metadata.get("owner_last_name", ""),
                    "filer_first_name": pdf_metadata.get("owner_first_name", ""),
                    "filer_middle_name": pdf_metadata.get("owner_middle_name", ""),
                    "filer_full_name": pdf_metadata.get("owner_full_name", ""),
                    "position_title": pdf_metadata.get("filer_position_title", ""),
                    "agency_name": pdf_metadata.get("filer_agency_name", ""),
                    "entity_name": pdf_metadata.get("filer_entity_name", ""),
                    "filing_year": pdf_metadata.get("filing_year", ""),
                    "due_date": pdf_metadata.get("due_date", ""),
                    "filed_date": pdf_metadata.get("filed_date", ""),
                    "filing_type": pdf_metadata.get("filing_type", ""),
                    "pdf_path": str(current_path),
                },
            )

            records = parse_form700_pdf(
                current_path,
                filing_metadata={"jurisdiction": self.jurisdiction},
            )
            database.record_form700_parse_success(filing["form700_cache_key"], records)
            print(f"[info] parsed Form 700 PDF: {current_path} -> {len(records)} rows")
            stats["parsed_filings"] += 1
        except Exception as exc:
            database.record_form700_parse_error(filing["form700_cache_key"], exc)
            print(f"[warn] failed to parse Form 700 PDF {current_path}: {exc}")

    def _download_pdf_candidate(self, session: _HttpSession, candidate: DownloadCandidate) -> bytes:
        if candidate.index_id and candidate.download_request:
            body = json.dumps(candidate.download_request).encode("utf-8")
            response = session.post_binary(
                FORM700_DOWNLOAD_ENDPOINT,
                body,
                content_type="application/json",
            )
            response_body = response.body or b""
            response_content_type = (response.content_type or "").lower()
            if response.status < 400 and (
                "pdf" in response_content_type or response_body.lstrip().startswith(b"%PDF-")
            ):
                return response_body
            json_urls = _extract_urls_from_json_text(response.text, base_url=response.url)
            for json_url in json_urls:
                follow_up = session.download(json_url)
                follow_up_body = follow_up.body or b""
                follow_up_content_type = (follow_up.content_type or "").lower()
                if "pdf" in follow_up_content_type or follow_up_body.lstrip().startswith(b"%PDF-"):
                    return follow_up_body
            print(
                "[warn] Form 700 direct PDF download request did not return a PDF: "
                f"status={response.status} content_type={response.content_type or '?'} "
                f"snippet={response.snippet[:200]}"
            )

        attempted: list[str] = []
        nested_seen = set()
        queue = [candidate.url, *candidate.alternate_urls]
        while queue:
            url = queue.pop(0)
            if not url or url in attempted:
                continue
            attempted.append(url)
            response = session.download(url)
            body = response.body or b""
            content_type = (response.content_type or "").lower()
            if "pdf" in content_type or body.lstrip().startswith(b"%PDF-"):
                return body

            nested_candidates = _extract_pdf_urls_from_text(response.text, response.url)
            for nested in nested_candidates:
                nested_url = nested.url
                if nested_url and nested_url not in nested_seen and nested_url not in attempted:
                    nested_seen.add(nested_url)
                    queue.append(nested_url)

        joined = ", ".join(attempted[:8])
        raise RuntimeError(
            f"Could not resolve a PDF download URL for Form 700 candidate "
            f"'{candidate.description or candidate.context or candidate.index_id}'. Tried: {joined}"
        )

    def _rename_pdf_from_metadata(self, pdf_path: Path, *, pdf_metadata: dict, form700_cache_key: str) -> Path:
        target_name = _build_pdf_named_filename(pdf_metadata, form700_cache_key)
        target_path = pdf_path.with_name(target_name)
        if target_path == pdf_path:
            return pdf_path

        if target_path.exists():
            target_path = pdf_path.with_name(
                f"{Path(target_name).stem}_{form700_cache_key}{Path(target_name).suffix}"
            )

        pdf_path.rename(target_path)
        print(f"[info] renamed Form 700 PDF from metadata: {target_path}")
        return target_path
