

import argparse
import contextlib
import csv
import html
import importlib.util
import io
import json
import os
import re
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from rapidfuzz import fuzz


DEFAULT_INPUT_ROOT = Path(tempfile.gettempdir()) / "council_crawler"
DEFAULT_OUTPUT_NAME = "award_candidates.jsonl"
DEFAULT_CSV_OUTPUT_NAME = "award_candidates.csv"
DEFAULT_DB_HOST = os.getenv("PGHOST", "localhost")
DEFAULT_DB_PORT = int(os.getenv("PGPORT", "5432"))
DEFAULT_DB_ADMIN_NAME = os.getenv("PGDATABASE", "postgres")
DEFAULT_DB_USER = os.getenv("PGUSER", "postgres")
DEFAULT_DB_PASSWORD = os.getenv("PGPASSWORD", "123sega")
FUZZY_COMPANY_DUPLICATE_THRESHOLD = 94
ABBREVIATION_PERIOD_PLACEHOLDER = "~"
LEGAL_SUFFIXES = {
    "inc",
    "inc.",
    "llc",
    "l.l.c.",
    "corp",
    "corp.",
    "corporation",
    "co",
    "co.",
    "company",
    "ltd",
    "ltd.",
    "lp",
    "l.p.",
    "llp",
    "l.l.p.",
    "pllc",
    "pllc.",
}
VALID_ONE_WORD_COMPANIES = {
    "advocates",
    "caltrans",
    "carollo",
    "cynet",
    "docupet",
    "equitable",
    "governmentjobs",
    "homeaway",
    "hope",
    "horne",
    "iem",
    "interfaith",
    "ksvy",
    "landpaths",
    "maxim",
    "mission",
    "parcelquest",
    "placeworks",
    "proterra",
    "reach",
    "rxbenefits",
    "zetron",
}
REJECTED_ONE_WORD_COMPANIES = {
    "bertrand",
    "burke",
    "dr",
    "eastern",
    "evelyn",
    "face",
    "first",
    "four",
    "gigi",
    "harris",
    "hooper",
    "john",
    "lee",
    "local",
    "neighboring",
    "other",
    "per",
    "progress",
    "requesting",
    "robbins",
    "santa",
    "solano",
    "st",
    "sultana",
    "the",
    "three",
}

AWARD_PATTERNS = (
    r"\bAward a construction contract to\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bAward a contract to\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bAward the contract to\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bAward a purchase order contract to\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bPurchase order contract to\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bAward the construction contract for\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bAward the contract for\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bAccepting bids and awarding a contract(?:\s+to\s+(?P<company>.+?))?(?:,|\.|\n)",
    r"\bAuthorize the City Manager to execute a construction contract(?:\s+with\s+(?P<company>.+?))?(?:,|\.|\n)",
    r"\bAuthorize the City Manager(?: and Clerk of the Council)? to execute an agreement with\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bAuthorize the General Manager to execute (?:a|an)\s+.+?\s+agreement(?:\s+with\s+(?P<company>.+?))?(?:,|\.|\n)",
    r"\bAuthorize the General Manager to execute (?:a|an)\s+.+?\s+contract(?:\s+with\s+(?P<company>.+?))?(?:,|\.|\n)",
    r"\bAuthorize the Director to execute (?:a|an)\s+.+?\s+agreement(?:\s+with\s+(?P<company>.+?))?(?:,|\.|\n)",
    r"\bAuthorize the Director to execute (?:a|an)\s+.+?\s+contract(?:\s+with\s+(?P<company>.+?))?(?:,|\.|\n)",
    r"\bAuthorize the Chair to execute (?:a|an)\s+.+?\s+agreement(?:\s+with\s+(?P<company>.+?))?(?:,|\.|\n)",
    r"\bApprove Agreement\s+with(?!\s+the\s+following\b)\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bApprove Agreements\s+with(?!\s+the\s+following\b)\s+(?P<company>.+?)(?:\.|\n)",
    r"\b(?:[A-Za-z0-9()/-]+\s+){0,6}Agreements?\s+with\s+the\s+following\b[^:\n]{0,120}:\s+(?P<company>.+?)(?=\nDepartment or Agency Name|\n\d+\.\s|$)",
    r"\bAgreement with(?!\s+the\s+following\b)\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bAgreements with(?!\s+the\s+following\b)\s+(?P<company>.+?)(?:\.|\n)",
    r"\bProfessional Service Agreement with\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bService Agreement with\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bPurchase order contract with\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bApprove (?:a|an)\s+.+?\s+agreement with\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bApprove (?:a|an)\s+.+?\s+contract with\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bthe lowest responsible bidder\b",
    r"\bthe lowest responsive(?: and responsible)? bid(?:der)?\b",
    r"\bConstruction Contract for\b",
)

NEGATIVE_PATTERNS = (
    r"\bamend(?:ment)? to (?:the )?agreement\b",
    r"\brenewal\b",
    r"\bextension\b",
    r"\bchange order\b",
    r"\bfirst amendment\b",
    r"\bsecond amendment\b",
    r"\bpersonal services agreement\b",
    r"\breject all bids\b",
)

SANTA_ANA_ITEM_ID_PATTERN = re.compile(r"\b(?:\d{1,2}[A-Z]|AGMT\.?\s*NO\.?\s*\d{4}-\d+)\b", re.IGNORECASE)
SONOMA_ITEM_ID_PATTERN = re.compile(r"\b\d{1,2}\.\d{1,2}\b")
SONOMA_LEGISLATION_FILE_PATTERN = re.compile(
    r"\b(?:File\s*#|File\s*No\.?|Matter\s*File)\s*:?\s*([A-Z0-9]+-\d+)\b",
    re.IGNORECASE,
)
SONOMA_AGENDA_TRACKER_PATTERN = re.compile(
    r"(?P<tracker>\d{3,5})_A_.*?_(?P<year>\d{2})-\d{2}-\d{2}_BOS_Agenda",
    re.IGNORECASE,
)
VOTE_BLOCK_PATTERN = re.compile(
    r"(?:VOTE:|Motion carried.*?)(?P<block>.{0,500})",
    re.IGNORECASE | re.DOTALL,
)
VOTE_LINE_PATTERNS = {
    "ayes": re.compile(r"\bAYES:\s*(.+?)(?:\n|$)", re.IGNORECASE),
    "noes": re.compile(r"\bNOES:\s*(.+?)(?:\n|$)", re.IGNORECASE),
    "abstain": re.compile(r"\bABSTAIN:\s*(.+?)(?:\n|$)", re.IGNORECASE),
    "absent": re.compile(r"\bABSENT:\s*(.+?)(?:\n|$)", re.IGNORECASE),
}


@dataclass
class AwardCandidate:
    city: str
    site: str
    source_path: str
    source_url: str | None
    matched_pattern: str
    item_id: str | None
    company_name: str | None
    snippet: str
    has_negative_context: bool
    vote_ayes: list[str]
    vote_noes: list[str]
    vote_abstain: list[str]
    vote_absent: list[str]


class HtmlTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.parts.append(data)

    def get_text(self) -> str:
        return "\n".join(self.parts)


class TextExtractor:
    def __init__(self) -> None:
        self.pdf_backends = self._resolve_pdf_backends()
        self.pdf_backend = self.pdf_backends[0] if self.pdf_backends else None

    def extract(self, path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix in {".txt", ".text", ".csv", ".json", ".jsonl"}:
            return path.read_text(encoding="utf-8", errors="replace")
        if suffix in {".html", ".htm", ".xhtml"}:
            parser = HtmlTextExtractor()
            parser.feed(path.read_text(encoding="utf-8", errors="replace"))
            return html.unescape(parser.get_text())
        if suffix == ".pdf":
            return self._extract_pdf_text(path)
        return path.read_text(encoding="utf-8", errors="replace")

    def _resolve_pdf_backends(self) -> list[str]:
        backends: list[str] = []
        if importlib.util.find_spec("PyPDF2"):
            backends.append("PyPDF2")
        if importlib.util.find_spec("pypdf"):
            backends.append("pypdf")
        if self._command_exists("pdftotext"):
            backends.append("pdftotext")
        # Prefer non-MuPDF backends because some council PDFs emit noisy layer-config errors.
        if importlib.util.find_spec("fitz"):
            backends.append("fitz")
        if self._command_exists("mutool"):
            backends.append("mutool")
        return backends

    def _extract_pdf_text(self, path: Path) -> str:
        if not self.pdf_backends:
            raise RuntimeError(
                "No PDF text extractor available. Install pypdf or PyMuPDF, or add pdftotext/mutool to PATH."
            )

        last_error: Exception | None = None
        best_text = ""

        for backend in self.pdf_backends:
            captured_stdout = io.StringIO()
            captured_stderr = io.StringIO()
            try:
                with contextlib.redirect_stdout(captured_stdout), contextlib.redirect_stderr(captured_stderr):
                    text = self._extract_pdf_text_with_backend(path, backend)
            except Exception as exc:
                last_error = exc
                continue

            if self._is_usable_pdf_text(text):
                return text
            if len(text.strip()) > len(best_text.strip()):
                best_text = text

        if best_text.strip():
            return best_text
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Unable to extract text from PDF: {path}")

    def _extract_pdf_text_with_backend(self, path: Path, backend: str) -> str:
        if backend == "pypdf":
            from pypdf import PdfReader  # type: ignore

            reader = PdfReader(str(path))
            return "\n".join(page.extract_text() or "" for page in reader.pages)

        if backend == "PyPDF2":
            from PyPDF2 import PdfReader  # type: ignore

            reader = PdfReader(str(path))
            return "\n".join(page.extract_text() or "" for page in reader.pages)

        if backend == "fitz":
            import fitz  # type: ignore

            text_parts: list[str] = []
            with fitz.open(path) as document:
                for page in document:
                    text_parts.append(page.get_text("text"))
            return "\n".join(text_parts)

        if backend == "pdftotext":
            completed = subprocess.run(
                ["pdftotext", "-layout", str(path), "-"],
                capture_output=True,
                text=True,
                check=True,
            )
            return completed.stdout

        if backend == "mutool":
            completed = subprocess.run(
                ["mutool", "draw", "-F", "txt", str(path)],
                capture_output=True,
                text=True,
                check=True,
            )
            return completed.stdout

        raise RuntimeError(f"Unsupported PDF backend: {backend}")

    def _is_usable_pdf_text(self, text: str) -> bool:
        normalized = re.sub(r"\s+", " ", text).strip()
        alnum_count = sum(character.isalnum() for character in normalized)
        return len(normalized) >= 200 and alnum_count >= 80

    def _command_exists(self, name: str) -> bool:
        completed = subprocess.run(
            ["where.exe", name],
            capture_output=True,
            text=True,
            check=False,
        )
        return completed.returncode == 0


class AwardParser:
    def __init__(self, input_root: Path) -> None:
        self.input_root = input_root
        self.extractor = TextExtractor()
        self.compiled_award_patterns = [re.compile(pattern, re.IGNORECASE | re.DOTALL) for pattern in AWARD_PATTERNS]
        self.compiled_negative_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in NEGATIVE_PATTERNS]

    def parse(self, sites: Iterable[str] | None = None) -> tuple[list[AwardCandidate], list[str]]:
        site_names = list(sites) if sites else self._discover_sites()
        candidates: list[AwardCandidate] = []
        warnings: list[str] = []
        seen_legislation_urls: set[str] = set()

        for site_name in site_names:
            site_root = self.input_root / site_name
            if not site_root.exists():
                warnings.append(f"Missing site folder: {site_root}")
                continue

            manifest_path = site_root / "manifest.jsonl"
            if not manifest_path.exists():
                warnings.append(f"Missing manifest: {manifest_path}")
                continue

            for entry in self._load_manifest_entries(manifest_path):
                if entry.get("kind") == "document" and entry.get("status") == "saved":
                    local_path = entry.get("local_path")
                    if not local_path:
                        continue

                    path = Path(local_path)
                    if not path.exists():
                        warnings.append(f"Missing downloaded file: {path}")
                        continue

                    try:
                        text = self.extractor.extract(path)
                    except Exception as exc:
                        warnings.append(f"{path}: {exc}")
                        continue

                    normalized_text = self._normalize_text(text)
                    candidates.extend(
                        self._extract_candidates(
                            site_name=site_name,
                            path=path,
                            source_url=entry.get("url"),
                            text=normalized_text,
                            source_kind="document",
                        )
                    )
                    continue

                if not self._is_legislation_page_entry(entry):
                    continue

                source_page_url = entry.get("url")
                source_url = source_page_url
                if not source_page_url or source_page_url in seen_legislation_urls:
                    continue

                seen_legislation_urls.add(source_page_url)
                try:
                    text = self._fetch_html_text(source_page_url or source_url)
                except Exception as exc:
                    warnings.append(f"{source_page_url or source_url}: {exc}")
                    continue

                normalized_text = self._normalize_text(text)
                candidates.extend(
                    self._extract_candidates(
                        site_name=site_name,
                        path=Path(source_page_url or source_url),
                        source_url=source_url,
                        text=normalized_text,
                        source_kind="legislation_page",
                    )
                )

        return self._deduplicate(candidates), warnings

    def _is_legislation_page_entry(self, entry: dict) -> bool:
        return (
            entry.get("kind") == "page"
            and entry.get("status") in {"fetched", "saved"}
            and "LegislationDetail.aspx" in str(entry.get("url") or "")
        )

    def _fetch_html_text(self, url: str) -> str:
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; AwardParser/1.0; +https://example.invalid)",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        with urlopen(request, timeout=30) as response:
            html_text = response.read().decode("utf-8", errors="replace")
        parser = HtmlTextExtractor()
        parser.feed(html_text)
        return html.unescape(parser.get_text())

    def _extract_candidates(
        self,
        site_name: str,
        path: Path,
        source_url: str | None,
        text: str,
        source_kind: str,
    ) -> list[AwardCandidate]:
        city = "Santa Ana" if site_name == "santa_ana" else "Sonoma"
        item_id_pattern = SANTA_ANA_ITEM_ID_PATTERN if site_name == "santa_ana" else SONOMA_ITEM_ID_PATTERN
        candidates: list[AwardCandidate] = []
        kept_ranges: list[tuple[int, int, str | None, str | None]] = []
        searchable_text = self._protect_abbreviation_periods(
            self._select_searchable_text(site_name, source_kind, text)
        )
        legislation_file = self._extract_legislation_file(site_name, source_kind, text)
        agenda_tracker = self._extract_agenda_tracker(site_name, source_kind, path, source_url)

        for pattern in self.compiled_award_patterns:
            for match in pattern.finditer(searchable_text):
                window_start = max(0, match.start() - 800)
                window_end = min(len(searchable_text), match.end() + 1200)
                window = searchable_text[window_start:window_end]
                snippet = self._extract_match_snippet(searchable_text, match.start(), match.end())
                raw_company_value = self._extend_short_multiline_company_name(
                    raw_value=match.groupdict().get("company"),
                    text=searchable_text,
                    company_end=match.end("company") if "company" in match.groupdict() and match.group("company") else None,
                )
                raw_company_value = self._extend_wrapped_company_name(
                    raw_value=raw_company_value,
                    text=searchable_text,
                    company_end=match.end("company") if "company" in match.groupdict() and match.group("company") else None,
                )
                raw_company_value = self._extend_company_with_legal_suffix(
                    raw_value=raw_company_value,
                    text=searchable_text,
                    company_end=match.end("company") if "company" in match.groupdict() and match.group("company") else None,
                )
                company_names = self._extract_company_names(
                    raw_value=raw_company_value,
                    matched_pattern=pattern.pattern,
                )
                if not company_names:
                    continue
                item_id = self._find_nearest_item_id(searchable_text, match.start(), item_id_pattern)
                item_id = self._merge_item_id_with_legislation_file(item_id, legislation_file)
                item_id = self._merge_item_id_with_agenda_tracker(item_id, agenda_tracker)
                votes = self._extract_votes(window)
                has_negative_context = any(pattern.search(window) for pattern in self.compiled_negative_patterns)

                for company_name in company_names:
                    # Keep the earliest, most specific pattern when multiple award phrases
                    # match the same text block.
                    if self._is_duplicate_match(
                        kept_ranges=kept_ranges,
                        start=match.start(),
                        end=match.end(),
                        item_id=item_id,
                        company_name=company_name,
                    ):
                        continue

                    candidates.append(
                        AwardCandidate(
                            city=city,
                            site=site_name,
                            source_path=str(path),
                            source_url=source_url,
                            matched_pattern=pattern.pattern,
                            item_id=item_id,
                            company_name=company_name,
                            snippet=snippet,
                            has_negative_context=has_negative_context,
                            vote_ayes=votes["ayes"],
                            vote_noes=votes["noes"],
                            vote_abstain=votes["abstain"],
                            vote_absent=votes["absent"],
                        )
                    )
                    kept_ranges.append((match.start(), match.end(), item_id, company_name))

        return candidates

    def _extract_votes(self, text: str) -> dict[str, list[str]]:
        result = {"ayes": [], "noes": [], "abstain": [], "absent": []}
        vote_block_match = VOTE_BLOCK_PATTERN.search(text)
        vote_text = vote_block_match.group("block") if vote_block_match else text

        for field, pattern in VOTE_LINE_PATTERNS.items():
            match = pattern.search(vote_text)
            if match:
                result[field] = self._split_names(match.group(1))

        return result

    def _find_nearest_item_id(self, text: str, match_start: int, pattern: re.Pattern[str]) -> str | None:
        search_start = max(0, match_start - 600)
        search_end = min(len(text), match_start + 200)
        nearby_text = text[search_start:search_end]
        matches = list(pattern.finditer(nearby_text))
        if not matches:
            return None
        return matches[-1].group(0).strip()

    def _extract_legislation_file(self, site_name: str, source_kind: str, text: str) -> str | None:
        if site_name != "sonoma" or source_kind != "legislation_page":
            return None

        match = SONOMA_LEGISLATION_FILE_PATTERN.search(text)
        if not match:
            return None
        return match.group(1).strip().upper()

    def _merge_item_id_with_legislation_file(
        self,
        item_id: str | None,
        legislation_file: str | None,
    ) -> str | None:
        if not legislation_file:
            return item_id
        if item_id and legislation_file.lower() in item_id.lower():
            return item_id
        if item_id:
            return f"{item_id} | {legislation_file}"
        return legislation_file

    def _extract_agenda_tracker(
        self,
        site_name: str,
        source_kind: str,
        path: Path,
        source_url: str | None,
    ) -> str | None:
        if site_name != "sonoma" or source_kind != "document":
            return None

        candidates = [path.name]
        if source_url:
            candidates.append(Path(urlparse(source_url).path).name)

        for candidate in candidates:
            match = SONOMA_AGENDA_TRACKER_PATTERN.search(candidate)
            if not match:
                continue
            return f"20{match.group('year')}-{match.group('tracker')}"
        return None

    def _merge_item_id_with_agenda_tracker(
        self,
        item_id: str | None,
        agenda_tracker: str | None,
    ) -> str | None:
        if not agenda_tracker:
            return item_id
        if item_id and agenda_tracker.lower() in item_id.lower():
            return item_id
        if item_id:
            return f"{item_id} | {agenda_tracker}"
        return agenda_tracker

    def _normalize_text(self, text: str) -> str:
        text = text.replace("\r", "\n")
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        return text

    def _select_searchable_text(self, site_name: str, source_kind: str, text: str) -> str:
        if site_name != "sonoma" or source_kind != "legislation_page":
            return text

        sections: list[str] = []
        for label in ("Title:", "Recommended Action:"):
            start = text.find(label)
            if start == -1:
                continue
            following_text = text[start:]
            next_headers = [
                following_text.find(header)
                for header in (
                    "Executive Summary:",
                    "Discussion:",
                    "Fiscal Summary",
                    "Attachments:",
                    "Summary Report",
                    "end",
                )
                if following_text.find(header) > len(label)
            ]
            end = min(next_headers) if next_headers else len(following_text)
            sections.append(following_text[:end])

        return "\n".join(sections) if sections else text

    def _extract_match_snippet(self, text: str, start: int, end: int) -> str:
        line_start = text.rfind("\n", 0, start)
        line_end = text.find("\n", end)
        if line_start == -1:
            line_start = 0
        else:
            line_start += 1
        if line_end == -1:
            line_end = len(text)

        snippet = text[line_start:line_end].strip()
        if snippet:
            return self._clean_snippet(self._trim_snippet_lead(snippet))

        sentence_start_candidates = [text.rfind(marker, 0, start) for marker in (". ", "! ", "? ", ": ")]
        sentence_start = max(sentence_start_candidates)
        sentence_end_candidates = [text.find(marker, end) for marker in (". ", "! ", "? ", "\n")]
        valid_sentence_ends = [index for index in sentence_end_candidates if index != -1]
        sentence_end = min(valid_sentence_ends) if valid_sentence_ends else len(text)
        if sentence_start == -1:
            sentence_start = 0
        else:
            sentence_start += 2

        return self._clean_snippet(self._trim_snippet_lead(text[sentence_start:sentence_end]))

    def _clean_snippet(self, value: str) -> str:
        value = re.sub(r"\s+", " ", value).strip()
        return value[:400]

    def _trim_snippet_lead(self, value: str) -> str:
        match = re.search(r"\b(to|for)\b", value, re.IGNORECASE)
        if match:
            trimmed = value[match.start():].strip()
            if trimmed:
                return trimmed
        return value.strip()

    def _clean_company_name(self, value: str | None) -> str | None:
        if not value:
            return None
        value = self._restore_abbreviation_periods(value)
        cleaned = re.sub(r"\s+", " ", value).strip(" ,.;:-")
        cleaned = re.sub(
            r"(?i)\b(Inc\.?|L\.L\.C\.?|LLC|Corp\.?|Corporation|Co\.?|Company|Ltd\.?|L\.P\.?|LP|L\.L\.P\.?|LLP|PLLC\.?),\s+in\b.*$",
            r"\1",
            cleaned,
        ).strip(" ,.;:-")
        cleaned = re.sub(r"\b(?:to|for)\b.*$", "", cleaned, flags=re.IGNORECASE).strip(" ,.;:-")
        cleaned = re.sub(
            r"\b(?:for a term|subject to|in an amount|which includes|beginning on|for the period).*$",
            "",
            cleaned,
            flags=re.IGNORECASE,
        ).strip(" ,.;:-")
        cleaned = re.sub(r"\s*\(Property No.*$", "", cleaned, flags=re.IGNORECASE).strip(" ,.;:-")
        cleaned = re.sub(r"\s+with a contract amount.*$", "", cleaned, flags=re.IGNORECASE).strip(" ,.;:-")
        return cleaned or None

    def _extend_company_with_legal_suffix(
        self,
        raw_value: str | None,
        text: str,
        company_end: int | None,
    ) -> str | None:
        if raw_value is None or company_end is None:
            return raw_value

        remaining_text = text[company_end:company_end + 20]
        match = re.match(
            r"\s*,\s*(Inc\.?|L\.L\.C\.?|LLC|Corp\.?|Corporation|Co\.?|Company|Ltd\.?|L\.P\.?|LP|L\.L\.P\.?|LLP|PLLC\.?)\b",
            remaining_text,
            re.IGNORECASE,
        )
        if not match:
            return raw_value

        suffix = match.group(1).strip()
        if suffix.lower() not in LEGAL_SUFFIXES:
            return raw_value
        return f"{raw_value}, {suffix}"

    def _extend_wrapped_company_name(
        self,
        raw_value: str | None,
        text: str,
        company_end: int | None,
    ) -> str | None:
        if raw_value is None or company_end is None:
            return raw_value

        remaining_text = text[company_end:company_end + 120]
        if not remaining_text.startswith("\n"):
            return raw_value

        next_line = remaining_text.lstrip("\n").split("\n", 1)[0].strip()
        if not next_line:
            return raw_value

        # Continue across wrapped lines when the next line looks like the rest
        # of the entity name instead of a new sentence or section.
        if not re.match(r"[A-Z][A-Za-z&'().,/\- ]+$", next_line):
            return raw_value
        if re.match(r"(?:[A-Z]\)|\d+\.)", next_line):
            return raw_value

        return f"{raw_value.rstrip()} {next_line}".strip()

    def _extend_short_multiline_company_name(
        self,
        raw_value: str | None,
        text: str,
        company_end: int | None,
    ) -> str | None:
        if raw_value is None:
            return None
        if company_end is None:
            return raw_value

        stripped_value = raw_value.strip()
        compact_length = len(re.sub(r"\W+", "", stripped_value))
        if compact_length >= 3:
            return raw_value

        remaining_text = text[company_end:company_end + 200]
        if not remaining_text.startswith("\n"):
            return raw_value

        continuation = remaining_text
        combined_value = stripped_value
        while continuation.startswith("\n") or continuation.startswith(" "):
            continuation = continuation.lstrip()
            if not continuation:
                break
            next_part = re.split(r"(?:,|\.|\n)", continuation, maxsplit=1)[0].strip()
            if not next_part:
                break
            combined_value = f"{combined_value} {next_part}".strip()
            if len(re.sub(r"\W+", "", combined_value)) >= 3:
                return combined_value
            consumed_index = continuation.find(next_part) + len(next_part)
            continuation = continuation[consumed_index:]

        return combined_value or raw_value

    def _extract_company_names(self, raw_value: str | None, matched_pattern: str) -> list[str | None]:
        if self._pattern_uses_following_list(matched_pattern):
            return self._extract_following_list_company_names(raw_value)

        cleaned = self._clean_company_name(raw_value)
        if not cleaned:
            return []

        if not self._pattern_supports_multiple_companies(matched_pattern):
            return [] if self._should_reject_company_name(cleaned) else [cleaned]

        normalized = re.sub(r"\s+(?:and|&)\s+", ", ", cleaned, flags=re.IGNORECASE)
        parts = [self._clean_company_name(part) for part in self._split_company_parts(normalized)]
        company_names: list[str | None] = []
        for part in parts:
            if not part or self._should_reject_company_name(part):
                continue
            company_names.append(part)
        return company_names or ([] if self._should_reject_company_name(cleaned) else [cleaned])

    def _pattern_supports_multiple_companies(self, matched_pattern: str) -> bool:
        return (
            r"\bApprove Agreements\s+with\s+" in matched_pattern
            or r"\bAgreements with\s+" in matched_pattern
        )

    def _pattern_uses_following_list(self, matched_pattern: str) -> bool:
        lowered = matched_pattern.lower()
        return "with\\s+the\\s+following\\b" in lowered

    def _extract_following_list_company_names(self, raw_value: str | None) -> list[str | None]:
        if not raw_value:
            return []

        normalized = self._restore_abbreviation_periods(raw_value)
        normalized = re.sub(
            r"(?is)(?:,?\s+to\s+provide\b[\s\S]*|,?\s+for\s+(?:a|an|the)\b[\s\S]*|,?\s+for\s+[A-Za-z][\s\S]*|,?\s+with\s+an\s+option\b[\s\S]*)$",
            "",
            normalized,
        ).strip()
        entries = [part.strip() for part in re.split(r"\s*;\s*", normalized) if part.strip()]
        expanded_entries: list[str] = []
        for entry in entries:
            split_entries = re.split(r",\s+and\s+", entry, maxsplit=1, flags=re.IGNORECASE)
            expanded_entries.extend(part.strip() for part in split_entries if part.strip())
        entries = expanded_entries

        company_names: list[str | None] = []
        for entry in entries:
            if not entry:
                continue
            entry = re.sub(r"^\s*and\s+", "", entry, flags=re.IGNORECASE).strip()
            entry = re.sub(r"(?is)\s+to\s+provide\b[\s\S]*$", "", entry).strip()
            entry = re.sub(r"\([^)]*\)", "", entry).strip()
            entry = re.sub(r",\s*\$\s*[\d,]+(?:\.\d{2})?\s*\.?$", "", entry).strip()
            entry = re.sub(r"\s+\$\s*[\d,]+(?:\.\d{2})?\s*\.?$", "", entry).strip()
            if not re.search(r"[A-Za-z]", entry):
                continue
            if re.match(r"(?i)^(?:\d|per\b|through\b|during\b|effective\b|option\b|term\b|up\b)", entry):
                continue
            if re.fullmatch(r"\$?\s*[\d,]+(?:\.\d{2})?", entry):
                continue
            cleaned = self._clean_company_name(entry)
            if cleaned and not re.search(r"[A-Za-z]", cleaned):
                continue
            if not cleaned or self._should_reject_company_name(cleaned):
                continue
            company_names.append(cleaned)
        return company_names

    def _should_reject_company_name(self, company_name: str | None) -> bool:
        if not company_name:
            return False

        lowered = company_name.lower().strip()
        lowered_core = lowered.rstrip(".")
        tokens = [token for token in re.split(r"\s+", lowered_core) if token]
        if len(tokens) == 1:
            token = tokens[0]
            if token in VALID_ONE_WORD_COMPANIES:
                return False
            if token in REJECTED_ONE_WORD_COMPANIES:
                return True

        if lowered_core in {"in"}:
            return True
        if lowered_core in {suffix.rstrip(".") for suffix in LEGAL_SUFFIXES}:
            return True

        bad_prefixes = (
            "the ihcp",
            "the california",
            "the commission",
            "the county",
            "the board",
            "the city of",
            "the state of",
            "department of",
            "the district",
            "the proposed",
            "in substantially",
            "in a form",
            "the following entities:",
            "the following grantees:",
        )
        if lowered.startswith(bad_prefixes):
            return True

        bad_fragments = (
            "city",
            "college",
            "university",
            "department of",
            "district",
            "county",
            "to ensure that",
            "for administration and distribution",
            "tax proceeds",
            "claim is processed",
        )
        return any(fragment in lowered for fragment in bad_fragments)

    def _split_names(self, raw_value: str) -> list[str]:
        normalized = re.sub(r"\(\d+\)", "", raw_value)
        normalized = normalized.replace(" and ", ", ")
        parts = [part.strip(" ,.;") for part in normalized.split(",")]
        return [part for part in parts if part and part.lower() != "none"]

    def _split_company_parts(self, value: str) -> list[str]:
        raw_parts = [part.strip() for part in value.split(",")]
        if not raw_parts:
            return []

        merged_parts: list[str] = []
        for part in raw_parts:
            if not part:
                continue
            normalized_part = part.strip().lower()
            if merged_parts and normalized_part in LEGAL_SUFFIXES:
                merged_parts[-1] = f"{merged_parts[-1]}, {part}"
                continue
            merged_parts.append(part)
        return merged_parts

    def _protect_abbreviation_periods(self, text: str) -> str:
        text = re.sub(
            r"\b(?:[A-Za-z]\.){2,}",
            lambda match: match.group(0).replace(".", ABBREVIATION_PERIOD_PLACEHOLDER),
            text,
        )
        return re.sub(
            r"\b([A-Za-z])\.(?=\s+[A-Z])",
            lambda match: match.group(1) + ABBREVIATION_PERIOD_PLACEHOLDER,
            text,
        )

    def _restore_abbreviation_periods(self, text: str) -> str:
        return text.replace(ABBREVIATION_PERIOD_PLACEHOLDER, ".")

    def _deduplicate(self, candidates: list[AwardCandidate]) -> list[AwardCandidate]:
        unique: dict[tuple[str | None, str | None, str | None], AwardCandidate] = {}
        for candidate in candidates:
            normalized_company = candidate.company_name.lower() if candidate.company_name else None
            key = (candidate.source_url, candidate.item_id, normalized_company)
            existing = unique.get(key)
            if existing is None or len(candidate.snippet) > len(existing.snippet):
                unique[key] = candidate
        deduped_candidates = list(unique.values())
        filtered_candidates: list[AwardCandidate] = []
        for candidate in deduped_candidates:
            if self._has_more_complete_company_match(candidate, deduped_candidates):
                continue
            filtered_candidates.append(candidate)

        return filtered_candidates

    def _has_more_complete_company_match(
        self,
        candidate: AwardCandidate,
        candidates: list[AwardCandidate],
    ) -> bool:
        if not candidate.company_name:
            return False

        candidate_name = self._normalize_company_for_comparison(candidate.company_name)
        if not candidate_name:
            return False

        for other in candidates:
            if other is candidate:
                continue
            if other.source_url != candidate.source_url or other.item_id != candidate.item_id:
                continue
            if not other.company_name:
                continue

            other_name = self._normalize_company_for_comparison(other.company_name)
            if not other_name or other_name == candidate_name:
                continue
            if other_name.startswith(candidate_name) and len(other_name) > len(candidate_name):
                return True
            if self._is_fuzzy_more_complete_company_match(candidate_name, other_name):
                return True
        return False

    def _normalize_company_for_comparison(self, company_name: str) -> str:
        normalized = company_name.strip().lower().replace("&", " and ")
        normalized = re.sub(r"[^a-z0-9\s]+", " ", normalized)
        tokens = [token for token in normalized.split() if token]
        legal_suffixes = {
            "inc",
            "llc",
            "corp",
            "corporation",
            "co",
            "company",
            "ltd",
            "lp",
            "llp",
            "pllc",
        }
        while tokens and tokens[-1] in legal_suffixes:
            tokens.pop()
        return " ".join(tokens)

    def _is_fuzzy_more_complete_company_match(self, candidate_name: str, other_name: str) -> bool:
        if not candidate_name or not other_name:
            return False
        if len(other_name) <= len(candidate_name):
            return False
        score = int(fuzz.token_set_ratio(candidate_name, other_name))
        return score >= FUZZY_COMPANY_DUPLICATE_THRESHOLD

    def _is_duplicate_match(
        self,
        kept_ranges: list[tuple[int, int, str | None, str | None]],
        start: int,
        end: int,
        item_id: str | None,
        company_name: str | None,
    ) -> bool:
        for kept_start, kept_end, kept_item_id, kept_company_name in kept_ranges:
            overlaps = start <= kept_end + 120 and end >= kept_start - 120
            same_company = company_name is not None and company_name == kept_company_name
            same_item_and_company = (
                item_id is not None
                and item_id == kept_item_id
                and (
                    company_name is None
                    or kept_company_name is None
                    or same_company
                )
            )
            if overlaps and (same_item_and_company or same_company or (item_id is None and company_name is None)):
                return True
        return False

    def _discover_sites(self) -> list[str]:
        return sorted(path.name for path in self.input_root.iterdir() if path.is_dir())

    def _load_manifest_entries(self, manifest_path: Path) -> Iterable[dict]:
        with manifest_path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    yield json.loads(line)


def write_jsonl(path: Path, candidates: list[AwardCandidate]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for candidate in candidates:
            handle.write(json.dumps(asdict(candidate), ensure_ascii=True) + "\n")


def write_json_array(path: Path, candidates: list[AwardCandidate]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump([asdict(candidate) for candidate in candidates], handle, ensure_ascii=True, indent=2)


def write_csv(path: Path, candidates: list[AwardCandidate]) -> None:
    fieldnames = [
        "city",
        "site",
        "source_path",
        "source_url",
        "matched_pattern",
        "item_id",
        "company_name",
        "snippet",
        "has_negative_context",
        "vote_ayes",
        "vote_noes",
        "vote_abstain",
        "vote_absent",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for candidate in candidates:
            row = asdict(candidate)
            row["vote_ayes"] = "; ".join(candidate.vote_ayes)
            row["vote_noes"] = "; ".join(candidate.vote_noes)
            row["vote_abstain"] = "; ".join(candidate.vote_abstain)
            row["vote_absent"] = "; ".join(candidate.vote_absent)
            writer.writerow(row)


def database_name_for_location(location: str) -> str:
    cleaned = re.sub(r"\W+", "_", location.strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "unknown_location"


def save_candidates_to_location_databases(
    candidates: list[AwardCandidate],
    host: str,
    port: int,
    admin_database: str,
    user: str,
    password: str,
) -> dict[str, int]:
    grouped_rows: dict[str, dict[tuple[str, str], dict[str, str]]] = {}

    for candidate in candidates:
        if not candidate.company_name or not candidate.source_url:
            continue
        database_name = database_name_for_location(candidate.city)
        grouped_rows.setdefault(database_name, {})
        key = (candidate.company_name, candidate.source_url)
        existing_row = grouped_rows[database_name].get(key, {"snippet": "", "source_path": ""})
        existing_snippet = existing_row.get("snippet", "")
        candidate_snippet = candidate.snippet.strip()
        if len(candidate_snippet) > len(existing_snippet):
            existing_row["snippet"] = candidate_snippet
        if candidate.source_path and not existing_row.get("source_path"):
            existing_row["source_path"] = candidate.source_path
        grouped_rows[database_name][key] = existing_row

    inserted_counts: dict[str, int] = {}
    for database_name, row_map in grouped_rows.items():
        ensure_postgres_database_exists(
            database_name=database_name,
            host=host,
            port=port,
            admin_database=admin_database,
            user=user,
            password=password,
        )
        inserted_counts[database_name] = upsert_company_links(
            database_name=database_name,
            rows=sorted(
                (
                    company_name,
                    source_url,
                    values.get("snippet", ""),
                    values.get("source_path", ""),
                )
                for (company_name, source_url), values in row_map.items()
            ),
            host=host,
            port=port,
            user=user,
            password=password,
        )

    return inserted_counts


def ensure_postgres_database_exists(
    database_name: str,
    host: str,
    port: int,
    admin_database: str,
    user: str,
    password: str,
) -> None:
    connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=admin_database,
        user=user,
        password=password,
    )
    connection.autocommit = True

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
            if cursor.fetchone():
                return
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
    finally:
        connection.close()


def upsert_company_links(
    database_name: str,
    rows: list[tuple[str, str, str, str]],
    host: str,
    port: int,
    user: str,
    password: str,
) -> int:
    if not rows:
        return 0

    connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=database_name,
        user=user,
        password=password,
    )

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS company_links (
                        id BIGSERIAL PRIMARY KEY,
                        company_name TEXT NOT NULL,
                        source_url TEXT NOT NULL,
                        snippet TEXT,
                        source_path TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE (company_name, source_url)
                    )
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE company_links
                    ADD COLUMN IF NOT EXISTS snippet TEXT
                    """
                )
                cursor.execute(
                    """
                    ALTER TABLE company_links
                    ADD COLUMN IF NOT EXISTS source_path TEXT
                    """
                )
                execute_values(
                    cursor,
                    """
                    INSERT INTO company_links (company_name, source_url, snippet, source_path)
                    VALUES %s
                    ON CONFLICT (company_name, source_url) DO UPDATE
                    SET snippet = CASE
                        WHEN EXCLUDED.snippet IS NOT NULL
                             AND BTRIM(EXCLUDED.snippet) <> ''
                             AND (
                                 company_links.snippet IS NULL
                                 OR BTRIM(company_links.snippet) = ''
                                 OR LENGTH(EXCLUDED.snippet) > LENGTH(company_links.snippet)
                             )
                        THEN EXCLUDED.snippet
                        ELSE company_links.snippet
                    END,
                    source_path = CASE
                        WHEN EXCLUDED.source_path IS NOT NULL
                             AND BTRIM(EXCLUDED.source_path) <> ''
                             AND (
                                 company_links.source_path IS NULL
                                 OR BTRIM(company_links.source_path) = ''
                             )
                        THEN EXCLUDED.source_path
                        ELSE company_links.source_path
                    END
                    """,
                    rows,
                )
                inserted_count = cursor.rowcount
        return inserted_count if inserted_count >= 0 else 0
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read downloaded meeting documents and emit award-pattern candidates."
    )
    parser.add_argument(
        "--input-root",
        default=str(DEFAULT_INPUT_ROOT),
        help=f"Root directory created by council_crawler.py. Default: {DEFAULT_INPUT_ROOT}",
    )
    parser.add_argument(
        "--sites",
        nargs="+",
        default=None,
        choices=("santa_ana", "sonoma"),
        help="Which site subfolders to parse. Default: all subfolders under input root.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="JSONL output file. Default: <input-root>/award_candidates.jsonl",
    )
    parser.add_argument(
        "--csv-output",
        default=None,
        help="CSV output path. Default: <input-root>/award_candidates.csv",
    )
    parser.add_argument(
        "--json-array-output",
        default=None,
        help="Optional JSON array output path.",
    )
    parser.add_argument(
        "--print-warnings",
        action="store_true",
        help="Print parser warnings such as unreadable PDFs.",
    )
    parser.add_argument(
        "--save-to-location-dbs",
        action="store_true",
        help="Save company_name and source_url into PostgreSQL databases named after each candidate city.",
    )
    parser.add_argument("--db-host", default=DEFAULT_DB_HOST, help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT, help="PostgreSQL port")
    parser.add_argument(
        "--db-admin-name",
        default=DEFAULT_DB_ADMIN_NAME,
        help="PostgreSQL database used to create location databases when needed.",
    )
    parser.add_argument("--db-user", default=DEFAULT_DB_USER, help="PostgreSQL user")
    parser.add_argument("--db-password", default=DEFAULT_DB_PASSWORD, help="PostgreSQL password")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_root = Path(args.input_root)
    output_path = Path(args.output) if args.output else input_root / DEFAULT_OUTPUT_NAME
    csv_path = Path(args.csv_output) if args.csv_output else input_root / DEFAULT_CSV_OUTPUT_NAME

    parser = AwardParser(input_root=input_root)
    candidates, warnings = parser.parse(sites=args.sites)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_jsonl(output_path, candidates)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_csv(csv_path, candidates)

    if args.json_array_output:
        json_array_path = Path(args.json_array_output)
        json_array_path.parent.mkdir(parents=True, exist_ok=True)
        write_json_array(json_array_path, candidates)

    inserted_counts: dict[str, int] = {}
    if args.save_to_location_dbs:
        inserted_counts = save_candidates_to_location_databases(
            candidates=candidates,
            host=args.db_host,
            port=args.db_port,
            admin_database=args.db_admin_name,
            user=args.db_user,
            password=args.db_password,
        )

    summary = {
        "input_root": str(input_root),
        "output_path": str(output_path),
        "csv_output": str(csv_path),
        "candidate_count": len(candidates),
        "warning_count": len(warnings),
        "pdf_backend": parser.extractor.pdf_backend,
    }
    if args.json_array_output:
        summary["json_array_output"] = str(Path(args.json_array_output))
    if args.save_to_location_dbs:
        summary["location_databases"] = inserted_counts
    print(json.dumps(summary, indent=2))

    if args.print_warnings and warnings:
        for warning in warnings:
            print(warning)


if __name__ == "__main__":
    main()
 main()
