from __future__ import annotations

import argparse
import contextlib
import csv
import html
import importlib.util
import io
import json
import re
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.request import Request, urlopen


DEFAULT_INPUT_ROOT = Path(tempfile.gettempdir()) / "council_crawler"
DEFAULT_OUTPUT_NAME = "award_candidates.jsonl"
DEFAULT_CSV_OUTPUT_NAME = "award_candidates.csv"

AWARD_PATTERNS = (
    r"\bAward a construction contract to\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bAward a contract to\s+(?P<company>.+?)(?:,|\.|\n)",
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
    r"\bApprove Agreement\s+with\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bApprove Agreements\s+with\s+(?P<company>.+?)(?:\.|\n)",
    r"\bAgreement with\s+(?P<company>.+?)(?:,|\.|\n)",
    r"\bAgreements with\s+(?P<company>.+?)(?:\.|\n)",
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
    r"\breject all bids\b",
)

SANTA_ANA_ITEM_ID_PATTERN = re.compile(r"\b(?:\d{1,2}[A-Z]|AGMT\.?\s*NO\.?\s*\d{4}-\d+)\b", re.IGNORECASE)
SONOMA_ITEM_ID_PATTERN = re.compile(r"\b\d{1,2}\.\d{1,2}\b")
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

                source_url = entry.get("url")
                if not source_url or source_url in seen_legislation_urls:
                    continue

                seen_legislation_urls.add(source_url)
                try:
                    text = self._fetch_html_text(source_url)
                except Exception as exc:
                    warnings.append(f"{source_url}: {exc}")
                    continue

                normalized_text = self._normalize_text(text)
                candidates.extend(
                    self._extract_candidates(
                        site_name=site_name,
                        path=Path(source_url),
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
        searchable_text = self._select_searchable_text(site_name, source_kind, text)

        for pattern in self.compiled_award_patterns:
            for match in pattern.finditer(searchable_text):
                window_start = max(0, match.start() - 800)
                window_end = min(len(searchable_text), match.end() + 1200)
                window = searchable_text[window_start:window_end]
                snippet = self._extract_match_snippet(searchable_text, match.start(), match.end())
                company_names = self._extract_company_names(
                    raw_value=match.groupdict().get("company"),
                    matched_pattern=pattern.pattern,
                )
                if not company_names:
                    company_names = [None]
                item_id = self._find_nearest_item_id(searchable_text, match.start(), item_id_pattern)
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
        cleaned = re.sub(r"\s+", " ", value).strip(" ,.;:-")
        cleaned = re.sub(
            r"\b(?:for a term|subject to|in an amount|which includes|beginning on|for the period).*$",
            "",
            cleaned,
            flags=re.IGNORECASE,
        ).strip(" ,.;:-")
        cleaned = re.sub(r"\s*\(Property No.*$", "", cleaned, flags=re.IGNORECASE).strip(" ,.;:-")
        cleaned = re.sub(r"\s+with a contract amount.*$", "", cleaned, flags=re.IGNORECASE).strip(" ,.;:-")
        return cleaned or None

    def _extract_company_names(self, raw_value: str | None, matched_pattern: str) -> list[str | None]:
        cleaned = self._clean_company_name(raw_value)
        if not cleaned:
            return []

        if not self._pattern_supports_multiple_companies(matched_pattern):
            return [] if self._should_reject_company_name(cleaned) else [cleaned]

        normalized = re.sub(r"\s+(?:and|&)\s+", ", ", cleaned, flags=re.IGNORECASE)
        parts = [self._clean_company_name(part) for part in normalized.split(",")]
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

    def _should_reject_company_name(self, company_name: str | None) -> bool:
        if not company_name:
            return False

        lowered = company_name.lower()
        bad_prefixes = (
            "the ihcp",
            "the commission",
            "the county",
            "the board",
            "the district",
            "the proposed",
        )
        if lowered.startswith(bad_prefixes):
            return True

        bad_fragments = (
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

    def _deduplicate(self, candidates: list[AwardCandidate]) -> list[AwardCandidate]:
        unique: dict[tuple[str | None, str | None, str | None], AwardCandidate] = {}
        for candidate in candidates:
            normalized_company = candidate.company_name.lower() if candidate.company_name else None
            key = (candidate.source_url, candidate.item_id, normalized_company)
            existing = unique.get(key)
            if existing is None or len(candidate.snippet) > len(existing.snippet):
                unique[key] = candidate

        return list(unique.values())

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
    print(json.dumps(summary, indent=2))

    if args.print_warnings and warnings:
        for warning in warnings:
            print(warning)


if __name__ == "__main__":
    main()
