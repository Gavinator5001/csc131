from __future__ import annotations

import hashlib
import io
import json
import re
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import urlparse, parse_qs

import requests
from pypdf import PdfReader


def safe_meeting_cache_key(meeting_date: str, body: str, minutes_url: str) -> str:
    qs = parse_qs(urlparse(minutes_url).query)
    file_id = qs.get("ID", [""])[0]
    guid = qs.get("GUID", [""])[0]
    if file_id:
        raw = f"{meeting_date}|{body}|{file_id}|{guid}"
    else:
        raw = f"{meeting_date}|{body}|{minutes_url}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def fetch_pdf_text(
    url: str,
    session: requests.Session | None = None,
    cache_dir: str | Path = "minutes_cache",
) -> str:
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    url_hash = hashlib.sha1(url.encode("utf-8")).hexdigest()
    text_path = cache_dir / f"{url_hash}.txt"
    pdf_path = cache_dir / f"{url_hash}.pdf"

    if text_path.exists():
        print(f"[info] using cached minutes text: {text_path}")
        return text_path.read_text(encoding="utf-8", errors="ignore")

    s = session or requests.Session()
    print(f"[info] downloading minutes pdf: {url}")
    resp = s.get(url, timeout=60)
    resp.raise_for_status()

    pdf_path.write_bytes(resp.content)
    print(f"[info] cached minutes pdf: {pdf_path}")

    reader = PdfReader(io.BytesIO(resp.content))
    parts = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    text = "\n".join(parts)

    text_path.write_text(text, encoding="utf-8")
    print(f"[info] cached minutes text: {text_path}")
    return text


def normalize_person_token(name: str) -> str:
    s = (name or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def canonical_person_name(name: str) -> str:
    s = normalize_person_token(name).lower()
    s = re.sub(r"[^a-z\.\-'\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def build_allowed_politician_names(form700_owner_rows):
    allowed = set()

    for row in form700_owner_rows:
        full = canonical_person_name(row.get("owner_full_name", ""))
        last = canonical_person_name(row.get("owner_last_name", ""))

        if full:
            allowed.add(full)
        if last:
            allowed.add(last)

    return allowed

BAD_EXACT_NAMES = {
    "none",
    "ne",
    "n e",
    "n/a",
    "na",
    "unknown",
    "present",
    "absent",
    "ayes",
    "aye",
    "no",
    "noes",
    "abstain",
    "abstained",
    "ABS",
    "IF",
}

BAD_NAME_FRAGMENTS = {
    "approved",
    "recommended",
    "resolution",
    "ordinance",
    "notice",
    "exemption",
    "summary",
    "report",
    "attachment",
    "board action",
    "public comment",
}
def matches_allowed_politician(candidate_name: str, allowed_names: set[str]) -> bool:
    candidate = canonical_person_name(candidate_name)
    return bool(candidate) and candidate in allowed_names

def build_exact_name_matchers(politician: str):
    full = canonical_person_name(politician)
    parts = [p for p in full.split() if p]
    last = parts[-1] if parts else ""
    return full, last


def matches_exact_full_or_last(candidate_name: str, target_full: str, target_last: str) -> bool:
    candidate = canonical_person_name(candidate_name)
    if not candidate:
        return False
    return candidate == target_full or candidate == target_last


def looks_like_name_list_line(ln: str) -> bool:
    s = (ln or "").strip()
    if not s:
        return False

    lower = s.lower()
    bad_phrases = [
        "approved as",
        "recommended",
        "resolution",
        "ordinance",
        "informational only",
        "board action",
        "department or agency",
        "summary report",
        "attachments",
        "attachment",
        "presenters",
        "speakers",
        "public comment",
        "moved by",
        "seconded by",
        "adopt a resolution",
        "adopt an ordinance",
    ]
    if any(p in lower for p in bad_phrases):
        return False

    if re.fullmatch(r"\d{4}", s):
        return False
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2,4}", s):
        return False
    if re.fullmatch(r"\d{4}-\d{3,}", s):
        return False
    if re.fullmatch(r"\d{2}-\d{3,}", s):
        return False
    if re.search(r"\b\d+\b", s) and not re.search(r"[A-Za-z]", s):
        return False

    if len(s) > 120:
        return False

    return bool(re.fullmatch(r"[A-Za-z\.\-'\s,;&]+", s))


def looks_like_person_name(name: str) -> bool:
    raw = normalize_person_token(name)
    canon = canonical_person_name(name)

    if not raw or not canon:
        return False

    if canon in BAD_EXACT_NAMES:
        return False

    if any(fragment in canon for fragment in BAD_NAME_FRAGMENTS):
        return False

    if re.search(r"\d", raw):
        return False

    parts_raw = [p.strip(",;") for p in raw.split() if p.strip(",;")]
    parts_canon = [p for p in canon.split() if p]

    if not parts_canon or len(parts_canon) > 4:
        return False

    if len(parts_raw) == 1:
        token = parts_raw[0]
        if token.lower() in BAD_EXACT_NAMES:
            return False
        return bool(re.fullmatch(r"[A-Z][a-zA-Z'\-]+", token))

    for token in parts_raw:
        if token.lower() in BAD_EXACT_NAMES:
            return False
        if re.fullmatch(r"[A-Z]\.", token):
            continue
        if not re.fullmatch(r"[A-Z][a-zA-Z'\-]+", token):
            return False

    return True


def extract_vote_rows_from_minutes_text(
    text: str,
    meeting_date: str = "",
    body: str = "",
    minutes_url: str = "",
    minutes_cache_key: str = "",
):
    rows = []
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines() if ln.strip()]

    current_matter_id = ""
    current_title = ""
    current_result = ""
    current_vote_bucket = None
    current_vote_names = []

    matter_pat = re.compile(r"\b(File No\.?|File #|File)\s*([A-Z0-9\-]+)", re.I)
    result_pat = re.compile(r"\b(ADOPTED|FINALLY PASSED|HEARD AND FILED|CONTINUED|PASSED|APPROVED|FAILED)\b", re.I)
    vote_pat = re.compile(r"^(Aye|Ayes|No|Noes|Absent|Abstain|Abstained)\s*[:\-]?\s*(.*)$", re.I)
    next_bucket_pat = re.compile(r"^(Aye|Ayes|No|Noes|Absent|Abstain|Abstained)\b", re.I)

    def normalize_bucket(bucket: str) -> str:
        b = bucket.lower()
        if b in {"aye", "ayes"}:
            return "Ayes"
        if b in {"no", "noes"}:
            return "Noes"
        if b in {"abstain", "abstained"}:
            return "Abstain"
        return "Absent"

    def flush_vote_bucket():
        nonlocal current_vote_bucket, current_vote_names
        if not current_vote_bucket:
            current_vote_names = []
            return

        joined = " ".join(current_vote_names).strip()
        if not joined:
            current_vote_bucket = None
            current_vote_names = []
            return

        names = [n.strip() for n in re.split(r",|;| and ", joined) if n.strip()]
        for name in names:
            if not looks_like_person_name(name):
                continue
            rows.append(
                {
                    "meeting_date": meeting_date,
                    "body": body,
                    "matter_id": current_matter_id,
                    "matter_title": current_title,
                    "result": current_result,
                    "politician_name": name,
                    "vote_bucket": current_vote_bucket,
                    "source_url": minutes_url,
                    "minutes_cache_key": minutes_cache_key,
                }
            )

        current_vote_bucket = None
        current_vote_names = []

    for ln in lines:
        m = matter_pat.search(ln)
        if m:
            flush_vote_bucket()
            current_matter_id = m.group(2).strip()
            current_title = ln

        rm = result_pat.search(ln)
        if rm:
            current_result = rm.group(1).upper()

        vm = vote_pat.match(ln)
        if vm:
            flush_vote_bucket()
            current_vote_bucket = normalize_bucket(vm.group(1))
            rest = vm.group(2).strip()
            current_vote_names = [rest] if rest and looks_like_name_list_line(rest) else []
            continue

        if current_vote_bucket and not next_bucket_pat.match(ln):
            if looks_like_name_list_line(ln):
                current_vote_names.append(ln)
                continue

            flush_vote_bucket()

    flush_vote_bucket()
    return rows


def scrape_votes_for_meetings(
    meetings: Iterable,
    politician: Optional[str] = None,
    allowed_names: Optional[set[str]] = None,
    cache_dir: str | Path = "minutes_cache",
    text_artifacts_path: str | Path = "minutes_text_index.json",
) -> List[dict]:
    out: List[dict] = []
    session = requests.Session()

    target_full, target_last = ("", "")
    if politician:
        target_full, target_last = build_exact_name_matchers(politician)

    total_rows_parsed = 0
    text_index = {}

    for meeting_num, meeting in enumerate(meetings, start=1):
        minutes_links = [lnk for lnk in meeting.links if lnk.label.lower() == "minutes"]
        if not minutes_links:
            continue

        print(f"[info] parsing meeting {meeting_num}: {meeting.meeting_date} | {meeting.body}")

        for link in minutes_links:
            try:
                text = fetch_pdf_text(link.url, session=session, cache_dir=cache_dir)
            except Exception as e:
                print(f"[info] failed to parse minutes PDF: {e}")
                continue

            cache_key = safe_meeting_cache_key(meeting.meeting_date or "", meeting.body or "", link.url)
            url_hash = hashlib.sha1(link.url.encode("utf-8")).hexdigest()
            text_index[cache_key] = {
                "meeting_date": meeting.meeting_date or "",
                "body": meeting.body or "",
                "minutes_url": link.url,
                "text_path": str(Path(cache_dir) / f"{url_hash}.txt"),
                "pdf_path": str(Path(cache_dir) / f"{url_hash}.pdf"),
            }

            rows = extract_vote_rows_from_minutes_text(
                text,
                meeting_date=meeting.meeting_date or "",
                body=meeting.body or "",
                minutes_url=link.url,
                minutes_cache_key=cache_key,
            )

            total_rows_parsed += len(rows)
            if total_rows_parsed and total_rows_parsed % 1000 == 0:
                print(f"[info] vote row parsing progress: {total_rows_parsed} rows parsed")

            if not politician and not allowed_names:
                out.extend(rows)
                continue

            for row in rows:
                candidate = row.get("politician_name", "")

                if allowed_names is not None:
                    if matches_allowed_politician(candidate, allowed_names):
                        out.append(row)
                    continue

                if matches_exact_full_or_last(candidate, target_full, target_last):
                    out.append(row)

    Path(text_artifacts_path).write_text(json.dumps(text_index, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[info] wrote minutes text index: {text_artifacts_path}")
    print(f"[info] vote parsing complete: {total_rows_parsed} rows parsed total")
    return out
