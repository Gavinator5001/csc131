from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from openpyxl import load_workbook


SHEET_TARGETS = {
    "a1": {
        "sheet_patterns": [r"schedule\s*a1\b", r"schedule\s*a-?1\b"],
        "type": "investment",
        "name_aliases": [
            "name of business entity",
        ],
    },
    "a2": {
        "sheet_patterns": [r"schedule\s*a-?2\b"],
        "type": "investment_or_entity",
        "name_aliases": [
            "name of business entity or trust",
            "name of business entity",
            "investment",
            "business entity/name",
        ],
    },
    "b": {
        "sheet_patterns": [r"schedule\s*b\b"],
        "type": "lender_or_property",
        "name_aliases": [
            "name of lender",
        ],
    },
    "c": {
        "sheet_patterns": [r"schedule\s*c\b"],
        "type": "income_source",
        "name_aliases": [
            "name of source",
        ],
    },
}

NON_DATA_VALUES = {
    "", "n/a", "na", "none", "not applicable",
    "schedule attached", "continued", "continued on next page"
}


def norm_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    return re.sub(r"\s+", " ", text)


def norm_key(value):
    text = norm_text(value).lower().replace("&", "and")
    text = re.sub(r"[^a-z0-9\s\-/]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def find_sheet(wb, target_key):
    cfg = SHEET_TARGETS[target_key]
    for ws in wb.worksheets:
        title = norm_key(ws.title)
        for pattern in cfg["sheet_patterns"]:
            if re.search(pattern, title):
                return ws
    return None


def score_header_row(row_values, aliases):
    keys = [norm_key(v) for v in row_values if norm_text(v)]
    score = 0
    for alias in aliases:
        alias_key = norm_key(alias)
        if alias_key in keys:
            score += 3
    return score


def detect_header(ws, aliases, max_scan_rows=20):
    best_row, best_score, best_headers = None, -1, None
    for r in range(1, min(ws.max_row, max_scan_rows) + 1):
        values = [ws.cell(r, c).value for c in range(1, ws.max_column + 1)]
        score = score_header_row(values, aliases)
        if score > best_score:
            best_row, best_score, best_headers = r, score, values
    if best_row is None or best_score <= 0:
        raise RuntimeError(f"Could not detect header row in sheet '{ws.title}'")
    return best_row, [norm_text(v) for v in best_headers]


def build_header_index(headers):
    idx = {}
    for i, h in enumerate(headers, start=1):
        key = norm_key(h)
        if key and key not in idx:
            idx[key] = i
    return idx


def pick_name_column(header_index, aliases):
    alias_keys = [norm_key(a) for a in aliases]
    for alias in alias_keys:
        if alias in header_index:
            return header_index[alias], alias
    return None, None


def row_to_dict(ws, row_num, headers):
    out = {}
    for i, header in enumerate(headers, start=1):
        out[header or f"column_{i}"] = norm_text(ws.cell(row_num, i).value)
    return out


def is_probable_data_row(values):
    nonempty = [v for v in values if norm_text(v)]
    return len(nonempty) >= 2


def extract_form700_owner(wb):
    ws = wb.worksheets[0]
    owner_last_name = norm_text(ws.cell(1, 1).value)
    owner_first_name = norm_text(ws.cell(1, 2).value)
    owner_full_name = f"{owner_first_name} {owner_last_name}".strip()
    print(f"[info] owner from workbook cols 1-2: {owner_full_name or '(blank)'}")
    return {
        "owner_last_name": owner_last_name,
        "owner_first_name": owner_first_name,
        "owner_full_name": owner_full_name,
    }


def extract_sheet_records(ws, target_key, owner_meta):
    cfg = SHEET_TARGETS[target_key]
    header_row, headers = detect_header(ws, cfg["name_aliases"])
    header_index = build_header_index(headers)
    name_col, matched_header = pick_name_column(header_index, cfg["name_aliases"])
    if not name_col:
        raise RuntimeError(f"Could not identify strict name column in sheet '{ws.title}'")
    print(f"[info] using entity column '{matched_header}' in sheet '{ws.title}'")

    records = []
    for r in range(header_row + 1, ws.max_row + 1):
        values = [ws.cell(r, c).value for c in range(1, ws.max_column + 1)]
        text_values = [norm_text(v) for v in values]
        if not any(text_values):
            continue
        if not is_probable_data_row(text_values):
            continue
        raw_name = norm_text(ws.cell(r, name_col).value)
        if norm_key(raw_name) in NON_DATA_VALUES or not raw_name:
            continue

        rec = row_to_dict(ws, r, headers)
        rec["_sheet"] = ws.title
        rec["_schedule"] = target_key.upper().replace("A2", "A-2")
        rec["_record_type"] = cfg["type"]
        rec["_name_column"] = matched_header
        rec["_row_number"] = r
        rec["entity_name"] = raw_name
        rec["owner_last_name"] = owner_meta["owner_last_name"]
        rec["owner_first_name"] = owner_meta["owner_first_name"]
        rec["owner_full_name"] = owner_meta["owner_full_name"]
        records.append(rec)
    return records


def parse_form700_workbook(input_path: str | Path):
    input_path = Path(input_path)
    print(f"[info] opening Form 700 workbook: {input_path}")
    wb = load_workbook(input_path, data_only=True, read_only=True)
    print(f"[info] workbook loaded with {len(wb.worksheets)} sheets")

    owner_meta = extract_form700_owner(wb)

    all_records = []
    for target_key in ["a1", "a2", "b", "c"]:
        schedule_label = target_key.upper().replace("A2", "A-2")
        print(f"[info] looking for schedule {schedule_label}")
        ws = find_sheet(wb, target_key)

        if ws is None:
            print(f"[info] schedule {schedule_label} not found")
            continue

        print(f"[info] found sheet: {ws.title}")
        rows = extract_sheet_records(ws, target_key, owner_meta)
        print(f"[info] extracted {len(rows)} rows from {ws.title}")
        all_records.extend(rows)

    print(f"[info] Form 700 workbook parsing complete: {len(all_records)} total rows")
    return all_records


def write_outputs(records, out_csv: str | Path, out_json: str | Path):
    out_csv = Path(out_csv)
    out_json = Path(out_json)

    print(f"[info] writing Form 700 CSV output: {out_csv}")
    print(f"[info] writing Form 700 JSON output: {out_json}")

    fieldnames = []
    seen = set()
    for row in records:
        for k in row.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    out_json.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[info] finished writing Form 700 outputs")
