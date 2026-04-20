from __future__ import annotations

import re
from typing import Iterable, List

VOTE_WORDS = {
    "aye": "Aye",
    "yes": "Aye",
    "y": "Aye",
    "no": "No",
    "nay": "No",
    "abstain": "Abstain",
    "abstained": "Abstain",
    "recuse": "Recused",
    "recused": "Recused",
    "absent": "Absent",
    "present": "Present",
}


def clean_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_vote_label(value: str) -> str:
    raw = clean_whitespace(value).lower()
    for key, normalized in VOTE_WORDS.items():
        if raw == key or raw.startswith(key + " "):
            return normalized
    return clean_whitespace(value)


def likely_vote_line(text: str) -> bool:
    lowered = text.lower()
    return any(token in lowered for token in ["aye", "no", "abstain", "absent", "recused"])


def politician_name_variants(name: str) -> List[str]:
    name = clean_whitespace(name)
    if not name:
        return []
    parts = name.split()
    variants = {name, name.lower()}
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        variants.update({last, f"{first} {last}", f"{last}, {first}"})
    return [v for v in variants if v]


def contains_name(text: str, name: str) -> bool:
    lowered = text.lower()
    return any(v.lower() in lowered for v in politician_name_variants(name))


def split_candidate_lines(text: str) -> Iterable[str]:
    for line in re.split(r"[\n\r]+", text):
        line = clean_whitespace(line)
        if line:
            yield line
