
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class MeetingLink:
    label: str
    url: str
    kind: str = "html"


@dataclass
class MeetingRecord:
    jurisdiction: str
    platform: str
    body: str
    meeting_title: str
    meeting_date: Optional[str]
    meeting_url: str
    links: List[MeetingLink] = field(default_factory=list)
