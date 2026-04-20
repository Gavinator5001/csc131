from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class MeetingLink:
    label: str
    url: str
    kind: str = "other"


@dataclass
class MeetingRecord:
    jurisdiction: str
    platform: str
    body: str
    meeting_title: str
    meeting_date: Optional[str]
    meeting_url: str
    links: List[MeetingLink] = field(default_factory=list)


@dataclass
class VoteRecord:
    jurisdiction: str
    platform: str
    body: str
    meeting_date: Optional[str]
    meeting_title: str
    item_number: str
    matter_id: str
    matter_title: str
    motion_text: str
    result: str
    member_name: str
    vote: str
    source_url: str
    source_type: str
    confidence: float
    snippet: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["extra"] = dict(sorted(data["extra"].items()))
        return data
