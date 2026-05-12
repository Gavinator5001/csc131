
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


@dataclass
class Form700FilingRecord:
    jurisdiction: str
    portal: str
    filer_last_name: str
    filer_first_name: str
    filer_middle_name: str = ""
    filer_full_name: str = ""
    position_title: str = ""
    agency_name: str = ""
    entity_name: str = ""
    form_description: str = ""
    filing_year: str = ""
    due_date: str = ""
    filed_date: str = ""
    filing_type: str = ""
    page_count: str = ""
    forms_page_url: str = ""
    view_form_url: str = ""
    download_form_url: str = ""
