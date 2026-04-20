from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from civic_vote_scraper.models import MeetingRecord, VoteRecord
from civic_vote_scraper.utils.http import HttpClient


class BaseAdapter(ABC):
    platform_name = "generic"

    def __init__(
        self,
        base_url: str,
        jurisdiction: str,
        http: HttpClient | None = None,
        *,
        body_filter: str = "",
        use_playwright_discovery: bool = False,
        playwright_headless: bool = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.jurisdiction = jurisdiction
        self.http = http or HttpClient()
        self.body_filter = body_filter
        self.use_playwright_discovery = use_playwright_discovery
        self.playwright_headless = playwright_headless

    @abstractmethod
    def discover_meetings(self, limit: int = 25) -> List[MeetingRecord]:
        raise NotImplementedError

    @abstractmethod
    def extract_votes(self, meeting: MeetingRecord, politician: str, html_only: bool = False) -> List[VoteRecord]:
        raise NotImplementedError
