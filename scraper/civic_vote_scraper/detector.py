from __future__ import annotations

from urllib.parse import urlparse

from civic_vote_scraper.adapters.generic_archive import GenericArchiveAdapter
from civic_vote_scraper.adapters.legistar import LegistarAdapter


def choose_adapter(base_url: str):
    host = urlparse(base_url).netloc.lower()
    if "legistar.com" in host:
        return LegistarAdapter
    return GenericArchiveAdapter
