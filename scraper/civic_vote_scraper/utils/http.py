from __future__ import annotations

import threading
import time
from typing import Dict, Optional

import requests

DEFAULT_HEADERS = {
    "User-Agent": "civic-vote-scraper/0.2",
    "Accept": "text/html,application/json,application/pdf;q=0.9,*/*;q=0.8",
}


class HttpClient:
    def __init__(self, timeout: int = 30, pause: float = 0.2):
        self.timeout = timeout
        self.pause = pause
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self._text_cache: Dict[str, str] = {}
        self._bytes_cache: Dict[str, bytes] = {}
        self._lock = threading.Lock()

    def get(self, url: str, **kwargs) -> requests.Response:
        response = self.session.get(url, timeout=self.timeout, **kwargs)
        response.raise_for_status()
        time.sleep(self.pause)
        return response

    def get_text(self, url: str, encoding: Optional[str] = None) -> str:
        cache_key = f"text::{url}::{encoding or ''}"
        with self._lock:
            if cache_key in self._text_cache:
                return self._text_cache[cache_key]
        response = self.get(url)
        if encoding:
            response.encoding = encoding
        text = response.text
        with self._lock:
            self._text_cache[cache_key] = text
        return text

    def get_bytes(self, url: str) -> bytes:
        with self._lock:
            if url in self._bytes_cache:
                return self._bytes_cache[url]
        content = self.get(url).content
        with self._lock:
            self._bytes_cache[url] = content
        return content
