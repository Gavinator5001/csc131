from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class MinutesDatabase:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS minutes_files (
                    minutes_cache_key TEXT PRIMARY KEY,
                    jurisdiction TEXT NOT NULL DEFAULT '',
                    platform TEXT NOT NULL DEFAULT '',
                    body TEXT NOT NULL DEFAULT '',
                    meeting_title TEXT NOT NULL DEFAULT '',
                    meeting_date TEXT NOT NULL DEFAULT '',
                    meeting_url TEXT NOT NULL DEFAULT '',
                    minutes_url TEXT NOT NULL UNIQUE,
                    pdf_path TEXT NOT NULL DEFAULT '',
                    text_path TEXT NOT NULL DEFAULT '',
                    content_sha1 TEXT NOT NULL DEFAULT '',
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    downloaded_at TEXT,
                    parsed_at TEXT,
                    parse_status TEXT NOT NULL DEFAULT 'discovered',
                    error_message TEXT NOT NULL DEFAULT '',
                    vote_row_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS vote_rows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    minutes_cache_key TEXT NOT NULL,
                    meeting_date TEXT NOT NULL DEFAULT '',
                    body TEXT NOT NULL DEFAULT '',
                    matter_id TEXT NOT NULL DEFAULT '',
                    matter_title TEXT NOT NULL DEFAULT '',
                    result TEXT NOT NULL DEFAULT '',
                    politician_name TEXT NOT NULL DEFAULT '',
                    vote_bucket TEXT NOT NULL DEFAULT '',
                    source_url TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(minutes_cache_key)
                        REFERENCES minutes_files(minutes_cache_key)
                        ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_minutes_files_url
                    ON minutes_files(minutes_url);
                CREATE INDEX IF NOT EXISTS idx_minutes_files_status
                    ON minutes_files(parse_status, parsed_at);
                CREATE INDEX IF NOT EXISTS idx_vote_rows_minutes
                    ON vote_rows(minutes_cache_key);
                CREATE INDEX IF NOT EXISTS idx_vote_rows_person
                    ON vote_rows(politician_name);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def upsert_discovered_minutes(self, meeting, link, minutes_cache_key: str) -> tuple[dict, bool]:
        now = utc_now()
        existing = self.get_minutes_by_url(link.url)
        if existing:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE minutes_files
                    SET jurisdiction = ?,
                        platform = ?,
                        body = ?,
                        meeting_title = ?,
                        meeting_date = ?,
                        meeting_url = ?,
                        last_seen_at = ?
                    WHERE minutes_url = ?
                    """,
                    (
                        meeting.jurisdiction or "",
                        meeting.platform or "",
                        meeting.body or "",
                        meeting.meeting_title or "",
                        meeting.meeting_date or "",
                        meeting.meeting_url or "",
                        now,
                        link.url,
                    ),
                )
            return self.get_minutes_by_url(link.url) or existing, False

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO minutes_files (
                    minutes_cache_key,
                    jurisdiction,
                    platform,
                    body,
                    meeting_title,
                    meeting_date,
                    meeting_url,
                    minutes_url,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    minutes_cache_key,
                    meeting.jurisdiction or "",
                    meeting.platform or "",
                    meeting.body or "",
                    meeting.meeting_title or "",
                    meeting.meeting_date or "",
                    meeting.meeting_url or "",
                    link.url,
                    now,
                    now,
                ),
            )
        row = self.get_minutes_by_url(link.url)
        return row or {}, True

    def get_minutes_by_url(self, minutes_url: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM minutes_files WHERE minutes_url = ?",
                (minutes_url,),
            ).fetchone()
        return dict(row) if row else None

    def record_download(
        self,
        minutes_cache_key: str,
        *,
        pdf_path: str | Path,
        text_path: str | Path,
        content_sha1: str,
    ) -> None:
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE minutes_files
                SET pdf_path = ?,
                    text_path = ?,
                    content_sha1 = ?,
                    downloaded_at = COALESCE(downloaded_at, ?),
                    parse_status = 'downloaded',
                    error_message = ''
                WHERE minutes_cache_key = ?
                """,
                (
                    str(pdf_path),
                    str(text_path),
                    content_sha1,
                    now,
                    minutes_cache_key,
                ),
            )

    def record_parse_success(self, minutes_cache_key: str, rows: Iterable[dict]) -> None:
        rows = list(rows)
        now = utc_now()
        with self._connect() as conn:
            conn.execute("DELETE FROM vote_rows WHERE minutes_cache_key = ?", (minutes_cache_key,))
            conn.executemany(
                """
                INSERT INTO vote_rows (
                    minutes_cache_key,
                    meeting_date,
                    body,
                    matter_id,
                    matter_title,
                    result,
                    politician_name,
                    vote_bucket,
                    source_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        minutes_cache_key,
                        row.get("meeting_date", ""),
                        row.get("body", ""),
                        row.get("matter_id", ""),
                        row.get("matter_title", ""),
                        row.get("result", ""),
                        row.get("politician_name", ""),
                        row.get("vote_bucket", ""),
                        row.get("source_url", ""),
                    )
                    for row in rows
                ],
            )
            conn.execute(
                """
                UPDATE minutes_files
                SET parsed_at = ?,
                    parse_status = 'parsed',
                    error_message = '',
                    vote_row_count = ?
                WHERE minutes_cache_key = ?
                """,
                (now, len(rows), minutes_cache_key),
            )

    def record_parse_error(self, minutes_cache_key: str, error: Exception | str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE minutes_files
                SET parse_status = 'error',
                    error_message = ?
                WHERE minutes_cache_key = ?
                """,
                (str(error), minutes_cache_key),
            )

    def fetch_vote_rows(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    vote_rows.meeting_date,
                    vote_rows.body,
                    vote_rows.matter_id,
                    vote_rows.matter_title,
                    vote_rows.result,
                    vote_rows.politician_name,
                    vote_rows.vote_bucket,
                    vote_rows.source_url,
                    vote_rows.minutes_cache_key
                FROM vote_rows
                JOIN minutes_files
                    ON minutes_files.minutes_cache_key = vote_rows.minutes_cache_key
                ORDER BY
                    minutes_files.meeting_date DESC,
                    vote_rows.matter_id,
                    vote_rows.politician_name
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def build_text_index(self) -> dict:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    minutes_cache_key,
                    meeting_date,
                    body,
                    minutes_url,
                    text_path,
                    pdf_path
                FROM minutes_files
                WHERE text_path != ''
                ORDER BY meeting_date DESC, body
                """
            ).fetchall()
        return {
            row["minutes_cache_key"]: {
                "meeting_date": row["meeting_date"],
                "body": row["body"],
                "minutes_url": row["minutes_url"],
                "text_path": row["text_path"],
                "pdf_path": row["pdf_path"],
            }
            for row in rows
        }

    def count_minutes(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM minutes_files").fetchone()
        return int(row["total"])

    def count_vote_rows(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM vote_rows").fetchone()
        return int(row["total"])
