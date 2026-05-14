from __future__ import annotations

import sqlite3
from contextlib import contextmanager
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
        with self._connection() as conn:
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

                CREATE TABLE IF NOT EXISTS form700_filings (
                    form700_cache_key TEXT PRIMARY KEY,
                    jurisdiction TEXT NOT NULL DEFAULT '',
                    portal TEXT NOT NULL DEFAULT '',
                    filer_last_name TEXT NOT NULL DEFAULT '',
                    filer_first_name TEXT NOT NULL DEFAULT '',
                    filer_middle_name TEXT NOT NULL DEFAULT '',
                    filer_full_name TEXT NOT NULL DEFAULT '',
                    position_title TEXT NOT NULL DEFAULT '',
                    agency_name TEXT NOT NULL DEFAULT '',
                    entity_name TEXT NOT NULL DEFAULT '',
                    form_description TEXT NOT NULL DEFAULT '',
                    filing_year TEXT NOT NULL DEFAULT '',
                    due_date TEXT NOT NULL DEFAULT '',
                    filed_date TEXT NOT NULL DEFAULT '',
                    filing_type TEXT NOT NULL DEFAULT '',
                    page_count TEXT NOT NULL DEFAULT '',
                    forms_page_url TEXT NOT NULL DEFAULT '',
                    view_form_url TEXT NOT NULL DEFAULT '',
                    download_form_url TEXT NOT NULL DEFAULT '',
                    pdf_path TEXT NOT NULL DEFAULT '',
                    content_sha1 TEXT NOT NULL DEFAULT '',
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    downloaded_at TEXT,
                    parsed_at TEXT,
                    parse_status TEXT NOT NULL DEFAULT 'discovered',
                    error_message TEXT NOT NULL DEFAULT '',
                    entity_row_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS form700_entities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    form700_cache_key TEXT NOT NULL,
                    jurisdiction TEXT NOT NULL DEFAULT '',
                    owner_last_name TEXT NOT NULL DEFAULT '',
                    owner_first_name TEXT NOT NULL DEFAULT '',
                    owner_middle_name TEXT NOT NULL DEFAULT '',
                    owner_full_name TEXT NOT NULL DEFAULT '',
                    filer_position_title TEXT NOT NULL DEFAULT '',
                    filer_agency_name TEXT NOT NULL DEFAULT '',
                    filer_entity_name TEXT NOT NULL DEFAULT '',
                    schedule TEXT NOT NULL DEFAULT '',
                    record_type TEXT NOT NULL DEFAULT '',
                    entity_name TEXT NOT NULL DEFAULT '',
                    raw_value TEXT NOT NULL DEFAULT '',
                    source_pdf_path TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(form700_cache_key)
                        REFERENCES form700_filings(form700_cache_key)
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
                CREATE INDEX IF NOT EXISTS idx_form700_filings_owner
                    ON form700_filings(jurisdiction, filer_full_name, filer_last_name);
                CREATE INDEX IF NOT EXISTS idx_form700_filings_status
                    ON form700_filings(parse_status, parsed_at);
                CREATE INDEX IF NOT EXISTS idx_form700_entities_owner
                    ON form700_entities(jurisdiction, owner_full_name, owner_last_name);
                CREATE INDEX IF NOT EXISTS idx_form700_entities_name
                    ON form700_entities(entity_name);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def _connection(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def upsert_discovered_minutes(self, meeting, link, minutes_cache_key: str) -> tuple[dict, bool]:
        now = utc_now()
        existing = self.get_minutes_by_url(link.url)
        if existing:
            with self._connection() as conn:
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

        with self._connection() as conn:
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
        with self._connection() as conn:
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
        with self._connection() as conn:
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
        with self._connection() as conn:
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
        with self._connection() as conn:
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
        with self._connection() as conn:
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
        with self._connection() as conn:
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

    def fetch_minutes_text_rows(self) -> list[dict]:
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    minutes_cache_key,
                    meeting_date,
                    body,
                    meeting_title,
                    meeting_url,
                    minutes_url,
                    text_path,
                    pdf_path
                FROM minutes_files
                WHERE text_path != ''
                ORDER BY meeting_date DESC, body
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_form700_filing(self, filing: dict) -> tuple[dict, bool]:
        cache_key = filing["form700_cache_key"]
        now = utc_now()
        existing = self.get_form700_filing(cache_key)
        payload = (
            filing.get("jurisdiction", ""),
            filing.get("portal", ""),
            filing.get("filer_last_name", ""),
            filing.get("filer_first_name", ""),
            filing.get("filer_middle_name", ""),
            filing.get("filer_full_name", ""),
            filing.get("position_title", ""),
            filing.get("agency_name", ""),
            filing.get("entity_name", ""),
            filing.get("form_description", ""),
            filing.get("filing_year", ""),
            filing.get("due_date", ""),
            filing.get("filed_date", ""),
            filing.get("filing_type", ""),
            filing.get("page_count", ""),
            filing.get("forms_page_url", ""),
            filing.get("view_form_url", ""),
            filing.get("download_form_url", ""),
        )

        with self._connection() as conn:
            if existing:
                conn.execute(
                    """
                    UPDATE form700_filings
                    SET jurisdiction = ?,
                        portal = ?,
                        filer_last_name = ?,
                        filer_first_name = ?,
                        filer_middle_name = ?,
                        filer_full_name = ?,
                        position_title = ?,
                        agency_name = ?,
                        entity_name = ?,
                        form_description = ?,
                        filing_year = ?,
                        due_date = ?,
                        filed_date = ?,
                        filing_type = ?,
                        page_count = ?,
                        forms_page_url = ?,
                        view_form_url = ?,
                        download_form_url = ?,
                        last_seen_at = ?
                    WHERE form700_cache_key = ?
                    """,
                    payload + (now, cache_key),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO form700_filings (
                        form700_cache_key,
                        jurisdiction,
                        portal,
                        filer_last_name,
                        filer_first_name,
                        filer_middle_name,
                        filer_full_name,
                        position_title,
                        agency_name,
                        entity_name,
                        form_description,
                        filing_year,
                        due_date,
                        filed_date,
                        filing_type,
                        page_count,
                        forms_page_url,
                        view_form_url,
                        download_form_url,
                        first_seen_at,
                        last_seen_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (cache_key,) + payload + (now, now),
                )

        row = self.get_form700_filing(cache_key)
        return row or {}, not bool(existing)

    def get_form700_filing(self, form700_cache_key: str) -> dict | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM form700_filings WHERE form700_cache_key = ?",
                (form700_cache_key,),
            ).fetchone()
        return dict(row) if row else None

    def record_form700_download(
        self,
        form700_cache_key: str,
        *,
        pdf_path: str | Path,
        content_sha1: str,
    ) -> None:
        now = utc_now()
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE form700_filings
                SET pdf_path = ?,
                    content_sha1 = ?,
                    downloaded_at = COALESCE(downloaded_at, ?),
                    parse_status = 'downloaded',
                    error_message = ''
                WHERE form700_cache_key = ?
                """,
                (
                    str(pdf_path),
                    content_sha1,
                    now,
                    form700_cache_key,
                ),
            )

    def update_form700_filing_metadata(self, form700_cache_key: str, updates: dict) -> None:
        allowed = {
            "filer_last_name",
            "filer_first_name",
            "filer_middle_name",
            "filer_full_name",
            "position_title",
            "agency_name",
            "entity_name",
            "form_description",
            "filing_year",
            "due_date",
            "filed_date",
            "filing_type",
            "page_count",
            "pdf_path",
        }
        assignments = []
        values = []
        for key, value in updates.items():
            if key not in allowed:
                continue
            assignments.append(f"{key} = ?")
            values.append(value)

        if not assignments:
            return

        with self._connection() as conn:
            conn.execute(
                f"""
                UPDATE form700_filings
                SET {", ".join(assignments)}
                WHERE form700_cache_key = ?
                """,
                tuple(values) + (form700_cache_key,),
            )

    def record_form700_parse_success(self, form700_cache_key: str, rows: Iterable[dict]) -> None:
        rows = list(rows)
        now = utc_now()
        with self._connection() as conn:
            conn.execute("DELETE FROM form700_entities WHERE form700_cache_key = ?", (form700_cache_key,))
            conn.executemany(
                """
                INSERT INTO form700_entities (
                    form700_cache_key,
                    jurisdiction,
                    owner_last_name,
                    owner_first_name,
                    owner_middle_name,
                    owner_full_name,
                    filer_position_title,
                    filer_agency_name,
                    filer_entity_name,
                    schedule,
                    record_type,
                    entity_name,
                    raw_value,
                    source_pdf_path
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        form700_cache_key,
                        row.get("jurisdiction", ""),
                        row.get("owner_last_name", ""),
                        row.get("owner_first_name", ""),
                        row.get("owner_middle_name", ""),
                        row.get("owner_full_name", ""),
                        row.get("filer_position_title", ""),
                        row.get("filer_agency_name", ""),
                        row.get("filer_entity_name", ""),
                        row.get("_schedule", ""),
                        row.get("_record_type", ""),
                        row.get("entity_name", ""),
                        row.get("raw_value", row.get("entity_name", "")),
                        row.get("_source_pdf_path", ""),
                    )
                    for row in rows
                ],
            )
            conn.execute(
                """
                UPDATE form700_filings
                SET parsed_at = ?,
                    parse_status = 'parsed',
                    error_message = '',
                    entity_row_count = ?
                WHERE form700_cache_key = ?
                """,
                (now, len(rows), form700_cache_key),
            )

    def record_form700_parse_error(self, form700_cache_key: str, error: Exception | str) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE form700_filings
                SET parse_status = 'error',
                    error_message = ?
                WHERE form700_cache_key = ?
                """,
                (str(error), form700_cache_key),
            )

    def fetch_form700_owner_rows(self, jurisdiction: str = "") -> list[dict]:
        where = ""
        params: tuple[str, ...] = ()
        if jurisdiction:
            where = "WHERE jurisdiction = ?"
            params = (jurisdiction,)

        with self._connection() as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT
                    filer_last_name AS owner_last_name,
                    filer_first_name AS owner_first_name,
                    filer_middle_name AS owner_middle_name,
                    filer_full_name AS owner_full_name,
                    position_title AS filer_position_title,
                    agency_name AS filer_agency_name,
                    entity_name AS filer_entity_name,
                    jurisdiction
                FROM form700_filings
                {where}
                ORDER BY filer_last_name, filer_first_name, filed_date DESC
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def fetch_form700_entity_rows(self, jurisdiction: str = "") -> list[dict]:
        where = ""
        params: tuple[str, ...] = ()
        if jurisdiction:
            where = "WHERE form700_entities.jurisdiction = ?"
            params = (jurisdiction,)

        with self._connection() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    form700_entities.owner_last_name,
                    form700_entities.owner_first_name,
                    form700_entities.owner_middle_name,
                    form700_entities.owner_full_name,
                    form700_entities.filer_position_title,
                    form700_entities.filer_agency_name,
                    form700_entities.filer_entity_name,
                    form700_entities.schedule AS _schedule,
                    form700_entities.record_type AS _record_type,
                    form700_entities.entity_name,
                    form700_entities.raw_value,
                    form700_entities.source_pdf_path AS _source_pdf_path,
                    form700_entities.jurisdiction
                FROM form700_entities
                JOIN form700_filings
                    ON form700_filings.form700_cache_key = form700_entities.form700_cache_key
                {where}
                ORDER BY form700_entities.owner_last_name, form700_entities.owner_first_name, form700_entities.entity_name
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def count_minutes(self) -> int:
        with self._connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM minutes_files").fetchone()
        return int(row["total"])

    def count_vote_rows(self) -> int:
        with self._connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM vote_rows").fetchone()
        return int(row["total"])

    def count_form700_filings(self) -> int:
        with self._connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM form700_filings").fetchone()
        return int(row["total"])

    def count_form700_entities(self) -> int:
        with self._connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM form700_entities").fetchone()
        return int(row["total"])

