#
# Copyright 2026 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Postgres reader for run/job statistics."""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import cached_property
from pathlib import Path
from typing import Any

import aiosql
from botocore.exceptions import BotoCoreError, ClientError

from src.utils.constants import (
    POSTGRES_DEFAULT_LIMIT,
    POSTGRES_DEFAULT_WINDOW_MS,
    POSTGRES_MAX_LIMIT,
    POSTGRES_STATEMENT_TIMEOUT_MS,
    REQUIRED_CONNECTION_FIELDS,
)
from src.utils.postgres_base import PsycopgError, PostgresBase

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).parent / "sql"


@dataclass(frozen=True)
class ReaderQueries:
    """Typed holder for reader SQL query strings loaded via aiosql."""

    get_stats: str
    get_stats_with_cursor: str


class ReaderPostgres(PostgresBase):
    """Read-only Postgres accessor for run/job statistics."""

    def __init__(self) -> None:
        super().__init__()
        logger.debug("Initialized PostgreSQL reader.")

    def _connect_options(self) -> str | None:
        """Set statement timeout and read-only mode for reader connections."""
        return f"-c statement_timeout={POSTGRES_STATEMENT_TIMEOUT_MS}" " -c default_transaction_read_only=on"

    @cached_property
    def _queries(self) -> ReaderQueries:
        """Load SQL queries from the `sql/` directory via aiosql."""
        queries = aiosql.from_path(_SQL_DIR, "psycopg2")
        return ReaderQueries(
            get_stats=queries.get_stats.sql,
            get_stats_with_cursor=queries.get_stats_with_cursor.sql,
        )

    def read_stats(
        self,
        timestamp_start: int | None = None,
        timestamp_end: int | None = None,
        cursor: int | None = None,
        limit: int = POSTGRES_DEFAULT_LIMIT,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Query run/job statistics with keyset pagination.
        Args:
            timestamp_start: Start of time window in epoch milliseconds.
                Defaults to *now - 7 days*.
            timestamp_end: End of time window in epoch milliseconds.
                Defaults to *now*.
            cursor: Last `internal_id` from previous page (keyset pagination).
            limit: Maximum number of rows per page.
        Returns:
            Tuple of (rows, pagination), where each row is a dict with
            raw and computed columns and pagination contains cursor info.
        Raises:
            RuntimeError: On database connectivity or query errors.
        """
        config = self._pg_config
        if not config.get("database"):
            raise RuntimeError("PostgreSQL config missing: database.")
        if not all(config.get(field) for field in REQUIRED_CONNECTION_FIELDS):
            missing = [field for field in REQUIRED_CONNECTION_FIELDS if not config.get(field)]
            raise RuntimeError(f"PostgreSQL config missing: {', '.join(missing)}.")

        limit = max(1, min(limit, POSTGRES_MAX_LIMIT))
        now_ms = int(time.time() * 1000)
        ts_start = timestamp_start if timestamp_start is not None else (now_ms - POSTGRES_DEFAULT_WINDOW_MS)
        ts_end = timestamp_end if timestamp_end is not None else now_ms

        try:
            col_names, raw_rows = self._execute_with_retry(
                lambda conn: self._run_stats_query(conn, ts_start, ts_end, cursor, limit)
            )
        except PsycopgError as exc:
            self._close_connection()
            raise RuntimeError(f"Database query error: {exc}") from exc

        rows = [dict(zip(col_names, row, strict=True)) for row in raw_rows]

        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]

        next_cursor: int | None = None
        if has_more and rows:
            next_cursor = rows[-1]["internal_id"]

        rows = [self._format_row(row) for row in rows]

        pagination: dict[str, Any] = {
            "cursor": next_cursor,
            "has_more": has_more,
            "limit": limit,
        }

        logger.debug("Stats query returned %d rows.", len(rows))
        return rows, pagination

    def _run_stats_query(
        self,
        connection: Any,
        ts_start: int,
        ts_end: int,
        cursor: int | None,
        limit: int,
    ) -> tuple[list[str], list[tuple[Any, ...]]]:
        """Execute the stats SQL query and return column names and raw rows."""
        try:
            with connection.cursor() as db_cursor:
                if cursor is not None:
                    db_cursor.execute(
                        self._queries.get_stats_with_cursor,
                        {"ts_start": ts_start, "ts_end": ts_end, "cursor_id": cursor, "lim": limit + 1},
                    )
                else:
                    db_cursor.execute(
                        self._queries.get_stats,
                        {"ts_start": ts_start, "ts_end": ts_end, "lim": limit + 1},
                    )
                if db_cursor.description is None:
                    raise RuntimeError("Stats query returned no result description.")
                col_names = [desc[0] for desc in db_cursor.description]
                raw_rows = db_cursor.fetchall()
        finally:
            # Rollback closes the implicit transaction opened by the SELECT,
            # leaving the cached connection in a clean idle state for reuse.
            try:
                connection.rollback()
            except PsycopgError:
                logger.debug("Failed to close the implicit transaction. Closing cached connection.", exc_info=True)
                self._close_connection()
        return col_names, raw_rows

    @staticmethod
    def _format_row(row: dict[str, Any]) -> dict[str, Any]:
        """Add computed columns to a result row.
        Computed fields mirror the Qlik load-script transformations:
        `run_date`, `run_status`, `formatted_tenant`, `elapsed_time`,
        `start_time`, `end_time`.
        Args:
            row: Raw database row dict.
        Returns:
            Row dict enriched with computed fields.
        """
        run_ts_start = row.get("run_timestamp_start")
        ts_start = row.get("timestamp_start")
        ts_end = row.get("timestamp_end")

        # run_date: date portion of run-level timestamp_start (DD-MM-YYYY).
        if run_ts_start is not None:
            row["run_date"] = datetime.fromtimestamp(run_ts_start / 1000, tz=timezone.utc).strftime("%d-%m-%Y")
        else:
            row["run_date"] = None

        # run_status: derived from job status + message.
        status = str(row.get("status", "")).lower()
        message = str(row.get("message", "")).lower()
        if status == "failed":
            if "no data" in message:
                row["run_status"] = "no data received"
            elif "no records to send" in message:
                row["run_status"] = "no data produced"
            else:
                row["run_status"] = "failed"
        else:
            row["run_status"] = "succeeded"

        # formatted_tenant: lowercase tenant_id.
        row["formatted_tenant"] = str(row.get("tenant_id", "")).lower()

        # elapsed_time: difference in milliseconds between job end and start.
        if ts_start is not None and ts_end is not None:
            row["elapsed_time"] = ts_end - ts_start
        else:
            row["elapsed_time"] = None

        # start_time / end_time: formatted job timestamps (UTC).
        if ts_start is not None:
            row["start_time"] = datetime.fromtimestamp(ts_start / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        else:
            row["start_time"] = None

        if ts_end is not None:
            row["end_time"] = datetime.fromtimestamp(ts_end / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        else:
            row["end_time"] = None

        return row

    def check_health(self) -> tuple[bool, str]:
        """Check PostgreSQL reader health.
        Returns:
            Tuple of (is_healthy, message).
        """
        if not self._secret_name or not self._secret_region:
            return False, "postgres secret not configured"

        try:
            pg_config = self._pg_config
        except (BotoCoreError, ClientError, RuntimeError, ValueError, KeyError) as err:
            return False, str(err)

        if not pg_config.get("database"):
            return False, "database not configured"

        missing = [field for field in REQUIRED_CONNECTION_FIELDS if not pg_config.get(field)]
        if missing:
            return False, f"{missing[0]} not configured"

        return True, "ok"
