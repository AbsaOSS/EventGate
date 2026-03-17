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
import os
import time
from datetime import datetime, timezone
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from src.utils.constants import (
    POSTGRES_DEFAULT_LIMIT,
    POSTGRES_DEFAULT_WINDOW_MS,
    POSTGRES_MAX_LIMIT,
)
from src.utils.utils import load_postgres_config

try:
    import psycopg2
    from psycopg2 import Error as PsycopgError
except ImportError:
    psycopg2 = None  # type: ignore

    class PsycopgError(Exception):  # type: ignore
        """Shim psycopg2 error base when psycopg2 is not installed."""


logger = logging.getLogger(__name__)

_RUNS_SQL = (
    "SELECT r.event_id, r.job_ref, r.tenant_id, r.source_app,"
    " r.source_app_version, r.environment,"
    " r.timestamp_start AS run_timestamp_start,"
    " r.timestamp_end AS run_timestamp_end,"
    " j.internal_id, j.country, j.catalog_id, j.status,"
    " j.timestamp_start, j.timestamp_end, j.message, j.additional_info"
    " FROM public_cps_za_runs_jobs j"
    " INNER JOIN public_cps_za_runs r ON j.event_id = r.event_id"
    " WHERE r.timestamp_start >= %s AND r.timestamp_start <= %s"
    "{cursor_condition}"
    " ORDER BY j.internal_id DESC"
    " LIMIT %s"
)


class ReaderPostgres:
    """Read-only Postgres accessor for run/job statistics."""

    def __init__(self) -> None:
        self._secret_name = os.environ.get("POSTGRES_SECRET_NAME", "")
        self._secret_region = os.environ.get("POSTGRES_SECRET_REGION", "")
        self._db_config: dict[str, Any] | None = None
        logger.debug("Initialized PostgreSQL reader.")

    def _load_db_config(self) -> dict[str, Any]:
        """Load database config from AWS Secrets Manager if not already loaded."""
        if self._db_config is None:
            self._db_config = load_postgres_config(self._secret_name, self._secret_region)
        config = self._db_config
        if config is None:
            raise RuntimeError("Failed to load database configuration.")
        return config

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
        db_config = self._load_db_config()
        required_keys = ("database", "host", "user", "password", "port")
        missing_keys = [key for key in required_keys if not db_config.get(key)]
        if missing_keys:
            raise RuntimeError(f"PostgreSQL config missing: {', '.join(missing_keys)}.")
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is not available.")

        limit = max(1, min(limit, POSTGRES_MAX_LIMIT))
        now_ms = int(time.time() * 1000)
        ts_start = timestamp_start if timestamp_start is not None else (now_ms - POSTGRES_DEFAULT_WINDOW_MS)
        ts_end = timestamp_end if timestamp_end is not None else now_ms

        params: list[Any] = [ts_start, ts_end]
        cursor_condition = ""
        if cursor is not None:
            cursor_condition = " AND j.internal_id < %s"
            params.append(cursor)
        params.append(limit + 1)

        sql = _RUNS_SQL.format(cursor_condition=cursor_condition)

        try:
            with psycopg2.connect(  # type: ignore[attr-defined]
                database=db_config["database"],
                host=db_config["host"],
                user=db_config["user"],
                password=db_config["password"],
                port=db_config["port"],
                options="-c statement_timeout=30000 -c default_transaction_read_only=on",
            ) as connection:
                with connection.cursor() as db_cursor:
                    db_cursor.execute(sql, params)
                    col_names = [desc[0] for desc in db_cursor.description]  # type: ignore[union-attr]
                    raw_rows = db_cursor.fetchall()
        except PsycopgError as exc:
            raise RuntimeError(f"Database query failed: {exc}") from exc

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

        # elapsed_time: difference in days between job end and start.
        if ts_start is not None and ts_end is not None:
            row["elapsed_time"] = round((ts_end - ts_start) / 86_400_000)
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
            db_config = self._load_db_config()
        except (BotoCoreError, ClientError, RuntimeError, ValueError, KeyError) as err:
            return False, str(err)

        if not db_config.get("database"):
            return False, "database not configured"

        missing = [f for f in ("host", "user", "password", "port") if not db_config.get(f)]
        if missing:
            return False, f"{missing[0]} not configured"

        return True, "ok"
