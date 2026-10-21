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
import psycopg2.extensions
from botocore.exceptions import BotoCoreError, ClientError

from src.readers.aggregation_models import AggregationsWindow, RunsJobsAggregations, Timeline, TimelineEntry
from src.readers.named_query_registry import SUPPORTED_AGGREGATION_QUERIES, SUPPORTED_QUERIES
from src.utils.constants import (
    AGGREGATION_BUCKET_THRESHOLD_MS,
    POSTGRES_DEFAULT_LIMIT,
    POSTGRES_DEFAULT_WINDOW_MS,
    POSTGRES_MAX_LIMIT,
    POSTGRES_STATEMENT_TIMEOUT_MS,
    REQUIRED_CONNECTION_FIELDS,
    TIMELINE_BUCKET_DAY,
    TIMELINE_BUCKET_HOUR,
)
from src.utils.postgres_base import PostgresConfig, PsycopgError, PostgresBase
from src.utils.utils import Pagination, QueryRow
from src.writers.writer import HealthCheckError

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).parent / "sql"

type DbRow = tuple[Any, ...]
type RawQueryResult = tuple[list[str], list[DbRow]]
type AggregationQueryResult = tuple[dict[str, Any], list[dict[str, Any]]]


@dataclass(frozen=True)
class ReaderQueries:
    """Typed holder for reader SQL query strings loaded via aiosql."""

    get_stats: str
    get_stats_with_cursor: str
    get_runs_jobs_detail: str
    get_runs_jobs_detail_with_cursor: str
    get_runs_jobs_aggregations: str
    get_runs_jobs_timeline_hourly: str
    get_runs_jobs_timeline_daily: str


class ReaderPostgres(PostgresBase):
    """Read-only Postgres accessor for run/job statistics."""

    def __init__(self) -> None:
        super().__init__()
        logger.debug("Initialized PostgreSQL reader.")

    def _connect_options(self) -> str | None:
        """Set statement timeout and read-only mode for reader connections."""
        return f"-c statement_timeout={POSTGRES_STATEMENT_TIMEOUT_MS} -c default_transaction_read_only=on"

    @cached_property
    def _queries(self) -> ReaderQueries:
        """Load SQL queries from the `sql/` directory via aiosql."""
        queries = aiosql.from_path(_SQL_DIR, "psycopg2")
        # aiosql loads SQL files and attaches query functions as dynamic attributes at runtime.
        # pylint cannot resolve these statically.
        return ReaderQueries(
            get_stats=queries.get_stats.sql,  # pylint: disable=no-member
            get_stats_with_cursor=queries.get_stats_with_cursor.sql,  # pylint: disable=no-member
            get_runs_jobs_detail=queries.get_runs_jobs_detail.sql,  # pylint: disable=no-member
            get_runs_jobs_detail_with_cursor=(
                queries.get_runs_jobs_detail_with_cursor.sql  # pylint: disable=no-member
            ),
            get_runs_jobs_aggregations=queries.get_runs_jobs_aggregations.sql,  # pylint: disable=no-member
            get_runs_jobs_timeline_hourly=queries.get_runs_jobs_timeline_hourly.sql,  # pylint: disable=no-member
            get_runs_jobs_timeline_daily=queries.get_runs_jobs_timeline_daily.sql,  # pylint: disable=no-member
        )

    def _require_connection_config(self) -> PostgresConfig:
        """Load and validate the PostgreSQL connection config, or raise.
        Returns:
            The validated connection config dict.
        Raises:
            RuntimeError: On config retrieval errors or missing required fields.
        """
        try:
            config = self._pg_config
        except (BotoCoreError, ClientError, ValueError, KeyError) as exc:
            raise RuntimeError(f"PostgreSQL configuration error: {exc}") from exc

        if not config.get("database"):
            raise RuntimeError("PostgreSQL config missing: database.")

        missing = [field for field in REQUIRED_CONNECTION_FIELDS if not config.get(field)]
        if missing:
            raise RuntimeError(f"PostgreSQL config missing: {', '.join(missing)}.")

        return config

    def read_stats(
        self,
        timestamp_start: int | None = None,
        timestamp_end: int | None = None,
        cursor: int | None = None,
        limit: int = POSTGRES_DEFAULT_LIMIT,
    ) -> tuple[list[QueryRow], Pagination]:
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
        self._require_connection_config()

        limit = max(1, min(limit, POSTGRES_MAX_LIMIT))
        now_ms = int(time.time() * 1000)
        ts_start = timestamp_start if timestamp_start is not None else (now_ms - POSTGRES_DEFAULT_WINDOW_MS)
        ts_end = timestamp_end if timestamp_end is not None else now_ms

        started_at = time.perf_counter()
        logger.debug(
            "Running stats query.",
            extra={"ts_start": ts_start, "ts_end": ts_end, "cursor": cursor, "limit": limit},
        )
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

        pagination: Pagination = {
            "cursor": next_cursor,
            "has_more": has_more,
            "limit": limit,
        }

        logger.debug(
            "Stats query returned rows.",
            extra={
                "row_count": len(rows),
                "has_more": has_more,
                "query_duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            },
        )
        return rows, pagination

    def _run_stats_query(
        self,
        connection: psycopg2.extensions.connection,
        ts_start: int,
        ts_end: int,
        cursor: int | None,
        limit: int,
    ) -> RawQueryResult:
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
            # PostgreSQL (psycopg2) wraps every statement, including SELECT, in an implicit transaction.
            # Without an explicit commit or rollback, the connection stays "idle in transaction".
            try:
                connection.rollback()
            except PsycopgError:
                logger.debug("Failed to close the implicit transaction. Closing cached connection.", exc_info=True)
                self._close_connection()
        return col_names, raw_rows

    def read_named_query(
        self,
        query_name: str,
        timestamp_start: int | None = None,
        timestamp_end: int | None = None,
        cursor: int | None = None,
        limit: int = POSTGRES_DEFAULT_LIMIT,
    ) -> tuple[list[QueryRow], Pagination]:
        """Execute a predefined named query with keyset pagination.
        Args:
            query_name: Registered named query identifier (see `SUPPORTED_QUERIES`).
            timestamp_start: Start of time window in epoch milliseconds (default to *now - 7 days*).
            timestamp_end: End of time window in epoch milliseconds (default to *now*).
            cursor: Last `internal_id` from previous page (keyset pagination).
            limit: Maximum number of rows per page.
        Returns:
            Tuple of (rows, pagination), where each row is a dict with raw and
            computed columns and pagination contains cursor info.
        Raises:
            RuntimeError: On unknown query name or database connectivity/query errors.
        """
        query = SUPPORTED_QUERIES.get(query_name)
        if query is None:
            raise RuntimeError(f"Unknown named query: {query_name}.")

        self._require_connection_config()

        limit = max(1, min(limit, POSTGRES_MAX_LIMIT))
        now_ms = int(time.time() * 1000)
        ts_start = timestamp_start if timestamp_start is not None else (now_ms - POSTGRES_DEFAULT_WINDOW_MS)
        ts_end = timestamp_end if timestamp_end is not None else now_ms

        sql = getattr(self._queries, query.sql_key)
        sql_with_cursor = getattr(self._queries, query.sql_key_with_cursor)

        try:
            col_names, raw_rows = self._execute_with_retry(
                lambda conn: self._run_named_query(conn, sql, sql_with_cursor, ts_start, ts_end, cursor, limit)
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

        rows = [self._format_runs_jobs_detail_row(row) for row in rows]

        pagination: Pagination = {
            "cursor": next_cursor,
            "has_more": has_more,
            "limit": limit,
        }

        logger.debug("Named query %s returned %d rows.", query_name, len(rows))
        return rows, pagination

    def _run_named_query(
        self,
        connection: psycopg2.extensions.connection,
        sql: str,
        sql_with_cursor: str,
        ts_start: int,
        ts_end: int,
        cursor: int | None,
        limit: int,
    ) -> RawQueryResult:
        """Execute a named SQL query and return column names and raw rows."""
        try:
            with connection.cursor() as db_cursor:
                if cursor is not None:
                    db_cursor.execute(
                        sql_with_cursor,
                        {"ts_start": ts_start, "ts_end": ts_end, "cursor_id": cursor, "lim": limit + 1},
                    )
                else:
                    db_cursor.execute(
                        sql,
                        {"ts_start": ts_start, "ts_end": ts_end, "lim": limit + 1},
                    )
                if db_cursor.description is None:
                    raise RuntimeError("Named query returned no result description.")
                col_names = [desc[0] for desc in db_cursor.description]
                raw_rows = db_cursor.fetchall()
        finally:
            # PostgreSQL (psycopg2) wraps every statement, including SELECT, in an implicit transaction.
            # Without an explicit commit or rollback, the connection stays "idle in transaction".
            try:
                connection.rollback()
            except PsycopgError:
                logger.debug("Failed to close the implicit transaction. Closing cached connection.", exc_info=True)
                self._close_connection()
        return col_names, raw_rows

    def read_aggregations(
        self,
        query_name: str,
        timestamp_start: int | None = None,
        timestamp_end: int | None = None,
    ) -> RunsJobsAggregations:
        """Execute a predefined window-wide aggregation named query.
        Args:
            query_name: Registered aggregation query identifier.
            timestamp_start: Start of time window in epoch milliseconds (default to *now - 7 days*).
            timestamp_end: End of time window in epoch milliseconds (default to *now*).
        Returns:
            `RunsJobsAggregations` with window-wide totals, success rate, and a bucketed timeline.
        Raises:
            RuntimeError: On unknown query name or database connectivity/query errors.
        """
        query = SUPPORTED_AGGREGATION_QUERIES.get(query_name)
        if query is None:
            raise RuntimeError(f"Unknown aggregation query: {query_name}.")

        self._require_connection_config()

        now_ms = int(time.time() * 1000)
        ts_start = timestamp_start if timestamp_start is not None else (now_ms - POSTGRES_DEFAULT_WINDOW_MS)
        ts_end = timestamp_end if timestamp_end is not None else now_ms

        window_ms = ts_end - ts_start
        bucket = TIMELINE_BUCKET_HOUR if window_ms <= AGGREGATION_BUCKET_THRESHOLD_MS else TIMELINE_BUCKET_DAY
        timeline_sql_key = (
            query.timeline_hourly_sql_key if bucket == TIMELINE_BUCKET_HOUR else (query.timeline_daily_sql_key)
        )

        summary_sql = getattr(self._queries, query.summary_sql_key)
        timeline_sql = getattr(self._queries, timeline_sql_key)

        try:
            summary_row, timeline_rows = self._execute_with_retry(
                lambda conn: self._run_aggregation_queries(conn, summary_sql, timeline_sql, ts_start, ts_end)
            )
        except PsycopgError as exc:
            self._close_connection()
            raise RuntimeError(f"Database aggregation query error: {exc}") from exc

        aggregations = self._build_aggregations(ts_start, ts_end, bucket, summary_row, timeline_rows)

        logger.debug("Aggregation query %s returned %d timeline buckets.", query_name, len(timeline_rows))
        return aggregations

    def _run_aggregation_queries(
        self,
        connection: psycopg2.extensions.connection,
        summary_sql: str,
        timeline_sql: str,
        ts_start: int,
        ts_end: int,
    ) -> AggregationQueryResult:
        """Execute the summary and timeline aggregation queries on the same connection."""
        try:
            with connection.cursor() as db_cursor:
                # Summary: single aggregate row for the whole window.
                db_cursor.execute(summary_sql, {"ts_start": ts_start, "ts_end": ts_end})
                if db_cursor.description is None:
                    raise RuntimeError("Aggregation query returned no result description.")
                summary_cols = [desc[0] for desc in db_cursor.description]
                summary_values = db_cursor.fetchone()
                summary_row = dict(zip(summary_cols, summary_values, strict=True)) if summary_values else {}

                # Timeline: bucketed rows for the heatmap.
                db_cursor.execute(timeline_sql, {"ts_start": ts_start, "ts_end": ts_end})
                if db_cursor.description is None:
                    raise RuntimeError("Timeline query returned no result description.")
                timeline_cols = [desc[0] for desc in db_cursor.description]
                timeline_rows = [dict(zip(timeline_cols, row, strict=True)) for row in db_cursor.fetchall()]
        finally:
            # PostgreSQL (psycopg2) wraps every statement, including SELECT, in an implicit transaction.
            # Without an explicit commit or rollback, the connection stays "idle in transaction".
            try:
                connection.rollback()
            except PsycopgError:
                logger.debug("Failed to close the implicit transaction. Closing cached connection.", exc_info=True)
                self._close_connection()
        return summary_row, timeline_rows

    @staticmethod
    def _build_aggregations(
        ts_start: int,
        ts_end: int,
        bucket: str,
        summary_row: dict[str, Any],
        timeline_rows: list[dict[str, Any]],
    ) -> RunsJobsAggregations:
        """Shape raw summary/timeline rows into a typed `RunsJobsAggregations`."""
        total_runs = summary_row.get("total_runs") or 0
        succeeded = summary_row.get("succeeded") or 0
        failed = summary_row.get("failed") or 0
        avg_duration_ms = summary_row.get("avg_duration_ms")

        return RunsJobsAggregations(
            window=AggregationsWindow(timestamp_start=ts_start, timestamp_end=ts_end),
            total_runs=total_runs,
            total_tenants=summary_row.get("total_tenants") or 0,
            total_catalogs=summary_row.get("total_catalogs") or 0,
            succeeded=succeeded,
            failed=failed,
            success_rate=round((succeeded / total_runs) * 100, 1) if total_runs else 0.0,
            avg_duration_ms=round(avg_duration_ms) if avg_duration_ms is not None else None,
            timeline=Timeline(
                bucket=bucket,
                entries=[
                    TimelineEntry(
                        bucket_start=int(row["bucket_start"]),
                        total=row["total"],
                        succeeded=row["succeeded"],
                        failed=row["failed"],
                    )
                    for row in timeline_rows
                ],
            ),
        )

    @staticmethod
    def _format_runs_jobs_detail_row(row: dict[str, Any]) -> dict[str, Any]:
        """Add computed columns to a `runs_jobs_detail` result row.
        Args:
            row: Raw database row dict.
        Returns:
            Row dict enriched with computed fields.
        """
        ts_start = row.get("timestamp_start")
        ts_end = row.get("timestamp_end")

        # run_date: date portion of the job-level timestamp_start (DD-MM-YYYY, UTC).
        if ts_start is not None:
            row["run_date"] = datetime.fromtimestamp(ts_start / 1000, tz=timezone.utc).strftime("%d-%m-%Y")
        else:
            row["run_date"] = None

        # run_status: message reclassification on all rows, else the raw status.
        status = str(row.get("status", "")).lower()
        message = str(row.get("message") or "").lower()
        if "no data" in message:
            row["run_status"] = "no data received"
        elif "no records to send" in message:
            row["run_status"] = "no data produced"
        elif "timeout" in message:
            row["run_status"] = "timeout"
        else:
            row["run_status"] = status

        # formatted_tenant
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

    def check_health(self) -> str | None:
        """Check PostgreSQL reader health.
        Raises:
            HealthCheckError: If the reader configuration is invalid or incomplete.
        """
        if not self._secret_name or not self._secret_region:
            raise HealthCheckError("postgres secret not configured")

        try:
            pg_config = self._pg_config
        except (BotoCoreError, ClientError, RuntimeError, ValueError, KeyError) as err:
            raise HealthCheckError(str(err)) from err

        if not pg_config.get("database"):
            raise HealthCheckError("database not configured")

        missing = [field for field in REQUIRED_CONNECTION_FIELDS if not pg_config.get(field)]
        if missing:
            raise HealthCheckError(f"{missing[0]} not configured")

        return None
