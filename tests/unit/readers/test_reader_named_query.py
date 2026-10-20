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


import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.readers.reader_postgres import ReaderPostgres
import src.utils.postgres_base as pb

_DETAIL_DESCRIPTION = [
    ("event_id",),
    ("job_ref",),
    ("tenant_id",),
    ("source_app",),
    ("source_app_version",),
    ("environment",),
    ("run_timestamp_start",),
    ("run_timestamp_end",),
    ("internal_id",),
    ("country",),
    ("catalog_id",),
    ("status",),
    ("timestamp_start",),
    ("timestamp_end",),
    ("message",),
    ("additional_info",),
]


@pytest.fixture
def pg_secret() -> dict[str, Any]:
    """Sample Postgres secret payload."""
    return {
        "database": "eventgate",
        "host": "localhost",
        "port": 5432,
        "user": "reader",
        "password": "secret",
    }


@pytest.fixture
def reader(monkeypatch: pytest.MonkeyPatch) -> ReaderPostgres:
    """Create a ReaderPostgres instance with env vars set."""
    monkeypatch.setenv("POSTGRES_SECRET_NAME", "eventgate/postgres")
    monkeypatch.setenv("POSTGRES_SECRET_REGION", "us-east-1")
    return ReaderPostgres()


def _make_mock_connection(description: list[tuple[str, ...]], rows: list[tuple[Any, ...]]) -> MagicMock:
    """Build a mock psycopg2 connection with cursor returning given rows."""
    mock_cursor = MagicMock()
    mock_cursor.description = description
    mock_cursor.fetchall.return_value = rows

    mock_conn = MagicMock()
    mock_conn.closed = 0
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn


class TestFormatRunsJobsDetailRow:
    """Tests for the named-query runs_jobs_detail row shaping (DIV-1, DIV-2)."""

    def test_run_date_uses_job_timestamp_start(self) -> None:
        """DIV-1: run_date derives from the JOB timestamp_start, not the run one."""
        row: dict[str, Any] = {
            "status": "succeeded",
            "message": None,
            "tenant_id": "T",
            "run_timestamp_start": 1704067200000,  # 2024-01-01 UTC
            "timestamp_start": 1704153600000,  # 2024-01-02 UTC
            "timestamp_end": 1704157200000,
        }
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "02-01-2024" == result["run_date"]

    def test_succeeded_status_passthrough(self) -> None:
        """A succeeded job with no special message keeps status 'succeeded'."""
        row = _base_row(status="succeeded", message=None)
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "succeeded" == result["run_status"]

    def test_killed_status_preserved(self) -> None:
        """DIV-2: killed is preserved, not collapsed to succeed."""
        row = _base_row(status="killed", message=None)
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "killed" == result["run_status"]

    def test_skipped_status_preserved(self) -> None:
        """DIV-2: skipped is preserved, not collapsed to succeed."""
        row = _base_row(status="skipped", message=None)
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "skipped" == result["run_status"]

    def test_failed_status_passthrough(self) -> None:
        """A failed job without a special message keeps status 'failed'."""
        row = _base_row(status="failed", message="boom")
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "failed" == result["run_status"]

    def test_no_data_message_on_succeeded_row(self) -> None:
        """DIV-2: message reclassification applies to all rows, not only failed."""
        row = _base_row(status="succeeded", message="No Data in source")
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "no data received" == result["run_status"]

    def test_no_records_to_send_message(self) -> None:
        """Message 'no records to send' maps to no data produced."""
        row = _base_row(status="failed", message="There were no records to send today")
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "no data produced" == result["run_status"]

    def test_timeout_message(self) -> None:
        """DIV-2: timeout bucket is derived from the message."""
        row = _base_row(status="failed", message="Job Timeout after 3600s")
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "timeout" == result["run_status"]

    def test_no_data_precedence_over_timeout(self) -> None:
        """Precedence: 'no data' wins over 'timeout' when both are present."""
        row = _base_row(status="failed", message="no data - timeout")
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "no data received" == result["run_status"]

    def test_null_message_falls_through_to_raw_status(self) -> None:
        """A null message falls through to the raw status."""
        row = _base_row(status="killed", message=None)
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "killed" == result["run_status"]

    def test_formatted_tenant_lowercased(self) -> None:
        """formatted_tenant lowercases the tenant_id."""
        row = _base_row(status="succeeded", message=None, tenant_id="ABC")
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "abc" == result["formatted_tenant"]

    def test_elapsed_time_in_milliseconds(self) -> None:
        """elapsed_time is the job duration in milliseconds."""
        row = _base_row(
            status="succeeded",
            message=None,
            timestamp_start=1704067200000,
            timestamp_end=1704070800000,
        )
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert 3_600_000 == result["elapsed_time"]

    def test_start_and_end_time_are_real_utc_timestamps(self) -> None:
        """start_time / end_time keep the real time-of-day in UTC."""
        row = _base_row(
            status="succeeded",
            message=None,
            timestamp_start=1704070800000,  # 2024-01-01 01:00:00 UTC
            timestamp_end=1704074400000,  # 2024-01-01 02:00:00 UTC
        )
        result = ReaderPostgres._format_runs_jobs_detail_row(row)

        assert "2024-01-01 01:00:00" == result["start_time"]
        assert "2024-01-01 02:00:00" == result["end_time"]


class TestReadNamedQuery:
    """Tests for read_named_query execution."""

    def test_returns_rows_and_pagination(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that read_named_query returns shaped rows and pagination."""
        rows = [
            ("ev1", "r", "T", "a", "1", "t", 0, 0, 2, "ZA", "c", "killed", 0, 0, None, None),
            ("ev2", "r", "T", "a", "1", "t", 0, 0, 1, "ZA", "c", "succeeded", 0, 0, None, None),
        ]
        mock_conn = _make_mock_connection(_DETAIL_DESCRIPTION, rows)

        with (
            patch("boto3.Session") as mock_session,
            patch.object(pb, "psycopg2") as mock_pg,
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            mock_pg.connect.return_value = mock_conn

            result_rows, pagination = reader.read_named_query(query_name="runs_jobs_detail", limit=50)

        assert 2 == len(result_rows)
        assert "killed" == result_rows[0]["run_status"]
        assert False is pagination["has_more"]

    def test_cursor_uses_cursor_variant(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that passing a cursor uses the keyset-cursor SQL variant."""
        mock_conn = _make_mock_connection(_DETAIL_DESCRIPTION, [])

        with (
            patch("boto3.Session") as mock_session,
            patch.object(pb, "psycopg2") as mock_pg,
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            mock_pg.connect.return_value = mock_conn

            reader.read_named_query(query_name="runs_jobs_detail", cursor=100, limit=10)

            executed_sql = mock_conn.cursor.return_value.__enter__.return_value.execute.call_args[0][0]
            executed_params = mock_conn.cursor.return_value.__enter__.return_value.execute.call_args[0][1]

        assert "j.internal_id <" in executed_sql
        assert 100 == executed_params["cursor_id"]

    def test_unknown_query_name_raises_runtime_error(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that an unknown query_name raises RuntimeError."""
        with (
            patch("boto3.Session") as mock_session,
            pytest.raises(RuntimeError, match="Unknown named query"),
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            reader.read_named_query(query_name="does_not_exist")


class TestReadAggregations:
    """Tests for read_aggregations execution."""

    def test_build_aggregations_shapes_populated_rows(self) -> None:
        """Test that _build_aggregations shapes summary/timeline rows into a typed result."""
        summary_row = {
            "total_runs": 2,
            "total_tenants": 1,
            "total_catalogs": 1,
            "succeeded": 1,
            "failed": 1,
            "avg_duration_ms": 1800000.0,
        }
        timeline_rows = [{"bucket_start": 1704067200000, "total": 2, "succeeded": 1, "failed": 1}]

        aggregations = ReaderPostgres._build_aggregations(
            ts_start=1704067200000,
            ts_end=1704070800000,
            bucket="hour",
            summary_row=summary_row,
            timeline_rows=timeline_rows,
        )

        assert 1704067200000 == aggregations.window.timestamp_start
        assert 1704070800000 == aggregations.window.timestamp_end
        assert 2 == aggregations.total_runs
        assert 1 == aggregations.total_tenants
        assert 1 == aggregations.total_catalogs
        assert 1 == aggregations.succeeded
        assert 1 == aggregations.failed
        assert 50.0 == aggregations.success_rate
        assert 1800000 == aggregations.avg_duration_ms
        assert "hour" == aggregations.timeline.bucket
        assert 1 == len(aggregations.timeline.entries)
        assert 1704067200000 == aggregations.timeline.entries[0].bucket_start
        assert 2 == aggregations.timeline.entries[0].total

    def test_build_aggregations_zeroes_empty_window(self) -> None:
        """Test that an empty summary/timeline still produces a fully-populated, zeroed result."""
        summary_row = {
            "total_runs": 0,
            "total_tenants": 0,
            "total_catalogs": 0,
            "succeeded": 0,
            "failed": 0,
            "avg_duration_ms": None,
        }

        aggregations = ReaderPostgres._build_aggregations(
            ts_start=1704067200000, ts_end=1704070800000, bucket="hour", summary_row=summary_row, timeline_rows=[]
        )

        assert 0 == aggregations.total_runs
        assert 0.0 == aggregations.success_rate
        assert aggregations.avg_duration_ms is None
        assert [] == aggregations.timeline.entries

    @pytest.mark.parametrize(
        "window_ms, expected_bucket, expected_fragment",
        [
            pytest.param(24 * 60 * 60 * 1000, "hour", "date_trunc('hour'", id="7_day_or_less_is_hourly"),
            pytest.param(30 * 24 * 60 * 60 * 1000, "day", "date_trunc('day'", id="over_7_days_is_daily"),
        ],
    )
    def test_window_length_selects_timeline_bucket(
        self,
        reader: ReaderPostgres,
        pg_secret: dict[str, Any],
        window_ms: int,
        expected_bucket: str,
        expected_fragment: str,
    ) -> None:
        """Test that the window length picks the hourly vs. daily timeline SQL variant."""
        mock_conn = _make_mock_multi_query_connection(
            [
                (
                    [
                        ("total_runs",),
                        ("total_tenants",),
                        ("total_catalogs",),
                        ("succeeded",),
                        ("failed",),
                        ("avg_duration_ms",),
                    ],
                    [(0, 0, 0, 0, 0, None)],
                ),
                ([("bucket_start",), ("total",), ("succeeded",), ("failed",)], []),
            ]
        )

        with (
            patch("boto3.Session") as mock_session,
            patch.object(pb, "psycopg2") as mock_pg,
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            mock_pg.connect.return_value = mock_conn

            aggregations = reader.read_aggregations(
                query_name="runs_jobs_detail_aggregation", timestamp_start=0, timestamp_end=window_ms
            )

            executed_sql = mock_conn.cursor.return_value.__enter__.return_value.execute.call_args_list[1][0][0]

        assert expected_bucket == aggregations.timeline.bucket
        assert expected_fragment in executed_sql

    def test_unknown_query_name_raises_runtime_error(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that an unknown aggregation query_name raises RuntimeError."""
        with (
            patch("boto3.Session") as mock_session,
            pytest.raises(RuntimeError, match="Unknown aggregation query"),
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            reader.read_aggregations(query_name="does_not_exist")


def _make_mock_multi_query_connection(
    query_results: list[tuple[list[tuple[str, ...]], list[tuple[Any, ...]]]],
) -> MagicMock:
    """Build a mock psycopg2 connection whose cursor returns a different result per `execute` call."""
    mock_cursor = MagicMock()
    descriptions = [description for description, _ in query_results]
    rows = [query_rows for _, query_rows in query_results]
    mock_cursor.description = descriptions[0]

    call_state = {"index": -1}

    def _execute(*_args: Any, **_kwargs: Any) -> None:
        call_state["index"] += 1
        mock_cursor.description = descriptions[call_state["index"]]
        mock_cursor.fetchall.return_value = rows[call_state["index"]]
        mock_cursor.fetchone.return_value = rows[call_state["index"]][0] if rows[call_state["index"]] else None

    mock_cursor.execute.side_effect = _execute

    mock_conn = MagicMock()
    mock_conn.closed = 0
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn


def _base_row(
    status: str,
    message: str | None,
    tenant_id: str = "T",
    timestamp_start: int | None = 1704067200000,
    timestamp_end: int | None = 1704070800000,
) -> dict[str, Any]:
    """Build a raw detail row for shaping tests."""
    return {
        "status": status,
        "message": message,
        "tenant_id": tenant_id,
        "run_timestamp_start": 1704067200000,
        "timestamp_start": timestamp_start,
        "timestamp_end": timestamp_end,
    }
