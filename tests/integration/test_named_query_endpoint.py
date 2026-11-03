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
import time
import uuid
from collections.abc import Generator
from datetime import datetime, timezone
from typing import Any

from psycopg2.extensions import TRANSACTION_STATUS_IDLE, cursor, parse_dsn
import pytest

from src.readers.reader_postgres import ReaderPostgres
from src.utils.postgres_base import PostgresConfig
from tests.integration.conftest import EventGateTestClient, EventStatsTestClient

_QUERY = "runs_jobs_detail"


def _post_job(
    client: EventGateTestClient,
    token: str,
    status: str,
    message: str | None,
    source_app: str,
    timestamp_start: int | None = None,
) -> dict[str, Any]:
    """Post a single-job run event with an explicit status and message."""
    now_ms = int(time.time() * 1000)
    job_duration_ms = 60_000
    ts_start = timestamp_start if timestamp_start is not None else now_ms - job_duration_ms
    ts_end = ts_start + job_duration_ms
    job: dict[str, Any] = {
        "catalog_id": "db.schema.table",
        "status": status,
        "timestamp_start": ts_start,
        "timestamp_end": ts_end,
    }
    if message is not None:
        job["message"] = message
    event = {
        "event_id": str(uuid.uuid4()),
        "job_ref": "spark-cq-001",
        "tenant_id": "CQ_TEST",
        "source_app": source_app,
        "source_app_version": "2.0.0",
        "environment": "test",
        "timestamp_start": ts_start,
        "timestamp_end": ts_end,
        "jobs": [job],
    }
    response = client.post_event("public.cps.za.runs", event, token=token)
    assert 202 == response["statusCode"], f"Seed event failed: {response}"
    return event


@pytest.fixture
def aggregation_reader(postgres_container: str) -> Generator[ReaderPostgres, None, None]:
    """Use an isolated cached reader connection to the real PostgreSQL container."""
    config = parse_dsn(postgres_container)
    reader = ReaderPostgres()
    reader._pg_config = PostgresConfig(
        database=config["dbname"],
        host=config["host"],
        port=int(config["port"]),
        user=config["user"],
        password=config["password"],
    )
    try:
        yield reader
    finally:
        reader._close_connection()


@pytest.mark.parametrize("bucket, month, window_days", [("hour", 5, 1), ("day", 6, 8)])
def test_timeline_buckets_are_utc_aligned(
    aggregation_reader: ReaderPostgres,
    eventgate_client: EventGateTestClient,
    valid_token: str,
    bucket: str,
    month: int,
    window_days: int,
) -> None:
    """Keep hourly and daily UTC boundaries under a fractional-offset session timezone."""
    window_start = int(datetime(2024, month, 1, tzinfo=timezone.utc).timestamp() * 1000)
    _post_job(eventgate_client, valid_token, "succeeded", None, "cq-utc-test", window_start + 10 * 60 * 1000)
    connection = aggregation_reader._get_connection()
    with connection.cursor() as db_cursor:
        db_cursor.execute("SET TIME ZONE %s", ("Asia/Kathmandu",))
    connection.commit()

    result = aggregation_reader.read_aggregations(
        "runs_jobs_detail_aggregation", window_start, window_start + window_days * 24 * 60 * 60 * 1000
    )

    assert 1 == result.total_runs
    assert 1 == result.timeline.entries[0].total
    assert bucket == result.timeline.bucket
    # Expect one bucket at UTC midnight, unaffected by Kathmandu's UTC+05:45 offset.
    assert [window_start] == [entry.bucket_start for entry in result.timeline.entries]


def test_aggregation_uses_one_snapshot_during_intervening_insert(
    aggregation_reader: ReaderPostgres, eventgate_client: EventGateTestClient, valid_token: str
) -> None:
    """Keep one response consistent while making a committed insert visible to the next request."""
    timestamp = int(datetime(2024, 4, 1, 12, tzinfo=timezone.utc).timestamp() * 1000)
    _post_job(eventgate_client, valid_token, "succeeded", None, "cq-snapshot-test", timestamp)
    connection = aggregation_reader._get_connection()
    with connection.cursor() as db_cursor:
        db_cursor.execute("SHOW default_transaction_isolation")
        default_isolation = db_cursor.fetchone()[0]
    connection.rollback()
    inserted = False

    class InsertAfterSummaryCursor(cursor):
        """Commit a job through the separate ingestion connection after the first summary read."""

        def fetchone(self) -> tuple[Any, ...] | None:
            nonlocal inserted
            row = super().fetchone()
            if not inserted:
                # Commit a second job on another connection after the summary SELECT, before the timeline SELECT.
                _post_job(eventgate_client, valid_token, "failed", None, "cq-snapshot-test", timestamp)
                inserted = True
            return row

    connection.cursor_factory = InsertAfterSummaryCursor
    first = aggregation_reader.read_aggregations("runs_jobs_detail_aggregation", timestamp, timestamp)

    # Both reads must still see only the original job, despite the committed insert between them.
    assert inserted
    assert 1 == first.total_runs
    assert 1 == sum(entry.total for entry in first.timeline.entries)
    assert 1 == first.succeeded
    assert 1 == sum(entry.succeeded for entry in first.timeline.entries)
    assert 0 == first.failed
    assert 0 == sum(entry.failed for entry in first.timeline.entries)
    assert TRANSACTION_STATUS_IDLE == connection.get_transaction_status()

    second = aggregation_reader.read_aggregations("runs_jobs_detail_aggregation", timestamp, timestamp)

    # The next transaction must see both jobs, including the newly committed failed job.
    assert 2 == second.total_runs
    assert 2 == sum(entry.total for entry in second.timeline.entries)
    assert 1 == second.failed
    assert 1 == sum(entry.failed for entry in second.timeline.entries)
    assert TRANSACTION_STATUS_IDLE == connection.get_transaction_status()
    with connection.cursor() as db_cursor:
        db_cursor.execute("SHOW default_transaction_isolation")
        assert default_isolation == db_cursor.fetchone()[0]
    connection.rollback()


def test_unknown_query_name_returns_400(stats_client: EventStatsTestClient) -> None:
    """Test that an unknown query_name returns 400, not 500."""
    response = stats_client.post_named_query("public.cps.za.runs", "does_not_exist", {})

    assert 400 == response["statusCode"]
    body = json.loads(response["body"])
    assert "does_not_exist" in body["errors"][0]["message"]


def test_unsupported_topic_returns_400(stats_client: EventStatsTestClient) -> None:
    """Test that a known but unsupported topic returns 400."""
    response = stats_client.post_named_query("public.cps.za.test", _QUERY, {})

    assert 400 == response["statusCode"]


def test_nonexistent_topic_returns_404(stats_client: EventStatsTestClient) -> None:
    """Test that an unknown topic returns 404."""
    response = stats_client.post_named_query("nonexistent.topic", _QUERY, {})

    assert 404 == response["statusCode"]


class TestNamedQueryRunStatus:
    """End-to-end run_status categorisation for runs_jobs_detail (DIV-2)."""

    @pytest.fixture(scope="class", autouse=True)
    def seed_events(self, eventgate_client: EventGateTestClient, valid_token: str) -> None:
        """Seed jobs covering every run_status bucket."""
        _post_job(eventgate_client, valid_token, "succeeded", None, "cq-status-test")
        _post_job(eventgate_client, valid_token, "failed", "boom", "cq-status-test")
        _post_job(eventgate_client, valid_token, "killed", None, "cq-status-test")
        _post_job(eventgate_client, valid_token, "skipped", None, "cq-status-test")
        _post_job(eventgate_client, valid_token, "failed", "connection Timeout after 60s", "cq-status-test")
        _post_job(eventgate_client, valid_token, "succeeded", "No Data found in source", "cq-status-test")
        _post_job(eventgate_client, valid_token, "failed", "there were no records to send", "cq-status-test")

    def _run_statuses_for_tenant(self, stats_client: EventStatsTestClient) -> set[str]:
        """Return the set of run_status values seeded by this test class."""
        response = stats_client.post_named_query("public.cps.za.runs", _QUERY, {"limit": 1000})
        assert 200 == response["statusCode"]
        body = json.loads(response["body"])
        return {row["run_status"] for row in body["data"] if row["formatted_tenant"] == "cq_test"}

    def test_returns_200_with_computed_columns(self, stats_client: EventStatsTestClient) -> None:
        """Test the named query returns 200 with the computed columns."""
        response = stats_client.post_named_query("public.cps.za.runs", _QUERY, {"limit": 1000})

        assert 200 == response["statusCode"]
        body = json.loads(response["body"])
        assert True is body["success"]
        row = next(r for r in body["data"] if r["formatted_tenant"] == "cq_test")
        for column in ("run_date", "run_status", "formatted_tenant", "elapsed_time", "start_time", "end_time"):
            assert column in row

    def test_raw_statuses_preserved(self, stats_client: EventStatsTestClient) -> None:
        """Test that succeeded/failed/killed/skipped are preserved, not collapsed."""
        statuses = self._run_statuses_for_tenant(stats_client)

        assert {"succeeded", "failed", "killed", "skipped"}.issubset(statuses)

    def test_message_buckets_derived(self, stats_client: EventStatsTestClient) -> None:
        """Test that timeout / no data received / no data produced buckets are derived."""
        statuses = self._run_statuses_for_tenant(stats_client)

        assert {"timeout", "no data received", "no data produced"}.issubset(statuses)


class TestNamedQueryAggregation:
    """End-to-end for the dedicated runs_jobs_detail_aggregation named query."""

    _AGG_QUERY = "runs_jobs_detail_aggregation"

    @pytest.fixture(scope="class", autouse=True)
    def seed_events(self, eventgate_client: EventGateTestClient, valid_token: str) -> None:
        """Seed a mix of succeeded/failed jobs to aggregate over."""
        _post_job(eventgate_client, valid_token, "succeeded", None, "cq-aggregation-test")
        _post_job(eventgate_client, valid_token, "succeeded", None, "cq-aggregation-test")
        _post_job(eventgate_client, valid_token, "failed", "boom", "cq-aggregation-test")

    def test_returns_populated_aggregations(self, stats_client: EventStatsTestClient) -> None:
        """Test that totals/success_rate/timeline reflect the seeded jobs, with no data/pagination keys."""
        response = stats_client.post_named_query("public.cps.za.runs", self._AGG_QUERY, {})

        assert 200 == response["statusCode"]
        body = json.loads(response["body"])
        assert True is body["success"]
        assert "aggregations" in body
        assert "data" not in body
        assert "pagination" not in body

        aggregations = body["aggregations"]
        assert aggregations["total_runs"] >= 3
        assert aggregations["succeeded"] >= 2
        assert aggregations["failed"] >= 1
        timeline = aggregations["timeline"]
        assert timeline["bucket"] in ("hour", "day")
        assert sum(entry["total"] for entry in timeline["entries"]) == aggregations["total_runs"]

    def test_cursor_or_limit_returns_400(self, stats_client: EventStatsTestClient) -> None:
        """Test that cursor/limit are rejected for the aggregation query (not silently ignored)."""
        response = stats_client.post_named_query("public.cps.za.runs", self._AGG_QUERY, {"limit": 10})

        assert 400 == response["statusCode"]
        body = json.loads(response["body"])
        assert "limit" in body["errors"][0]["message"]


class TestNamedQueryPagination:
    """Keyset pagination for runs_jobs_detail."""

    @pytest.fixture(scope="class", autouse=True)
    def seed_events(self, eventgate_client: EventGateTestClient, valid_token: str) -> None:
        """Seed enough jobs to page through."""
        for _ in range(5):
            _post_job(eventgate_client, valid_token, "succeeded", None, "cq-pagination-test")

    def test_limit_and_cursor_page_through(self, stats_client: EventStatsTestClient) -> None:
        """Test that limit caps the page and the cursor fetches the next page."""
        resp1 = stats_client.post_named_query("public.cps.za.runs", _QUERY, {"limit": 2})
        body1 = json.loads(resp1["body"])

        assert 200 == resp1["statusCode"]
        assert len(body1["data"]) <= 2
        assert True is body1["pagination"]["has_more"]
        cursor = body1["pagination"]["cursor"]
        assert cursor is not None

        resp2 = stats_client.post_named_query("public.cps.za.runs", _QUERY, {"limit": 2, "cursor": cursor})
        body2 = json.loads(resp2["body"])

        assert 200 == resp2["statusCode"]
        first_page_ids = {row["internal_id"] for row in body1["data"]}
        second_page_ids = {row["internal_id"] for row in body2["data"]}
        assert first_page_ids.isdisjoint(second_page_ids)
