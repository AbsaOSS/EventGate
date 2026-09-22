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
from typing import Any

import pytest

from tests.integration.conftest import EventGateTestClient, EventStatsTestClient

_QUERY = "runs_jobs_detail"


def _post_job(
    client: EventGateTestClient,
    token: str,
    status: str,
    message: str | None,
    source_app: str,
) -> dict[str, Any]:
    """Post a single-job run event with an explicit status and message."""
    now_ms = int(time.time() * 1000)
    job: dict[str, Any] = {
        "catalog_id": "db.schema.table",
        "status": status,
        "timestamp_start": now_ms - 60000,
        "timestamp_end": now_ms,
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
        "timestamp_start": now_ms - 60000,
        "timestamp_end": now_ms,
        "jobs": [job],
    }
    response = client.post_event("public.cps.za.runs", event, token=token)
    assert 202 == response["statusCode"], f"Seed event failed: {response}"
    return event


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
