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
from typing import Any, Dict

import pytest

from tests.integration.conftest import EventGateTestClient, EventStatsTestClient
from tests.integration.utils.jwt_helper import generate_token


def _post_seed_events(
    client: EventGateTestClient,
    token: str,
    count: int = 3,
    source_app: str = "integration-test-stats",
    statuses: Any = None,
) -> list:
    """Post multiple runs events for seeding and return payloads."""
    events = []
    for i in range(count):
        status = statuses[i] if statuses and i < len(statuses) else "succeeded"
        now_ms = int(time.time() * 1000)
        event = {
            "event_id": str(uuid.uuid4()),
            "job_ref": f"spark-stats-{i:03d}",
            "tenant_id": "STATS_TEST",
            "source_app": source_app,
            "source_app_version": "2.0.0",
            "environment": "test",
            "timestamp_start": now_ms - 60000,
            "timestamp_end": now_ms,
            "jobs": [
                {
                    "catalog_id": f"db.schema.table_{i}",
                    "status": status,
                    "timestamp_start": now_ms - 60000,
                    "timestamp_end": now_ms,
                }
            ],
        }
        response = client.post_event("public.cps.za.runs", event, token=token)
        assert 202 == response["statusCode"], f"Seed event {i} failed: {response}"
        events.append(event)
    return events


class TestStatsEndpointAuth:
    """Authentication and authorization tests for /stats."""

    def test_missing_token_returns_401(self, stats_client: EventStatsTestClient) -> None:
        """Test POST /stats without token returns 401."""
        response = stats_client.post_stats("public.cps.za.runs", {})

        assert 401 == response["statusCode"]

    def test_unauthorized_user_returns_403(
        self, stats_client: EventStatsTestClient, jwt_keypair: Dict[str, Any]
    ) -> None:
        """Test POST /stats from unauthorized user returns 403."""
        bad_token = generate_token(jwt_keypair["private_key_pem"], "UnauthorizedStatsUser")

        response = stats_client.post_stats("public.cps.za.runs", {}, token=bad_token)

        assert 403 == response["statusCode"]

    def test_nonexistent_topic_returns_404(self, stats_client: EventStatsTestClient, valid_token: str) -> None:
        """Test POST /stats for unknown topic returns 404."""
        response = stats_client.post_stats("nonexistent.topic", {}, token=valid_token)

        assert 404 == response["statusCode"]


class TestStatsEndpointBasicQuery:
    """Basic query tests for /stats."""

    @pytest.fixture(scope="class", autouse=True)
    def seed_events(self, eventgate_client: EventGateTestClient, valid_token: str) -> list:
        """Seed events for stats query tests."""
        return _post_seed_events(
            eventgate_client,
            valid_token,
            count=3,
            source_app="stats-basic-test",
            statuses=["succeeded", "failed", "succeeded"],
        )

    def test_default_query_returns_200(self, stats_client: EventStatsTestClient, valid_token: str) -> None:
        """Test default stats query returns 200 with data."""
        response = stats_client.post_stats("public.cps.za.runs", {}, token=valid_token)

        assert 200 == response["statusCode"]
        body = json.loads(response["body"])
        assert True is body["success"]
        assert isinstance(body["data"], list)
        assert len(body["data"]) > 0
        assert "pagination" in body

    def test_joined_data_contains_run_and_job_fields(
        self, stats_client: EventStatsTestClient, valid_token: str
    ) -> None:
        """Test that response rows contain both run-level and job-level fields."""
        response = stats_client.post_stats("public.cps.za.runs", {}, token=valid_token)
        body = json.loads(response["body"])
        row = body["data"][0]

        # Run-level fields.
        assert "event_id" in row
        assert "job_ref" in row
        assert "tenant_id" in row
        assert "source_app" in row

        # Job-level fields.
        assert "internal_id" in row
        assert "catalog_id" in row
        assert "status" in row

    def test_computed_columns_present(self, stats_client: EventStatsTestClient, valid_token: str) -> None:
        """Test that computed columns are present in the response."""
        response = stats_client.post_stats("public.cps.za.runs", {}, token=valid_token)
        body = json.loads(response["body"])
        row = body["data"][0]

        assert "run_date" in row
        assert "run_status" in row
        assert "formatted_tenant" in row
        assert "elapsed_time" in row
        assert "start_time" in row
        assert "end_time" in row

    def test_results_sorted_by_internal_id_desc(self, stats_client: EventStatsTestClient, valid_token: str) -> None:
        """Test that results are sorted by internal_id descending."""
        response = stats_client.post_stats("public.cps.za.runs", {}, token=valid_token)
        body = json.loads(response["body"])

        ids = [row["internal_id"] for row in body["data"]]
        assert ids == sorted(ids, reverse=True)


class TestStatsEndpointTimestampFilter:
    """Timestamp filter tests for /stats."""

    @pytest.fixture(scope="class", autouse=True)
    def seed_events(self, eventgate_client: EventGateTestClient, valid_token: str) -> list:
        """Seed events for timestamp filter tests."""
        return _post_seed_events(
            eventgate_client,
            valid_token,
            count=2,
            source_app="ts-filter-test",
        )

    def test_timestamp_range_returns_data(self, stats_client: EventStatsTestClient, valid_token: str) -> None:
        """Test that timestamp_start/timestamp_end narrows results."""
        now_ms = int(time.time() * 1000)
        one_hour_ago = now_ms - 3600000

        response = stats_client.post_stats(
            "public.cps.za.runs",
            {"timestamp_start": one_hour_ago, "timestamp_end": now_ms},
            token=valid_token,
        )
        body = json.loads(response["body"])

        assert 200 == response["statusCode"]
        assert len(body["data"]) > 0

    def test_unsupported_topic_returns_400(self, stats_client: EventStatsTestClient, valid_token: str) -> None:
        """Test that a known but unsupported topic returns 400."""
        response = stats_client.post_stats("public.cps.za.test", {}, token=valid_token)

        assert 400 == response["statusCode"]


class TestStatsEndpointPagination:
    """Pagination tests for /stats."""

    @pytest.fixture(scope="class", autouse=True)
    def seed_pagination_events(self, eventgate_client: EventGateTestClient, valid_token: str) -> list:
        """Seed enough events to test pagination."""
        return _post_seed_events(eventgate_client, valid_token, count=5, source_app="pagination-test")

    def test_limit_controls_page_size(self, stats_client: EventStatsTestClient, valid_token: str) -> None:
        """Test that limit controls number of returned rows."""
        response = stats_client.post_stats(
            "public.cps.za.runs",
            {"limit": 2},
            token=valid_token,
        )
        body = json.loads(response["body"])

        assert 200 == response["statusCode"]
        assert len(body["data"]) <= 2

    def test_cursor_pagination_returns_next_page(self, stats_client: EventStatsTestClient, valid_token: str) -> None:
        """Test that cursor from first page fetches the next page."""
        # First page.
        resp1 = stats_client.post_stats(
            "public.cps.za.runs",
            {"limit": 2},
            token=valid_token,
        )
        body1 = json.loads(resp1["body"])

        assert 200 == resp1["statusCode"]
        assert True is body1["pagination"]["has_more"]
        cursor = body1["pagination"]["cursor"]
        assert cursor is not None

        # Second page — cursor is now an integer (internal_id).
        resp2 = stats_client.post_stats(
            "public.cps.za.runs",
            {"limit": 2, "cursor": cursor},
            token=valid_token,
        )
        body2 = json.loads(resp2["body"])

        assert 200 == resp2["statusCode"]
        assert len(body2["data"]) > 0

        # Pages should not overlap (by internal_id).
        ids_page1 = {row["internal_id"] for row in body1["data"]}
        ids_page2 = {row["internal_id"] for row in body2["data"]}
        assert 0 == len(ids_page1 & ids_page2)


class TestStatsHealthEndpoint:
    """Health endpoint tests for the stats Lambda."""

    def test_health_returns_200(self, stats_client: EventStatsTestClient) -> None:
        """Test GET /health on stats Lambda returns 200."""
        response = stats_client.get_health()

        assert 200 == response["statusCode"]
        body = json.loads(response["body"])
        assert "ok" == body["status"]
