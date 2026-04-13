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


import time
import uuid

import pytest

from tests.integration.conftest import EventGateTestClient, EventStatsTestClient


def _make_test_event() -> dict:
    """Build a minimal runs event payload."""
    now_ms = int(time.time() * 1000)
    return {
        "event_id": str(uuid.uuid4()),
        "job_ref": f"conn-reuse-{uuid.uuid4().hex[:8]}",
        "tenant_id": "CONN_REUSE_TEST",
        "source_app": "integration-conn-reuse",
        "source_app_version": "1.0.0",
        "environment": "test",
        "timestamp_start": now_ms - 60000,
        "timestamp_end": now_ms,
        "jobs": [
            {
                "catalog_id": "db.schema.conn_reuse_table",
                "status": "succeeded",
                "timestamp_start": now_ms - 60000,
                "timestamp_end": now_ms,
            }
        ],
    }


class TestWriterConnectionReuse:
    """Verify that WriterPostgres reuses connections across invocations."""

    @pytest.fixture(scope="class", autouse=True)
    def seed_events(self, eventgate_client: EventGateTestClient, valid_token: str) -> None:
        """Post events so the writer connection is established."""
        for _ in range(2):
            event = _make_test_event()
            response = eventgate_client.post_event("public.cps.za.runs", event, token=valid_token)
            assert 202 == response["statusCode"]

    def test_writer_reuses_connection_across_writes(
        self, seed_events: None, eventgate_client: EventGateTestClient, valid_token: str
    ) -> None:
        """Test that subsequent writes reuse the same cached connection."""
        from src.event_gate_lambda import writers

        writer = writers["postgres"]
        conn_before = writer._connection
        assert conn_before is not None
        assert 0 == conn_before.closed

        event = _make_test_event()
        response = eventgate_client.post_event("public.cps.za.runs", event, token=valid_token)
        assert 202 == response["statusCode"]
        assert conn_before is writer._connection


class TestReaderConnectionReuse:
    """Verify that ReaderPostgres reuses connections across invocations."""

    @pytest.fixture(scope="class", autouse=True)
    def seed_events(self, eventgate_client: EventGateTestClient, valid_token: str) -> None:
        """Seed events so stats queries return data."""
        for _ in range(2):
            event = _make_test_event()
            response = eventgate_client.post_event("public.cps.za.runs", event, token=valid_token)
            assert 202 == response["statusCode"]

    def test_reader_reuses_connection_across_reads(self, seed_events: None, stats_client: EventStatsTestClient) -> None:
        """Test that successive queries reuse the same cached connection."""
        from src.event_stats_lambda import reader_postgres

        stats_client.post_stats("public.cps.za.runs", {})

        conn_after_first = reader_postgres._connection
        assert conn_after_first is not None
        assert 0 == conn_after_first.closed

        stats_client.post_stats("public.cps.za.runs", {})
        assert conn_after_first is reader_postgres._connection
