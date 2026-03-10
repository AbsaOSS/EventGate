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

import boto3
import psycopg2
import pytest
from confluent_kafka import Consumer

from tests.integration.conftest import EventGateTestClient
from tests.integration.utils.jwt_helper import generate_token
from tests.integration.utils.utils import create_runs_event


class TestTopicsListEndpoint:
    """Tests for GET /topics endpoint."""

    def test_get_topics_returns_200(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /topics returns successful response."""
        response = eventgate_client.get_topics()

        assert 200 == response["statusCode"]

    def test_get_topics_includes_list_of_test_topic(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /topics includes the test topic."""
        response = eventgate_client.get_topics()

        body = json.loads(response["body"])
        assert isinstance(body, list)
        assert "public.cps.za.test" in body


class TestTopicSchemaEndpoint:
    """Tests for GET /topics/{topic_name} endpoint."""

    def test_get_topic_schema_returns_200(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /topics/{topic_name} returns schema."""
        response = eventgate_client.get_topic_schema("public.cps.za.test")

        assert 200 == response["statusCode"]

    def test_get_topic_schema_contains_properties(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /topics/{topic_name} returns valid JSON schema."""
        response = eventgate_client.get_topic_schema("public.cps.za.test")

        body = json.loads(response["body"])
        assert "object" == body["type"]
        assert "properties" in body
        assert "event_id" in body["properties"]

    def test_get_topic_schema_nonexistent_returns_404(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /topics/{topic_name} returns 404 for nonexistent topic."""
        response = eventgate_client.get_topic_schema("nonexistent.topic")

        assert 404 == response["statusCode"]


class TestPostEventEndpoint:
    """Tests for POST /topics/{topic_name} endpoint."""

    @pytest.fixture
    def valid_event(self) -> dict:
        """Generate a valid run event."""
        return create_runs_event(
            event_id=str(uuid.uuid4()),
            job_ref="spark-app-001",
            tenant_id="TEST",
            source_app="integration-test",
            catalog_id="db.schema.table",
        )

    def test_post_event_without_token_returns_401(
        self, eventgate_client: EventGateTestClient, valid_event: dict
    ) -> None:
        """Test POST without JWT returns 401."""
        response = eventgate_client.post_event(
            "public.cps.za.runs",
            valid_event,
            token=None,
        )

        assert 401 == response["statusCode"]

    def test_post_event_with_valid_token_returns_202(
        self, eventgate_client: EventGateTestClient, valid_event: dict, valid_token: str
    ) -> None:
        """Test POST with valid JWT returns accepted status."""
        response = eventgate_client.post_event(
            "public.cps.za.runs",
            valid_event,
            token=valid_token,
        )

        assert 202 == response["statusCode"]

    def test_post_event_invalid_schema_returns_400(
        self, eventgate_client: EventGateTestClient, valid_token: str
    ) -> None:
        """Test POST with invalid event schema returns 400."""
        invalid_event = {"invalid_field": "value"}

        response = eventgate_client.post_event(
            "public.cps.za.runs",
            invalid_event,
            token=valid_token,
        )

        assert 400 == response["statusCode"]

    def test_post_event_unauthorized_user_returns_403(
        self, eventgate_client: EventGateTestClient, valid_event: dict, jwt_keypair: Dict[str, Any]
    ) -> None:
        """Test POST from unauthorized user returns 403."""
        unauthorized_token = generate_token(jwt_keypair["private_key_pem"], "UnauthorizedUser")
        response = eventgate_client.post_event(
            "public.cps.za.runs",
            valid_event,
            token=unauthorized_token,
        )

        assert 403 == response["statusCode"]

    def test_post_event_nonexistent_topic_returns_404(
        self, eventgate_client: EventGateTestClient, valid_event: dict, valid_token: str
    ) -> None:
        """Test POST to nonexistent topic returns 404."""
        response = eventgate_client.post_event(
            "nonexistent.topic",
            valid_event,
            token=valid_token,
        )

        assert 404 == response["statusCode"]


class TestPostEventWriterVerification:
    """Verify events are dispatched after a successful POST."""

    @pytest.fixture(scope="class")
    def posted_event(self, eventgate_client: EventGateTestClient, valid_token: str) -> Dict[str, Any]:
        """Post a unique runs event and return its payload for downstream assertions."""
        event = create_runs_event(
            event_id=str(uuid.uuid4()),
            job_ref="spark-verify-001",
            tenant_id="VERIFY",
            source_app="integration-test-verify",
            catalog_id="db.schema.verify_table",
        )
        response = eventgate_client.post_event("public.cps.za.runs", event, token=valid_token)
        assert 202 == response["statusCode"]
        return event

    def test_event_received_by_kafka(self, posted_event: Dict[str, Any], kafka_container: str) -> None:
        """Verify the posted event is consumable from the Kafka topic."""
        consumer = Consumer(
            {
                "bootstrap.servers": kafka_container,
                "group.id": f"test-verify-{uuid.uuid4()}",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe(["public.cps.za.runs"])

        try:
            messages = []
            deadline = time.time() + 10
            while time.time() < deadline:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    continue
                messages.append(json.loads(msg.value().decode("utf-8")))
                if any(m["event_id"] == posted_event["event_id"] for m in messages):
                    break

            matched = [m for m in messages if m["event_id"] == posted_event["event_id"]]
            assert 1 == len(matched)
            assert posted_event["tenant_id"] == matched[0]["tenant_id"]
        finally:
            consumer.close()

    def test_eventbridge_bus_reachable(self, localstack_container: dict) -> None:
        """Verify EventBridge event bus exists and is reachable after event dispatch."""
        client = boto3.client(
            "events",
            endpoint_url=localstack_container["url"],
            region_name=localstack_container["region"],
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        response = client.describe_event_bus(Name="default")
        assert "Arn" in response

    def test_event_received_by_postgres(self, posted_event: Dict[str, Any], postgres_container: str) -> None:
        """Verify the posted event was inserted into the PostgreSQL runs tables."""
        conn = psycopg2.connect(postgres_container)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT event_id, tenant_id FROM public_cps_za_runs WHERE event_id = %s",
                    (posted_event["event_id"],),
                )
                run_row = cursor.fetchone()
                assert run_row is not None
                assert posted_event["event_id"] == run_row[0]
                assert posted_event["tenant_id"] == run_row[1]

                cursor.execute(
                    "SELECT event_id, catalog_id, status FROM public_cps_za_runs_jobs WHERE event_id = %s",
                    (posted_event["event_id"],),
                )
                job_row = cursor.fetchone()
                assert job_row is not None
                assert posted_event["event_id"] == job_row[0]
                assert posted_event["jobs"][0]["catalog_id"] == job_row[1]
                assert posted_event["jobs"][0]["status"] == job_row[2]
        finally:
            conn.close()
