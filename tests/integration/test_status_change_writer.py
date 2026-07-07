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

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import psycopg2
import pytest

from tests.integration.conftest import EventGateTestClient

_TOPIC = "public.cps.za.status_change"


def _fetch_job_row(conn: Any, job_id: str) -> dict[str, Any] | None:
    """Fetch a single row from the job table by job_id as a column-keyed dict."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM public_cps_za_status_change_aggregated_job WHERE job_id = %s", (job_id,))
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
    if row is None:
        return None
    return dict(zip(columns, row))


class TestStatusChangeJobCreatedAndStarted:
    """Verify a JobCreatedAndStartedEvent is upserted into the job table."""

    @pytest.fixture(scope="class")
    def created_event(self, eventgate_client: EventGateTestClient, valid_token: str) -> Dict[str, Any]:
        """Post a JobCreatedAndStartedEvent and return the payload."""
        job_id = str(uuid.uuid4())
        event: Dict[str, Any] = {
            "event_type": "JobCreatedAndStartedEvent",
            "event_id": str(uuid.uuid4()),
            "job_ref": "myJobRef",
            "tenant_id": "abcd",
            "source_app": "ingestapp",
            "source_app_version": "2.14.0",
            "environment": "dev",
            "timestamp_event": 1747646400000,
            "country": "za",
            "job_id": job_id,
            "parent_job_id": str(uuid.uuid4()),
            "initial_job_id": str(uuid.uuid4()),            
            "job_group_id": job_id,
            "job_name": "IngestApp Ingestion",
            "platform": "aws.stepfunctions",
            "platform_metadata": {"platform_key": "platform_value"},
            "input_arguments": {"pipelineId": 1234, "currentDate": "2026-05-19", "triggerType": "SCHEDULE"},
            "definition_id": "1234",
            "definition_version": "v1.0",
            "status_type": "RUNNING",
            "status_subtype": "myStatusSubtype",
            "status_detail": "myStatusDetail",
            "additional_context": {"context_key": "context_value"},
        }
        response = eventgate_client.post_event(_TOPIC, event, token=valid_token)
        assert 202 == response["statusCode"]
        return event

    def test_all_fields_populated(
        self, created_event: Dict[str, Any], postgres_container: str
    ) -> None:
        """source_app, platform, tenant_id and environment must be stored from the event."""
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, created_event["job_id"])
            assert row is not None                       
            assert created_event["job_id"] == str(row["job_id"])
            assert created_event["job_group_id"] == str(row["job_group_id"])
            assert created_event["parent_job_id"] == str(row["parent_job_id"])
            assert created_event["initial_job_id"] == str(row["initial_job_id"])
            assert created_event["job_ref"] == row["job_ref"]
            assert created_event["job_name"] == row["job_name"]
            assert created_event["definition_id"] == row["definition_id"]
            assert created_event["definition_version"] == row["definition_version"]
            assert created_event["tenant_id"] == row["tenant_id"]
            assert created_event["country"] == row["country"]
            assert created_event["source_app"] == row["source_app"]
            assert created_event["source_app_version"] == row["source_app_version"]
            assert created_event["environment"] == row["environment"]
            assert created_event["platform"] == row["platform"]
            assert created_event["platform_metadata"] == row["platform_metadata"]
            assert created_event["input_arguments"] == row["input_arguments"]
            assert created_event["additional_context"] == row["additional_context"]
            assert 1 == row["attempt_number"]
            assert created_event["status_type"] == row["status_type"]
            assert created_event["status_subtype"] == row["status_subtype"]
            assert created_event["status_detail"] == row["status_detail"]
            ts = datetime.fromtimestamp(created_event["timestamp_event"] / 1000, tz=timezone.utc)
            assert ts == row["started_at"]
            assert ts == row["created_at"]
            assert None is row["finished_at"]
            assert ts == row["last_updated_at"]

        finally:
            conn.close()


class TestStatusChangeJobFinished:
    """Verify a JobFinishedEvent overwrites terminal status and timestamps."""
    def create_event(self, job_id) -> Dict[str, Any]:
        return {
            "event_type": "JobCreatedAndStartedEvent",
            "event_id": str(uuid.uuid4()),
            "job_ref": "myJobRef",
            "tenant_id": "abcd",
            "source_app": "ingestapp",
            "source_app_version": "2.14.0",
            "environment": "dev",
            "timestamp_event": 1000000000000,
            "country": "za",
            "job_id": job_id,
            "parent_job_id": str(uuid.uuid4()),
            "initial_job_id": str(uuid.uuid4()),            
            "job_group_id": job_id,
            "job_name": "IngestApp Ingestion",
            "attempt_number": 2,
            "platform": "aws.stepfunctions",
            "platform_metadata": {"create_platform_key": "create_platform_value"},
            "input_arguments": {"pipelineId": 1234, "currentDate": "2026-05-19", "triggerType": "SCHEDULE"},
            "definition_id": "1234",
            "definition_version": "v1.0",
            "status_type": "RUNNING",
            "status_subtype": "myStatusSubtype",
            "status_detail": "myStatusDetail",
            "additional_context": {"create_key": "create_value", "common_key": "create_value"},
        }
    def finish_event(self, job_id) -> Dict[str, Any]:
        return {
            "event_type": "JobFinishedEvent",
            "event_id": str(uuid.uuid4()),
            "timestamp_event": 2000000000000,
            "job_id": job_id,
            "status_type": "SUCCEEDED",
            "additional_context": {"finish_key": "finish_value", "common_key": "finish_value"},
            "platform_metadata": {"finish_platform_key": "finish_platform_value"},
        }

    @pytest.fixture(scope="class")
    def in_order_events(
        self, eventgate_client: EventGateTestClient, valid_token: str
    ) -> Dict[str, Any]:
        """Post a JobCreatedAndStartedEvent followed by a SUCCEEDED JobFinishedEvent."""
        job_id = str(uuid.uuid4())
        create_event = self.create_event(job_id)
        finish_event = self.finish_event(job_id)
        for event in (create_event, finish_event):
            response = eventgate_client.post_event(_TOPIC, event, token=valid_token)
            assert 202 == response["statusCode"]
        return create_event

    @pytest.fixture(scope="class")
    def out_of_order_events(
        self, eventgate_client: EventGateTestClient, valid_token: str
    ) -> Dict[str, Any]:
        """First post a JobFinishedEvent followed by a JobCreatedAndStartedEvent."""
        job_id = str(uuid.uuid4())
        create_event = self.create_event(job_id)
        finish_event = self.finish_event(job_id)
        for event in (finish_event, create_event):
            response = eventgate_client.post_event(_TOPIC, event, token=valid_token)
            assert 202 == response["statusCode"]
        return create_event

    def _test_all_fields(
        self, create_event: Dict[str, Any], postgres_container: str
    ) -> None:        
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, create_event["job_id"])

            assert create_event["job_id"] == str(row["job_id"])
            assert create_event["job_group_id"] == str(row["job_group_id"])
            assert create_event["parent_job_id"] == str(row["parent_job_id"])
            assert create_event["initial_job_id"] == str(row["initial_job_id"])
            assert create_event["job_ref"] == row["job_ref"]
            assert create_event["job_name"] == row["job_name"]
            assert create_event["definition_id"] == row["definition_id"]
            assert create_event["definition_version"] == row["definition_version"]
            assert create_event["tenant_id"] == row["tenant_id"]
            assert create_event["country"] == row["country"]
            assert create_event["source_app"] == row["source_app"]
            assert create_event["source_app_version"] == row["source_app_version"]
            assert create_event["environment"] == row["environment"]
            assert create_event["platform"] == row["platform"]
            assert {"finish_platform_key": "finish_platform_value"} == row["platform_metadata"]
            assert create_event["input_arguments"] == row["input_arguments"]
            # Cumulative merge of additional_context
            assert {"create_key": "create_value", "finish_key": "finish_value", "common_key": "finish_value"} == row["additional_context"]
            assert create_event["attempt_number"] == 2
            # Take latest with nulls for status fields
            assert "SUCCEEDED" == row["status_type"]
            assert None is row["status_subtype"]
            assert None is row["status_detail"]
            ts_create = datetime.fromtimestamp(1000000000000 / 1000, tz=timezone.utc)
            ts_finish = datetime.fromtimestamp(2000000000000 / 1000, tz=timezone.utc)
            assert ts_create == row["started_at"]
            assert ts_create == row["created_at"]
            assert ts_finish == row["finished_at"]
            assert ts_finish == row["last_updated_at"]
        finally:
            conn.close()

    def test_all_fields_with_in_order_events(
        self, in_order_events: Dict[str, Any], postgres_container: str
    ) -> None:
        """Status fields must use latest values, even if null, and timestamps must be updated to the latest event."""
        self._test_all_fields(in_order_events, postgres_container)

    def test_all_fields_with_out_of_order_events(
        self, out_of_order_events: Dict[str, Any], postgres_container: str
    ) -> None:
        """Out of order events must lead to same outcome as in order events"""
        self._test_all_fields(out_of_order_events, postgres_container)


    def test_single_row_after_create_and_finish(
        self, in_order_events: Dict[str, Any], postgres_container: str
    ) -> None:
        """Two events for the same job_id must produce exactly one row."""
        conn = psycopg2.connect(postgres_container)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM public_cps_za_status_change_aggregated_job WHERE job_id = %s",
                    (in_order_events["job_id"],),
                )
                count = cursor.fetchone()[0]
            assert 1 == count
        finally:
            conn.close()


class TestNestedJobsScenario:

    @pytest.fixture(scope="class")
    def nested_jobs_posted(
        self, eventgate_client: EventGateTestClient, valid_token: str
    ) -> Dict[str, Any]:
        """Post a JobCreatedAndStartedEvent followed by a nested event."""
        job_id = str(uuid.uuid4())
        parent_event: Dict[str, Any] = {
            "event_type": "JobCreatedAndStartedEvent",
            "event_id": str(uuid.uuid4()),
            "tenant_id": "abcd",
            "source_app": "ingestapp",
            "source_app_version": "2.14.0",
            "environment": "dev",
            "timestamp_event": 1747646400000,
            "country": "za",
            "job_id": job_id,
            "job_group_id": job_id,
            "job_name": "IngestApp Ingestion",
            "platform": "aws.stepfunctions",
            "input_arguments": {
                "pipelineId": 1234,
                "currentDate": "2026-05-19",
                "triggerType": "SCHEDULE"
            },
            "definition_id": "1234",
            "status_type": "RUNNING"
        }

        nested_event: Dict[str, Any] = {
            "event_type": "JobCreatedAndStartedEvent",
            "event_id": str(uuid.uuid4()),
            "tenant_id": "abcd",
            "source_app": "ingestapp",
            "source_app_version": "2.14.0",
            "environment": "dev",
            "timestamp_event": 1747646410000,
            "country": "za",
            "job_id": str(uuid.uuid4()),
            "parent_job_id": job_id,
            "job_group_id": job_id,
            "job_name": "Create Pramen Config",
            "platform": "aws.lambda",
            "input_arguments": {
                "pipelineId": 1234,
                "currentDate": "2026-05-19"
            },
            "definition_id": "1234",
            "status_type": "RUNNING"
        }

        for event in (parent_event, nested_event):
            response = eventgate_client.post_event(_TOPIC, event, token=valid_token)
            assert 202 == response["statusCode"]
        return {"job_id": job_id, "parent_event": parent_event, "nested_event": nested_event}

    def test_parent_job_row_created(
        self, nested_jobs_posted: Dict[str, Any], postgres_container: str
    ) -> None:
        """The parent job must have a row in the job table."""
        conn = psycopg2.connect(postgres_container)
        try:
            parent_row = _fetch_job_row(conn, nested_jobs_posted["parent_event"]["job_id"])
            assert parent_row is not None
            assert "RUNNING" == parent_row["status_type"]

        finally:
            conn.close()


    def test_nested_job_row_created(
        self, nested_jobs_posted: Dict[str, Any], postgres_container: str
    ) -> None:
        """The nested job must have a row in the job table and the parent job id must be set."""
        conn = psycopg2.connect(postgres_container)
        try:
            nested_row = _fetch_job_row(conn, nested_jobs_posted["nested_event"]["job_id"])
            assert nested_row is not None
            assert "RUNNING" == nested_row["status_type"]
            assert nested_jobs_posted["parent_event"]["job_id"] == str(nested_row["parent_job_id"])
        finally:
            conn.close()

class TestJobRetryScenario:
    @pytest.fixture(scope="class")
    def retry_jobs_posted(
        self, eventgate_client: EventGateTestClient, valid_token: str
    ) -> Dict[str, Any]:
        """Post a JobCreatedAndStartedEvent followed by a FAILED JobFinishedEvent."""
        initial_job_id = str(uuid.uuid4())
        initial_event: Dict[str, Any] = {
            "event_type": "JobCreatedAndStartedEvent",
            "event_id": str(uuid.uuid4()),
            "tenant_id": "abcd",
            "source_app": "ingestapp",
            "source_app_version": "2.14.0",
            "environment": "dev",
            "timestamp_event": 1747646400000,
            "country": "za",
            "job_id": initial_job_id,
            "job_group_id": initial_job_id,
            "job_name": "IngestApp Ingestion",
            "platform": "aws.stepfunctions",
            "input_arguments": {
                "pipelineId": 1234,
                "currentDate": "2026-05-19",
                "triggerType": "SCHEDULE"
            },
            "definition_id": "1234",
            "status_type": "RUNNING"
        }

        retry_event: Dict[str, Any] = {
            "event_type": "JobCreatedAndStartedEvent",
            "event_id": str(uuid.uuid4()),
            "source_app": "ingestapp",
            "source_app_version": "2.14.0",
            "environment": "dev",
            "timestamp_event": 1747646800000,
            "job_id": str(uuid.uuid4()),
            "initial_job_id": initial_job_id,
            "job_name": "IngestApp Ingestion",
            "attempt_number": 2,
            "platform": "aws.glue",
            "input_arguments": {
                "pipelineId": 1234,
                "currentDate": "2026-05-19"
            },
            "definition_id": "1234",
            "status_type": "RUNNING"
        }


        for event in (initial_event, retry_event):
            response = eventgate_client.post_event(_TOPIC, event, token=valid_token)
            assert 202 == response["statusCode"]
        return {"job_id": initial_job_id, "initial_event": initial_event, "retry_event": retry_event}

    def test_initial_job_row_created(
        self, retry_jobs_posted: Dict[str, Any], postgres_container: str
    ) -> None:
        """The initial job must have a row in the job table."""
        conn = psycopg2.connect(postgres_container)
        try:
            initial_row = _fetch_job_row(conn, retry_jobs_posted["initial_event"]["job_id"])
            assert initial_row is not None
            assert "RUNNING" == initial_row["status_type"]

        finally:
            conn.close()


    def test_retry_job_row_created(
        self, retry_jobs_posted: Dict[str, Any], postgres_container: str
    ) -> None:
        """The retry job must have a row in the job table and the initial job id must be set."""
        conn = psycopg2.connect(postgres_container)
        try:
            retry_row = _fetch_job_row(conn, retry_jobs_posted["retry_event"]["job_id"])
            assert retry_row is not None
            assert "RUNNING" == retry_row["status_type"]
            assert retry_jobs_posted["initial_event"]["job_id"] == str(retry_row["initial_job_id"])
            assert 2 == retry_row["attempt_number"]
        finally:
            conn.close()


class TestStatusChangeIdempotency:
    """Verify that posting the same event twice produces exactly one job row."""

    def test_duplicate_event_produces_one_row(
        self,
        eventgate_client: EventGateTestClient,
        valid_token: str,
        postgres_container: str,
    ) -> None:
        """Posting the identical event twice must not create duplicate rows."""
        job_id = str(uuid.uuid4())
        event: Dict[str, Any] = {
            "event_type": "JobCreatedAndStartedEvent",
            "event_id": str(uuid.uuid4()),
            "tenant_id": "abcd",
            "source_app": "ingestapp",
            "source_app_version": "2.14.0",
            "environment": "dev",
            "timestamp_event": 1747646400000,
            "job_id": job_id,
            "job_group_id": job_id,
            "job_name": "Idempotency Test Job",
            "platform": "aws.stepfunctions",
            "definition_id": "1234",
            "status_type": "RUNNING",
            "additional_context": {"test_key": "test_value"},
        }
        for _ in range(2):
            response = eventgate_client.post_event(_TOPIC, event, token=valid_token)
            assert 202 == response["statusCode"]
        conn = psycopg2.connect(postgres_container)
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM public_cps_za_status_change_aggregated_job WHERE job_id = %s", (job_id,))
                count = cursor.fetchone()[0]
            row = _fetch_job_row(conn, job_id)

            assert 1 == count
            assert {"test_key": "test_value"} == row["additional_context"]
        finally:
            conn.close()

class TestStatusChangeOutOfOrder:
    """Verify that a late-arriving event does not overwrite a newer terminal status."""

    def test_late_event_does_not_overwrite_terminal_status(
        self,
        eventgate_client: EventGateTestClient,
        valid_token: str,
        postgres_container: str,
    ) -> None:
        """A JobUpdatedEvent with an older timestamp must not overwrite a FAILED status."""
        job_id = str(uuid.uuid4())
        create_event: Dict[str, Any] = {
            "event_type": "JobCreatedAndStartedEvent",
            "event_id": str(uuid.uuid4()),
            "tenant_id": "abcd",
            "source_app": "ingestapp",
            "source_app_version": "2.14.0",
            "environment": "dev",
            "timestamp_event": 1000000000000,
            "job_id": job_id,
            "job_group_id": job_id,
            "job_name": "Out-of-Order Test Job",
            "platform": "aws.stepfunctions",
            "definition_id": "1234",
            "status_type": "RUNNING",
        }
        finish_event: Dict[str, Any] = {
            "event_type": "JobFinishedEvent",
            "event_id": str(uuid.uuid4()),
            "timestamp_event": 2000000000000,
            "job_id": job_id,
            "status_type": "FAILED",
            "status_subtype": "TIMEOUT",
            "status_detail": "Job timed out.",
        }
        # Older timestamp — must not overwrite the terminal FAILED state.
        late_update_event: Dict[str, Any] = {
            "event_type": "JobUpdatedEvent",
            "event_id": str(uuid.uuid4()),
            "timestamp_event": 500000000000,
            "job_id": job_id,
            "input_arguments": {"myInput": 1234},
            "attempt_number": 3,
            "status_type": "RUNNING",
            "platform_metadata": {"platform_key": "platform_value"},
            "additional_context": {"late_key": "late_value"},
        }
        for event in (create_event, finish_event, late_update_event):
            response = eventgate_client.post_event(_TOPIC, event, token=valid_token)
            assert 202 == response["statusCode"]
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, job_id)
            assert row is not None
            assert "FAILED" == row["status_type"]
            assert "TIMEOUT" == row["status_subtype"]
            assert "Job timed out." == row["status_detail"]
            assert 3 == row["attempt_number"]
            assert {"myInput": 1234} == row["input_arguments"]
            assert {"platform_key": "platform_value"} == row["platform_metadata"]
            assert {"late_key": "late_value"} == row["additional_context"]
        finally:
            conn.close()

    def test_late_started_event_does_not_overwrite_terminal_status(
        self,
        eventgate_client: EventGateTestClient,
        valid_token: str,
        postgres_container: str,
    ) -> None:
        """A late-arriving JobStartedEvent must not overwrite the status set by a JobFinishedEvent.

        The JobFinishedEvent carries only status_type. The later-processed but older-timestamped
        JobStartedEvent carries status_type, status_subtype, and status_detail. None of those
        fields must overwrite the terminal row because the event timestamp is older.
        """
        job_id = str(uuid.uuid4())
        create_event: Dict[str, Any] = {
            "event_type": "JobCreatedAndStartedEvent",
            "event_id": str(uuid.uuid4()),
            "tenant_id": "abcd",
            "source_app": "ingestapp",
            "source_app_version": "2.14.0",
            "environment": "dev",
            "timestamp_event": 1000000000000,
            "job_id": job_id,
            "job_group_id": job_id,
            "job_name": "Late-Started Out-of-Order Test Job",
            "platform": "aws.stepfunctions",
            "definition_id": "1234",
            "status_type": "RUNNING",
        }
        # JobFinishedEvent has only status_type — no subtype or detail.
        finish_event: Dict[str, Any] = {
            "event_type": "JobFinishedEvent",
            "event_id": str(uuid.uuid4()),
            "timestamp_event": 2000000000000,
            "job_id": job_id,
            "status_type": "SUCCEEDED",
        }
        # Older timestamp — must not overwrite the terminal SUCCEEDED state.
        late_started_event: Dict[str, Any] = {
            "event_type": "JobStartedEvent",
            "event_id": str(uuid.uuid4()),
            "timestamp_event": 500000000000,
            "job_id": job_id,
            "status_type": "RUNNING",
            "status_subtype": "RETRY",
            "status_detail": "Retry attempt started.",
        }
        for event in (create_event, finish_event, late_started_event):
            response = eventgate_client.post_event(_TOPIC, event, token=valid_token)
            assert 202 == response["statusCode"]
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, job_id)
            assert row is not None
            assert "SUCCEEDED" == row["status_type"]
            assert row["status_subtype"] is None
            assert row["status_detail"] is None
        finally:
            conn.close()
