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
            "input_arguments": {"pipelineId": 1234, "currentDate": "2026-05-19", "triggerType": "SCHEDULE"},
            "definition_id": "1234",
            "status_type": "RUNNING",
        }
        response = eventgate_client.post_event(_TOPIC, event, token=valid_token)
        assert 202 == response["statusCode"]
        return event

    def test_row_is_inserted_in_job_table(
        self, created_event: Dict[str, Any], postgres_container: str
    ) -> None:
        """A job row must exist after posting a JobCreatedAndStartedEvent."""
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, created_event["job_id"])
            assert row is not None
        finally:
            conn.close()

    def test_source_and_platform_fields_populated(
        self, created_event: Dict[str, Any], postgres_container: str
    ) -> None:
        """source_app, platform, tenant_id and environment must be stored from the event."""
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, created_event["job_id"])
            assert row is not None
            assert created_event["source_app"] == row["source_app"]
            assert created_event["platform"] == row["platform"]
            assert created_event["tenant_id"] == row["tenant_id"]
            assert created_event["environment"] == row["environment"]
        finally:
            conn.close()

    def test_status_type_is_running(
        self, created_event: Dict[str, Any], postgres_container: str
    ) -> None:
        """status_type must be RUNNING after a JobCreatedAndStartedEvent."""
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, created_event["job_id"])
            assert row is not None
            assert "RUNNING" == row["status_type"]
        finally:
            conn.close()

    def test_lifecycle_timestamps_set(
        self, created_event: Dict[str, Any], postgres_container: str
    ) -> None:
        """created_at, started_at and last_updated_at must be derived from timestamp_event."""
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, created_event["job_id"])
            assert row is not None
            assert row["created_at"] is not None
            assert row["started_at"] is not None
            assert row["last_updated_at"] is not None
        finally:
            conn.close()

    def test_input_arguments_stored_as_json(
        self, created_event: Dict[str, Any], postgres_container: str
    ) -> None:
        """input_arguments must be stored as JSONB and round-trip correctly."""
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, created_event["job_id"])
            assert row is not None
            assert created_event["input_arguments"] == row["input_arguments"]
        finally:
            conn.close()

    def test_attempt_number_defaults_to_one(
        self, created_event: Dict[str, Any], postgres_container: str
    ) -> None:
        """attempt_number must default to 1 when not present in the event."""
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, created_event["job_id"])
            assert row is not None
            assert 1 == row["attempt_number"]
        finally:
            conn.close()


class TestStatusChangeJobUpdated:
    """Verify a JobUpdatedEvent merges into an existing job row."""

    @pytest.fixture(scope="class")
    def updated_event_pair(
        self, eventgate_client: EventGateTestClient, valid_token: str
    ) -> Dict[str, Any]:
        """Post a JobCreatedAndStartedEvent followed by a JobUpdatedEvent."""
        job_id = str(uuid.uuid4())
        create_event: Dict[str, Any] = {
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
            "definition_id": "1234",
            "status_type": "RUNNING",
        }
        update_event: Dict[str, Any] = {
            "event_type": "JobUpdatedEvent",
            "event_id": str(uuid.uuid4()),
            "timestamp_event": 1747646447000,
            "job_id": job_id,
            "status_type": "RUNNING",
            "additional_context": {"pipeline_name": "MYPIPELINE"},
        }
        for event in (create_event, update_event):
            response = eventgate_client.post_event(_TOPIC, event, token=valid_token)
            assert 202 == response["statusCode"]
        return {"job_id": job_id, "create_event": create_event, "update_event": update_event}

    def test_additional_context_populated_after_update(
        self, updated_event_pair: Dict[str, Any], postgres_container: str
    ) -> None:
        """additional_context must be set after a JobUpdatedEvent with context."""
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, updated_event_pair["job_id"])
            assert row is not None
            assert {"pipeline_name": "MYPIPELINE"} == row["additional_context"]
        finally:
            conn.close()

    def test_single_row_after_create_and_update(
        self, updated_event_pair: Dict[str, Any], postgres_container: str
    ) -> None:
        """Two events for the same job_id must produce exactly one row."""
        conn = psycopg2.connect(postgres_container)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM public_cps_za_status_change_aggregated_job WHERE job_id = %s",
                    (updated_event_pair["job_id"],),
                )
                count = cursor.fetchone()[0]
            assert 1 == count
        finally:
            conn.close()


class TestStatusChangeJobFinished:
    """Verify a JobFinishedEvent sets terminal status and timestamps."""

    @pytest.fixture(scope="class")
    def finished_event_pair(
        self, eventgate_client: EventGateTestClient, valid_token: str
    ) -> Dict[str, Any]:
        """Post a JobCreatedAndStartedEvent followed by a FAILED JobFinishedEvent."""
        job_id = str(uuid.uuid4())
        create_event: Dict[str, Any] = {
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
            "definition_id": "1234",
            "status_type": "RUNNING",
        }
        finish_event: Dict[str, Any] = {
            "event_type": "JobFinishedEvent",
            "event_id": str(uuid.uuid4()),
            "timestamp_event": 1747646705000,
            "job_id": job_id,
            "status_type": "FAILED",
            "status_subtype": "HIVE_TABLE_UPDATE_FAILED",
            "status_detail": "PKIX path building failed.",
        }
        for event in (create_event, finish_event):
            response = eventgate_client.post_event(_TOPIC, event, token=valid_token)
            assert 202 == response["statusCode"]
        return {"job_id": job_id, "finish_event": finish_event}

    def test_status_type_is_failed(
        self, finished_event_pair: Dict[str, Any], postgres_container: str
    ) -> None:
        """status_type must be FAILED after a JobFinishedEvent."""
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, finished_event_pair["job_id"])
            assert row is not None
            assert "FAILED" == row["status_type"]
        finally:
            conn.close()

    def test_status_subtype_and_detail_set(
        self, finished_event_pair: Dict[str, Any], postgres_container: str
    ) -> None:
        """status_subtype and status_detail must be stored from the finish event."""
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, finished_event_pair["job_id"])
            assert row is not None
            assert finished_event_pair["finish_event"]["status_subtype"] == row["status_subtype"]
            assert finished_event_pair["finish_event"]["status_detail"] == row["status_detail"]
        finally:
            conn.close()

    def test_finished_at_is_set(
        self, finished_event_pair: Dict[str, Any], postgres_container: str
    ) -> None:
        """finished_at must be populated from the JobFinishedEvent timestamp."""
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, finished_event_pair["job_id"])
            assert row is not None
            assert row["finished_at"] is not None
        finally:
            conn.close()

    def test_timestamps_stored_as_timestamptz(
        self, finished_event_pair: Dict[str, Any], postgres_container: str
    ) -> None:
        """created_at, started_at, finished_at and last_updated_at must be stored
        as TIMESTAMPTZ values matching the epoch-millisecond timestamps from the events."""
        expected_create_ts = datetime.fromtimestamp(1747646400000 / 1000, tz=timezone.utc)
        expected_finish_ts = datetime.fromtimestamp(1747646705000 / 1000, tz=timezone.utc)
        conn = psycopg2.connect(postgres_container)
        try:
            row = _fetch_job_row(conn, finished_event_pair["job_id"])
            assert row is not None
            assert expected_create_ts == row["created_at"]
            assert expected_create_ts == row["started_at"]
            assert expected_finish_ts == row["finished_at"]
            assert expected_finish_ts == row["last_updated_at"]
        finally:
            conn.close()

class TestNestedJobsScenario:

    @pytest.fixture(scope="class")
    def nested_jobs_posted(
        self, eventgate_client: EventGateTestClient, valid_token: str
    ) -> Dict[str, Any]:
        """Post a JobCreatedAndStartedEvent followed by a FAILED JobFinishedEvent."""
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
            "status_type": "COMPLETED",
        }
        # Older timestamp — must not overwrite the terminal COMPLETED state.
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
            assert "COMPLETED" == row["status_type"]
            assert row["status_subtype"] is None
            assert row["status_detail"] is None
        finally:
            conn.close()
