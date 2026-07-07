#
# Copyright 2025 ABSA Group Limited
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

"""Postgres writer for storing events in PostgreSQL database."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import cached_property
from pathlib import Path
from typing import Any

import aiosql
from botocore.exceptions import BotoCoreError, ClientError

from src.utils.constants import (
    REQUIRED_CONNECTION_FIELDS,
    TOPIC_DLCHANGE,
    TOPIC_RUNS,
    TOPIC_STATUS_CHANGE,
    TOPIC_TEST,
    POSTGRES_WRITE_TOPICS,
)
from src.utils.postgres_base import PsycopgError, PostgresBase
import src.utils.postgres_base as _pb
from src.utils.trace_logging import log_payload_at_trace
from src.writers.writer import HealthCheckError, WriteError, Writer

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).parent / "sql"


@dataclass(frozen=True)
class WriterQueries:
    """Typed holder for writer SQL query strings loaded via aiosql."""

    insert_dlchange: str
    insert_run: str
    insert_run_job: str
    insert_test: str
    upsert_status_change: str


class WriterPostgres(Writer, PostgresBase):
    """Postgres writer for storing events in PostgreSQL database."""

    def __init__(self, config: dict[str, Any]) -> None:
        Writer.__init__(self, config)
        PostgresBase.__init__(self)
        logger.debug("Initialized PostgreSQL writer.")

    @cached_property
    def _queries(self) -> WriterQueries:
        """Load SQL queries from the `sql/` directory via aiosql."""
        queries = aiosql.from_path(_SQL_DIR, "psycopg2")
        # aiosql loads SQL files and attaches query functions as dynamic attributes at runtime.
        # pylint cannot resolve these statically.
        return WriterQueries(
            insert_dlchange=queries.insert_dlchange.sql,  # pylint: disable=no-member
            insert_run=queries.insert_run.sql,  # pylint: disable=no-member
            insert_run_job=queries.insert_run_job.sql,  # pylint: disable=no-member
            insert_test=queries.insert_test.sql,  # pylint: disable=no-member
            upsert_status_change=queries.upsert_status_change.sql,  # pylint: disable=no-member
        )

    def _insert_dlchange(self, cursor: Any, message: dict[str, Any]) -> None:
        """Insert a dlchange style event row.
        Args:
            cursor: Database cursor.
            message: Event payload.
        """
        logger.debug("Sending to Postgres - dlchange.")
        cursor.execute(
            self._queries.insert_dlchange,
            {
                "event_id": message["event_id"],
                "tenant_id": message["tenant_id"],
                "source_app": message["source_app"],
                "source_app_version": message["source_app_version"],
                "environment": message["environment"],
                "timestamp_event": message["timestamp_event"],
                "country": message.get("country", ""),
                "catalog_id": message["catalog_id"],
                "operation": message["operation"],
                "location": message.get("location"),
                "format": message["format"],
                "format_options": (json.dumps(message.get("format_options")) if "format_options" in message else None),
                "additional_info": (
                    json.dumps(message.get("additional_info")) if "additional_info" in message else None
                ),
            },
        )

    def _insert_run(self, cursor: Any, message: dict[str, Any]) -> None:
        """Insert a run event row plus related job rows.
        Args:
            cursor: Database cursor.
            message: Event payload (includes jobs array).
        """
        logger.debug("Sending to Postgres - runs.")
        cursor.execute(
            self._queries.insert_run,
            {
                "event_id": message["event_id"],
                "job_ref": message["job_ref"],
                "tenant_id": message["tenant_id"],
                "source_app": message["source_app"],
                "source_app_version": message["source_app_version"],
                "environment": message["environment"],
                "timestamp_start": message["timestamp_start"],
                "timestamp_end": message["timestamp_end"],
            },
        )
        for job in message["jobs"]:
            cursor.execute(
                self._queries.insert_run_job,
                {
                    "event_id": message["event_id"],
                    "country": job.get("country", ""),
                    "catalog_id": job["catalog_id"],
                    "status": job["status"],
                    "timestamp_start": job["timestamp_start"],
                    "timestamp_end": job["timestamp_end"],
                    "message": job.get("message"),
                    "additional_info": (json.dumps(job.get("additional_info")) if "additional_info" in job else None),
                },
            )

    def _insert_test(self, cursor: Any, message: dict[str, Any]) -> None:
        """Insert a test topic row.
        Args:
            cursor: Database cursor.
            message: Event payload.
        """
        logger.debug("Sending to Postgres - test.")
        cursor.execute(
            self._queries.insert_test,
            {
                "event_id": message["event_id"],
                "tenant_id": message["tenant_id"],
                "source_app": message["source_app"],
                "environment": message["environment"],
                "timestamp_event": message["timestamp"],
                "additional_info": (
                    json.dumps(message.get("additional_info")) if "additional_info" in message else None
                ),
            },
        )

    def _upsert_status_change(self, cursor: Any, message: dict[str, Any]) -> None:
        """Upsert a status_change event into the aggregated job table.
        Args:
            cursor: Database cursor.
            message: Event payload.
        """
        logger.debug("Sending to Postgres - status_change.")
        ts = datetime.fromtimestamp(message["timestamp_event"] / 1000.0, tz=timezone.utc)
        event_type = message["event_type"]

        created_at: datetime | None = None
        started_at: datetime | None = None
        finished_at: datetime | None = None

        if event_type == "JobCreatedEvent":
            created_at = ts
        elif event_type == "JobCreatedAndStartedEvent":
            created_at = ts
            started_at = ts
        elif event_type == "JobStartedEvent":
            started_at = ts
        elif event_type == "JobFinishedEvent":
            finished_at = ts

        cursor.execute(
            self._queries.upsert_status_change,
            {
                "job_id": message["job_id"],
                "job_group_id": message.get("job_group_id"),
                "parent_job_id": message.get("parent_job_id"),
                "initial_job_id": message.get("initial_job_id"),
                "job_ref": message.get("job_ref"),
                "job_name": message.get("job_name"),
                "definition_id": message.get("definition_id"),
                "definition_version": message.get("definition_version"),
                "tenant_id": message.get("tenant_id"),
                "country": message.get("country"),
                "source_app": message.get("source_app"),
                "source_app_version": message.get("source_app_version"),
                "environment": message.get("environment") or "",
                "platform": message.get("platform"),
                "platform_metadata": (
                    json.dumps(message["platform_metadata"]) if message.get("platform_metadata") is not None else None
                ),
                "input_arguments": (
                    json.dumps(message["input_arguments"]) if message.get("input_arguments") is not None else None
                ),
                "additional_context": (
                    json.dumps(message["additional_context"]) if message.get("additional_context") is not None else None
                ),
                "attempt_number": message.get("attempt_number") or 1,
                "status_type": message.get("status_type"),
                "status_subtype": message.get("status_subtype"),
                "status_detail": message.get("status_detail"),
                "created_at": created_at,
                "started_at": started_at,
                "finished_at": finished_at,
                "last_updated_at": ts,
            },
        )

    def write(self, topic_name: str, message: dict[str, Any], message_key: str = "") -> None:
        """Dispatch insertion for a topic into the correct Postgres table(s).
        Args:
            topic_name: Incoming topic identifier.
            message: JSON-serializable payload.
            message_key: Optional transport key (unused by Postgres writer).
        Raises:
            WriteError: If publishing fails.
        """
        try:
            pg_config = self._pg_config
        except (RuntimeError, BotoCoreError, ClientError, ValueError, KeyError) as e:
            err_msg = f"The Postgres writer failed with unknown error: {e!s}"
            logger.exception(err_msg)
            raise WriteError(err_msg) from e

        if not pg_config.get("database"):
            logger.debug("No Postgres - skipping Postgres writer.")
            return

        missing = [field for field in REQUIRED_CONNECTION_FIELDS if not pg_config.get(field)]
        if missing:
            msg = f"PostgreSQL connection field '{missing[0]}' not configured."
            logger.error(msg)
            raise WriteError(msg)

        if not self._is_psycopg2_available():
            raise WriteError("psycopg2 is not available for the configured Postgres writer.")

        log_payload_at_trace(logger, "Postgres", topic_name, message)

        if topic_name not in POSTGRES_WRITE_TOPICS:
            msg = f"Unknown topic for Postgres/{topic_name}."
            logger.debug(msg)
            raise WriteError(msg)

        try:
            self._execute_with_retry(lambda conn: self._write_topic(conn, topic_name, message), retry=False)
        except (RuntimeError, PsycopgError, ValueError, KeyError) as e:
            self._close_connection()
            err_msg = f"The Postgres writer failed with unknown error: {e!s}"
            logger.exception(err_msg)
            raise WriteError(err_msg) from e

    def _write_topic(self, connection: Any, topic_name: str, message: dict[str, Any]) -> None:
        """Execute the insert for the given topic inside a transaction."""
        with connection.cursor() as cursor:
            if topic_name == TOPIC_DLCHANGE:
                self._insert_dlchange(cursor, message)
            elif topic_name == TOPIC_RUNS:
                self._insert_run(cursor, message)
            elif topic_name == TOPIC_TEST:
                self._insert_test(cursor, message)
            elif topic_name == TOPIC_STATUS_CHANGE:
                self._upsert_status_change(cursor, message)
        connection.commit()

    @staticmethod
    def _is_psycopg2_available() -> bool:
        """Check whether psycopg2 is importable."""
        return _pb.psycopg2 is not None

    def check_health(self) -> str | None:
        """Check PostgreSQL writer health.
        Returns:
            `None` when healthy, `"not configured"` when intentionally disabled.
        Raises:
            HealthCheckError: If the PostgreSQL configuration is invalid or incomplete.
        """
        if not self._secret_name or not self._secret_region:
            return "not configured"

        try:
            pg_config = self._pg_config
            logger.debug("PostgreSQL config loaded during health check.")
        except (RuntimeError, BotoCoreError, ClientError, ValueError, KeyError) as err:
            raise HealthCheckError(str(err)) from err

        if not pg_config.get("database"):
            return "not configured"

        missing_fields = [field for field in REQUIRED_CONNECTION_FIELDS if not pg_config.get(field)]
        if missing_fields:
            raise HealthCheckError(f"{missing_fields[0]} not configured")

        return None
