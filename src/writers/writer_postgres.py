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
from functools import cached_property
from pathlib import Path
from typing import Any

import aiosql
from botocore.exceptions import BotoCoreError, ClientError

from src.utils.constants import REQUIRED_CONNECTION_FIELDS, TOPIC_DLCHANGE, TOPIC_RUNS, TOPIC_TABLE_MAP, TOPIC_TEST
from src.utils.postgres_base import PsycopgError, PostgresBase
import src.utils.postgres_base as _pb
from src.utils.trace_logging import log_payload_at_trace
from src.writers.writer import Writer

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).parent / "sql"


@dataclass(frozen=True)
class WriterQueries:
    """Typed holder for writer SQL query strings loaded via aiosql."""

    insert_dlchange: str
    insert_run: str
    insert_run_job: str
    insert_test: str


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
        return WriterQueries(
            insert_dlchange=queries.insert_dlchange.sql,
            insert_run=queries.insert_run.sql,
            insert_run_job=queries.insert_run_job.sql,
            insert_test=queries.insert_test.sql,
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

    def write(self, topic_name: str, message: dict[str, Any]) -> tuple[bool, str | None]:
        """Dispatch insertion for a topic into the correct Postgres table(s).
        Args:
            topic_name: Incoming topic identifier.
            message: JSON-serializable payload.
        Returns:
            Tuple of (success: bool, error_message: str | None).
        """
        try:
            pg_config = self._pg_config

            if not pg_config.get("database"):
                logger.debug("No Postgres - skipping Postgres writer.")
                return True, None

            missing = [field for field in REQUIRED_CONNECTION_FIELDS if not pg_config.get(field)]
            if missing:
                msg = f"PostgreSQL connection field '{missing[0]}' not configured."
                logger.error(msg)
                return False, msg

            if not self._is_psycopg2_available():
                logger.debug("psycopg2 not available - skipping actual Postgres write.")
                return True, None

            log_payload_at_trace(logger, "Postgres", topic_name, message)

            if topic_name not in TOPIC_TABLE_MAP:
                msg = f"Unknown topic for Postgres/{topic_name}"
                logger.error(msg)
                return False, msg

            self._execute_with_retry(lambda conn: self._write_topic(conn, topic_name, message), retry=False)
        except (RuntimeError, PsycopgError, BotoCoreError, ClientError, ValueError, KeyError) as e:
            self._close_connection()
            err_msg = f"The Postgres writer failed with unknown error: {e!s}"
            logger.exception("The Postgres writer failed with unknown error: %s.", e)
            return False, err_msg

        return True, None

    def _write_topic(self, connection: Any, topic_name: str, message: dict[str, Any]) -> None:
        """Execute the insert for the given topic inside a transaction."""
        with connection.cursor() as cursor:
            if topic_name == TOPIC_DLCHANGE:
                self._insert_dlchange(cursor, message)
            elif topic_name == TOPIC_RUNS:
                self._insert_run(cursor, message)
            elif topic_name == TOPIC_TEST:
                self._insert_test(cursor, message)
        connection.commit()

    @staticmethod
    def _is_psycopg2_available() -> bool:
        """Check whether psycopg2 is importable."""
        return _pb.psycopg2 is not None

    def check_health(self) -> tuple[bool, str]:
        """Check PostgreSQL writer health.
        Returns:
            Tuple of (is_healthy: bool, message: str).
        """
        if not self._secret_name or not self._secret_region:
            return True, "not configured"

        try:
            pg_config = self._pg_config
            logger.debug("PostgreSQL config loaded during health check.")
        except (BotoCoreError, ClientError, ValueError, KeyError) as err:
            return False, str(err)

        if not pg_config.get("database"):
            return True, "database not configured"

        missing_fields = [field for field in REQUIRED_CONNECTION_FIELDS if not pg_config.get(field)]
        if missing_fields:
            return False, f"{missing_fields[0]} not configured"

        return True, "ok"
