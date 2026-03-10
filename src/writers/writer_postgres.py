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
import os
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from src.utils.constants import TOPIC_TABLE_MAP
from src.utils.trace_logging import log_payload_at_trace
from src.utils.utils import load_postgres_config
from src.writers.writer import Writer

try:
    import psycopg2
    from psycopg2 import Error as PsycopgError
except ImportError:
    psycopg2 = None  # type: ignore

    class PsycopgError(Exception):  # type: ignore
        """Shim psycopg2 error base when psycopg2 is not installed."""


logger = logging.getLogger(__name__)



class WriterPostgres(Writer):
    """Postgres writer for storing events in PostgreSQL database.
    Database credentials are loaded from AWS Secrets Manager at initialization.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._secret_name = os.environ.get("POSTGRES_SECRET_NAME", "")
        self._secret_region = os.environ.get("POSTGRES_SECRET_REGION", "")
        self._db_config: dict[str, Any | None] | None = None
        logger.debug("Initialized PostgreSQL writer.")

    def _load_db_config(self) -> None:
        """Load database config from AWS Secrets Manager."""
        self._db_config = load_postgres_config(self._secret_name, self._secret_region)

    def _ensure_db_config(self) -> dict[str, Any]:
        """Ensure database config is loaded and return it."""
        if self._db_config is None:
            self._load_db_config()
        return self._db_config  # type: ignore[return-value]

    def _postgres_edla_write(self, cursor: Any, table: str, message: dict[str, Any]) -> None:
        """Insert a dlchange style event row.
        Args:
            cursor: Database cursor.
            table: Target table name.
            message: Event payload.
        """
        logger.debug("Sending to Postgres - %s.", table)
        cursor.execute(
            f"""
            INSERT INTO {table}
            (
                event_id,
                tenant_id,
                source_app,
                source_app_version,
                environment,
                timestamp_event,
                country,
                catalog_id,
                operation,
                "location",
                "format",
                format_options,
                additional_info
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )""",
            (
                message["event_id"],
                message["tenant_id"],
                message["source_app"],
                message["source_app_version"],
                message["environment"],
                message["timestamp_event"],
                message.get("country", ""),
                message["catalog_id"],
                message["operation"],
                message.get("location"),
                message["format"],
                (json.dumps(message.get("format_options")) if "format_options" in message else None),
                (json.dumps(message.get("additional_info")) if "additional_info" in message else None),
            ),
        )

    def _postgres_run_write(self, cursor: Any, table_runs: str, table_jobs: str, message: dict[str, Any]) -> None:
        """Insert a run event row plus related job rows.
        Args:
            cursor: Database cursor.
            table_runs: Runs table name.
            table_jobs: Jobs table name.
            message: Event payload (includes jobs array).
        """
        logger.debug("Sending to Postgres - %s and %s.", table_runs, table_jobs)
        cursor.execute(
            f"""
            INSERT INTO {table_runs}
            (
                    event_id,
                    job_ref,
                    tenant_id,
                    source_app,
                    source_app_version,
                    environment,
                    timestamp_start,
                    timestamp_end
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )""",
            (
                message["event_id"],
                message["job_ref"],
                message["tenant_id"],
                message["source_app"],
                message["source_app_version"],
                message["environment"],
                message["timestamp_start"],
                message["timestamp_end"],
            ),
        )

        for job in message["jobs"]:
            cursor.execute(
                f"""
            INSERT INTO {table_jobs}
            (
                    event_id,
                    country,
                    catalog_id,
                    status,
                    timestamp_start,
                    timestamp_end,
                    message,
                    additional_info
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )""",
                (
                    message["event_id"],
                    job.get("country", ""),
                    job["catalog_id"],
                    job["status"],
                    job["timestamp_start"],
                    job["timestamp_end"],
                    job.get("message"),
                    (json.dumps(job.get("additional_info")) if "additional_info" in job else None),
                ),
            )

    def _postgres_test_write(self, cursor: Any, table: str, message: dict[str, Any]) -> None:
        """Insert a test topic row.
        Args:
            cursor: Database cursor.
            table: Target table name.
            message: Event payload.
        """
        logger.debug("Sending to Postgres - %s.", table)
        cursor.execute(
            f"""
            INSERT INTO {table}
            (
                event_id,
                tenant_id,
                source_app,
                environment,
                timestamp_event,
                additional_info
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )""",
            (
                message["event_id"],
                message["tenant_id"],
                message["source_app"],
                message["environment"],
                message["timestamp"],
                (json.dumps(message.get("additional_info")) if "additional_info" in message else None),
            ),
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
            db_config = self._ensure_db_config()

            if not db_config.get("database"):
                logger.debug("No Postgres - skipping Postgres writer.")
                return True, None
            if psycopg2 is None:
                logger.debug("psycopg2 not available - skipping actual Postgres write.")
                return True, None

            log_payload_at_trace(logger, "Postgres", topic_name, message)

            if topic_name not in TOPIC_TABLE_MAP:
                msg = f"Unknown topic for Postgres/{topic_name}"
                logger.error(msg)
                return False, msg

            table_info = TOPIC_TABLE_MAP[topic_name]

            with psycopg2.connect(  # type: ignore[attr-defined]
                database=db_config["database"],
                host=db_config["host"],
                user=db_config["user"],
                password=db_config["password"],
                port=db_config["port"],
            ) as connection:
                with connection.cursor() as cursor:
                    if topic_name == "public.cps.za.dlchange":
                        self._postgres_edla_write(cursor, table_info["main"], message)
                    elif topic_name == "public.cps.za.runs":
                        self._postgres_run_write(cursor, table_info["main"], table_info["jobs"], message)
                    elif topic_name == "public.cps.za.test":
                        self._postgres_test_write(cursor, table_info["main"], message)

                connection.commit()
        except (RuntimeError, PsycopgError, BotoCoreError, ClientError) as e:
            err_msg = f"The Postgres writer failed with unknown error: {str(e)}"
            logger.exception(err_msg)
            return False, err_msg

        return True, None

    def check_health(self) -> tuple[bool, str]:
        """Check PostgreSQL writer health.
        Returns:
            Tuple of (is_healthy: bool, message: str).
        """
        # Checking if Postgres intentionally disabled
        if not self._secret_name or not self._secret_region:
            return True, "not configured"

        try:
            db_config = self._ensure_db_config()
            logger.debug("PostgreSQL config loaded during health check.")
        except (BotoCoreError, ClientError) as err:
            return False, str(err)

        # Validate database configuration fields
        if not db_config.get("database"):
            return True, "database not configured"

        missing_fields = [field for field in ("host", "user", "password", "port") if not db_config.get(field)]
        if missing_fields:
            return False, f"{missing_fields[0]} not configured"

        return True, "ok"
