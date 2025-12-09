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

"""Postgres writer module.

Handles optional initialization via AWS Secrets Manager and topic-based inserts into Postgres.
"""

import json
import os
import logging
from typing import Any, Dict, Tuple, Optional

import boto3

try:
    import psycopg2  # noqa: F401
except ImportError:  # pragma: no cover - environment without psycopg2
    psycopg2 = None  # type: ignore

from src.utils.trace_logging import log_payload_at_trace

# Define a unified psycopg2 error base for safe exception handling even if psycopg2 missing
if psycopg2 is not None:  # type: ignore
    try:  # pragma: no cover - attribute presence depends on installed psycopg2 variant
        PsycopgError = psycopg2.Error  # type: ignore[attr-defined]
    except AttributeError:  # pragma: no cover

        class PsycopgError(Exception):  # type: ignore
            """Shim psycopg2 error base when psycopg2 provides no Error attribute."""

else:  # fallback shim when psycopg2 absent

    class PsycopgError(Exception):  # type: ignore
        """Shim psycopg2 error base when psycopg2 is not installed."""


# Module level globals for typing
logger: logging.Logger = logging.getLogger(__name__)
POSTGRES: Dict[str, Any] = {"database": ""}


def init(logger_instance: logging.Logger) -> None:
    """Initialize Postgres credentials either from AWS Secrets Manager or fallback empty config.

    Args:
        logger_instance: Shared application logger.
    """
    global logger  # pylint: disable=global-statement
    global POSTGRES  # pylint: disable=global-statement

    logger = logger_instance

    secret_name = os.environ.get("POSTGRES_SECRET_NAME", "")
    secret_region = os.environ.get("POSTGRES_SECRET_REGION", "")

    if secret_name and secret_region:
        aws_secrets = boto3.Session().client(service_name="secretsmanager", region_name=secret_region)
        postgres_secret = aws_secrets.get_secret_value(SecretId=secret_name)["SecretString"]
        POSTGRES = json.loads(postgres_secret)
    else:
        POSTGRES = {"database": ""}

    logger.debug("Initialized POSTGRES writer")


def postgres_edla_write(cursor, table: str, message: Dict[str, Any]) -> None:
    """Insert a dlchange style event row.

    Args:
        cursor: Database cursor.
        table: Target table name.
        message: Event payload.
    """
    logger.debug("Sending to Postgres - %s", table)
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


def postgres_run_write(cursor, table_runs: str, table_jobs: str, message: Dict[str, Any]) -> None:
    """Insert a run event row plus related job rows.

    Args:
        cursor: Database cursor.
        table_runs: Runs table name.
        table_jobs: Jobs table name.
        message: Event payload (includes jobs array).
    """
    logger.debug("Sending to Postgres - %s and %s", table_runs, table_jobs)
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
                catalog_id,
                country,
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
                job["catalog_id"],
                job.get("country", ""),
                job["status"],
                job["timestamp_start"],
                job["timestamp_end"],
                job.get("message"),
                (json.dumps(job.get("additional_info")) if "additional_info" in job else None),
            ),
        )


def postgres_test_write(cursor, table: str, message: Dict[str, Any]) -> None:
    """Insert a test topic row.

    Args:
        cursor: Database cursor.
        table: Target table name.
        message: Event payload.
    """
    logger.debug("Sending to Postgres - %s", table)
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


def write(topic_name: str, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Dispatch insertion for a topic into the correct Postgres table(s).

    Skips if Postgres not configured or psycopg2 unavailable. Returns success flag and optional error.

    Args:
        topic_name: Incoming topic identifier.
        message: Event payload.
    """
    try:
        if not POSTGRES.get("database"):
            logger.debug("No Postgres - skipping")
            return True, None
        if psycopg2 is None:  # type: ignore
            logger.debug("psycopg2 not available - skipping actual Postgres write")
            return True, None

        log_payload_at_trace(logger, "Postgres", topic_name, message)

        with psycopg2.connect(  # type: ignore[attr-defined]
            database=POSTGRES["database"],
            host=POSTGRES["host"],
            user=POSTGRES["user"],
            password=POSTGRES["password"],
            port=POSTGRES["port"],
        ) as connection:  # type: ignore[call-arg]
            with connection.cursor() as cursor:  # type: ignore
                if topic_name == "public.cps.za.dlchange":
                    postgres_edla_write(cursor, "public_cps_za_dlchange", message)
                elif topic_name == "public.cps.za.runs":
                    postgres_run_write(cursor, "public_cps_za_runs", "public_cps_za_runs_jobs", message)
                elif topic_name == "public.cps.za.test":
                    postgres_test_write(cursor, "public_cps_za_test", message)
                else:
                    msg = f"unknown topic for postgres {topic_name}"
                    logger.error(msg)
                    return False, msg

            connection.commit()  # type: ignore
    except (RuntimeError, PsycopgError) as e:  # narrowed exception set
        err_msg = f"The Postgres writer with failed unknown error: {str(e)}"
        logger.exception(err_msg)
        return False, err_msg

    return True, None
