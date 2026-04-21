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

"""Shared base class for PostgreSQL reader and writer."""

import logging
import os
from collections.abc import Callable
from functools import cached_property
from typing import Any, TypedDict

from src.utils.constants import POSTGRES_MAX_RETRIES
from src.utils.utils import load_postgres_config

try:
    import psycopg2
    from psycopg2 import Error as PsycopgError
    from psycopg2 import OperationalError
except ImportError:
    psycopg2 = None  # type: ignore

    class PsycopgError(Exception):  # type: ignore
        """Shim psycopg2 error base when psycopg2 is not installed."""

    class OperationalError(PsycopgError):  # type: ignore
        """Shim psycopg2 OperationalError when psycopg2 is not installed."""


logger = logging.getLogger(__name__)


class PostgresConfig(TypedDict):
    """PostgreSQL connection configuration."""

    database: str
    host: str
    user: str
    password: str
    port: int


def _build_postgres_config(aws_secret: dict[str, Any]) -> PostgresConfig:
    """Validate and build a `PostgresConfig` from an AWS Secrets Manager dict.
    Args:
        aws_secret: Dictionary loaded from AWS Secrets Manager.
    Returns:
        A validated `PostgresConfig`.
    """
    return PostgresConfig(
        database=str(aws_secret.get("database", "")),
        host=str(aws_secret.get("host", "")),
        user=str(aws_secret.get("user", "")),
        password=str(aws_secret.get("password", "")),
        port=int(aws_secret.get("port", 0)),
    )


class PostgresBase:
    """Shared base for PostgreSQL reader and writer."""

    def __init__(self) -> None:
        self._secret_name = os.environ.get("POSTGRES_SECRET_NAME", "")
        self._secret_region = os.environ.get("POSTGRES_SECRET_REGION", "")
        # Any because psycopg2.extensions.connection is unavailable when psycopg2 is not installed.
        self._connection: Any | None = None

    @cached_property
    def _pg_config(self) -> PostgresConfig:
        """Load database config from AWS Secrets Manager on first access."""
        aws_secret = load_postgres_config(self._secret_name, self._secret_region)
        return _build_postgres_config(aws_secret)

    def _connect_options(self) -> str | None:
        """Return psycopg2 connection `options` string.
        Reader overrides this to inject connection-level settings.
        """
        return None

    def _get_connection(self) -> Any:
        """Return a cached database connection, creating one if needed."""
        if self._connection is not None and not self._connection.closed:
            return self._connection
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is not installed.")
        pg_config = self._pg_config
        connect_kwargs: dict[str, str | int] = {
            "database": pg_config["database"],
            "host": pg_config["host"],
            "user": pg_config["user"],
            "password": pg_config["password"],
            "port": pg_config["port"],
        }
        options = self._connect_options()
        if options:
            connect_kwargs["options"] = options
        self._connection = psycopg2.connect(**connect_kwargs)
        logger.debug("New PostgreSQL connection established.")
        return self._connection

    def _close_connection(self) -> None:
        """Close and discard the cached connection."""
        conn_to_close = self._connection
        self._connection = None
        if conn_to_close is not None:
            try:
                conn_to_close.close()
            except (PsycopgError, OSError):
                logger.debug("Failed to close PostgreSQL connection.")

    def _execute_with_retry[T](self, operation: Callable[..., T]) -> T:
        """Run `operation(connection)` with one retry on `OperationalError`.
        Args:
            operation: Callable receiving a psycopg2 connection.
        Returns:
            Whatever `operation` returns on success.
        Raises:
            RuntimeError: If the retry is also exhausted.
        """
        last_exc: OperationalError | None = None
        for attempt in range(POSTGRES_MAX_RETRIES):
            try:
                connection = self._get_connection()
                return operation(connection)
            except OperationalError as exc:
                last_exc = exc
                self._close_connection()
                if attempt < POSTGRES_MAX_RETRIES - 1:
                    logger.warning("PostgreSQL connection lost, reconnecting.")
        raise RuntimeError(f"Database connection failed after {POSTGRES_MAX_RETRIES} attempts: {last_exc}")
