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
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.readers.reader_postgres import ReaderPostgres
import src.utils.postgres_base as pb

_STATS_DESCRIPTION = [
    ("event_id",),
    ("job_ref",),
    ("tenant_id",),
    ("source_app",),
    ("source_app_version",),
    ("environment",),
    ("run_timestamp_start",),
    ("run_timestamp_end",),
    ("internal_id",),
    ("country",),
    ("catalog_id",),
    ("status",),
    ("timestamp_start",),
    ("timestamp_end",),
    ("message",),
    ("additional_info",),
]


@pytest.fixture
def mock_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set required environment variables for ReaderPostgres."""
    monkeypatch.setenv("POSTGRES_SECRET_NAME", "eventgate/postgres")
    monkeypatch.setenv("POSTGRES_SECRET_REGION", "us-east-1")


@pytest.fixture
def pg_secret() -> dict[str, Any]:
    """Sample Postgres secret payload."""
    return {
        "database": "eventgate",
        "host": "localhost",
        "port": 5432,
        "user": "reader",
        "password": "secret",
    }


@pytest.fixture
def reader(mock_env: None) -> ReaderPostgres:
    """Create a ReaderPostgres instance with env vars set."""
    return ReaderPostgres()


class TestDbConfig:
    """Tests for lazy database configuration loading via cached_property."""

    def test_loads_config_from_secrets_manager(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that _pg_config loads from Secrets Manager."""
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}

        with patch("boto3.Session") as mock_session:
            mock_session.return_value.client.return_value = mock_client
            result = reader._pg_config

        assert "eventgate" == result["database"]
        assert "localhost" == result["host"]

    def test_caches_config_after_first_load(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that config is only loaded once (cached)."""
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}

        with patch("boto3.Session") as mock_session:
            mock_session.return_value.client.return_value = mock_client
            _ = reader._pg_config
            _ = reader._pg_config

        assert 1 == mock_client.get_secret_value.call_count

    def test_returns_empty_db_when_no_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that missing env vars produce empty database config."""
        monkeypatch.setenv("POSTGRES_SECRET_NAME", "")
        monkeypatch.setenv("POSTGRES_SECRET_REGION", "")
        reader = ReaderPostgres()

        result = reader._pg_config

        assert "" == result["database"]


def _make_mock_connection(description: list[tuple[str, ...]], rows: list[tuple[Any, ...]]) -> MagicMock:
    """Build a mock psycopg2 connection with cursor returning given rows."""
    mock_cursor = MagicMock()
    mock_cursor.description = description
    mock_cursor.fetchall.return_value = rows

    mock_conn = MagicMock()
    mock_conn.closed = 0
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn


class TestReadStats:
    """Tests for read_stats query execution."""

    def test_returns_rows_and_pagination(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that read_stats returns rows and pagination info."""
        rows = [
            (
                "ev1",
                "ref1",
                "T1",
                "app",
                "1.0",
                "test",
                1704067200000,
                1704070800000,
                1,
                "ZA",
                "db.t1",
                "succeeded",
                1704067200000,
                1704070800000,
                None,
                None,
            ),
            (
                "ev2",
                "ref2",
                "T2",
                "app",
                "1.0",
                "test",
                1704067200000,
                1704070800000,
                2,
                "ZA",
                "db.t2",
                "failed",
                1704067200000,
                1704070800000,
                "no data",
                None,
            ),
        ]
        mock_conn = _make_mock_connection(_STATS_DESCRIPTION, rows)

        with (
            patch("boto3.Session") as mock_session,
            patch.object(pb, "psycopg2") as mock_pg,
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            mock_pg.connect.return_value = mock_conn

            result_rows, pagination = reader.read_stats(limit=50)

        assert 2 == len(result_rows)
        assert "ev1" == result_rows[0]["event_id"]
        assert "run_date" in result_rows[0]
        assert "run_status" in result_rows[0]
        assert False is pagination["has_more"]
        assert 50 == pagination["limit"]

    def test_has_more_when_extra_row_returned(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that has_more is True when more rows than limit exist."""
        rows = [
            ("ev1", "r", "T", "a", "1", "t", 0, 0, 3, "ZA", "c", "s", 0, 0, None, None),
            ("ev2", "r", "T", "a", "1", "t", 0, 0, 2, "ZA", "c", "s", 0, 0, None, None),
            ("ev3", "r", "T", "a", "1", "t", 0, 0, 1, "ZA", "c", "s", 0, 0, None, None),
        ]
        mock_conn = _make_mock_connection(_STATS_DESCRIPTION, rows)

        with (
            patch("boto3.Session") as mock_session,
            patch.object(pb, "psycopg2") as mock_pg,
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            mock_pg.connect.return_value = mock_conn

            result_rows, pagination = reader.read_stats(limit=2)

        assert 2 == len(result_rows)
        assert True is pagination["has_more"]
        assert 2 == pagination["cursor"]

    def test_no_database_raises_runtime_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that empty database config raises RuntimeError."""
        monkeypatch.setenv("POSTGRES_SECRET_NAME", "")
        monkeypatch.setenv("POSTGRES_SECRET_REGION", "")
        reader = ReaderPostgres()

        with pytest.raises(RuntimeError, match="config missing"):
            reader.read_stats()

    def test_missing_connection_field_raises_runtime_error(
        self, reader: ReaderPostgres, pg_secret: dict[str, Any]
    ) -> None:
        """Test that a secret missing host raises RuntimeError."""
        incomplete_secret = {"database": "db", "port": 5432}
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {"SecretString": json.dumps(incomplete_secret)}

        with (
            patch("boto3.Session") as mock_session,
            pytest.raises(ValueError, match="Missing PostgreSQL secret fields"),
        ):
            mock_session.return_value.client.return_value = mock_client
            reader.read_stats()

    def test_cursor_filters_by_internal_id(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that passing cursor uses the cursor query variant."""
        mock_conn = _make_mock_connection(_STATS_DESCRIPTION, [])

        with (
            patch("boto3.Session") as mock_session,
            patch.object(pb, "psycopg2") as mock_pg,
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            mock_pg.connect.return_value = mock_conn

            reader.read_stats(cursor=100, limit=10)

            executed_sql = mock_conn.cursor.return_value.__enter__.return_value.execute.call_args[0][0]
            executed_params = mock_conn.cursor.return_value.__enter__.return_value.execute.call_args[0][1]

        assert "j.internal_id <" in executed_sql
        assert 100 == executed_params["cursor_id"]


class TestFormatRow:
    """Tests for computed column formatting."""

    def test_succeeded_status(self) -> None:
        """Test that non-failed status maps to succeeded."""
        row: dict[str, Any] = {
            "status": "succeeded",
            "message": None,
            "tenant_id": "ABC",
            "run_timestamp_start": 1704067200000,
            "timestamp_start": 1704067200000,
            "timestamp_end": 1704153600000,
        }
        result = ReaderPostgres._format_row(row)

        assert "succeeded" == result["run_status"]
        assert "abc" == result["formatted_tenant"]
        assert "01-01-2024" == result["run_date"]
        assert 86_400_000 == result["elapsed_time"]
        assert "2024-01-01 00:00:00" == result["start_time"]
        assert "2024-01-02 00:00:00" == result["end_time"]

    def test_failed_no_data(self) -> None:
        """Test that failed + 'no data' message maps to no data received."""
        row: dict[str, Any] = {
            "status": "failed",
            "message": "Error: No Data found",
            "tenant_id": "XYZ",
            "run_timestamp_start": None,
            "timestamp_start": None,
            "timestamp_end": None,
        }
        result = ReaderPostgres._format_row(row)

        assert "no data received" == result["run_status"]
        assert result["run_date"] is None
        assert result["elapsed_time"] is None

    def test_failed_no_records(self) -> None:
        """Test that failed + 'no records to send' message maps to no data produced."""
        row: dict[str, Any] = {
            "status": "failed",
            "message": "no records to send",
            "tenant_id": "T",
            "run_timestamp_start": None,
            "timestamp_start": None,
            "timestamp_end": None,
        }
        result = ReaderPostgres._format_row(row)

        assert "no data produced" == result["run_status"]

    def test_failed_generic(self) -> None:
        """Test that failed without special message maps to failed."""
        row: dict[str, Any] = {
            "status": "failed",
            "message": "timeout",
            "tenant_id": "T",
            "run_timestamp_start": None,
            "timestamp_start": None,
            "timestamp_end": None,
        }
        result = ReaderPostgres._format_row(row)

        assert "failed" == result["run_status"]


class TestCheckHealth:
    """Tests for reader health check."""

    def test_unhealthy_when_not_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test returns unhealthy when no secret env vars set."""
        monkeypatch.setenv("POSTGRES_SECRET_NAME", "")
        monkeypatch.setenv("POSTGRES_SECRET_REGION", "")
        reader = ReaderPostgres()

        healthy, message = reader.check_health()

        assert False is healthy
        assert "postgres secret not configured" == message

    def test_healthy_when_config_valid(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test returns healthy when config is valid."""
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}

        with patch("boto3.Session") as mock_session:
            mock_session.return_value.client.return_value = mock_client
            healthy, message = reader.check_health()

        assert True is healthy
        assert "ok" == message

    def test_unhealthy_when_load_raises_runtime_error(self, reader: ReaderPostgres) -> None:
        """Test returns unhealthy when _pg_config raises RuntimeError."""
        with patch.object(
            type(reader),
            "_pg_config",
            new_callable=lambda: property(lambda self: (_ for _ in ()).throw(RuntimeError("Failed to load."))),
        ):
            healthy, message = reader.check_health()

        assert False is healthy
        assert "Failed to load." == message


class TestConnectionReuse:
    """Tests for PostgreSQL connection reuse error handling."""

    def test_reconnects_on_closed_connection(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that a closed connection triggers reconnection."""
        mock_conn = _make_mock_connection(_STATS_DESCRIPTION, [])

        with (
            patch("boto3.Session") as mock_session,
            patch.object(pb, "psycopg2") as mock_pg,
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            mock_pg.connect.return_value = mock_conn

            reader.read_stats(limit=10)
            assert 1 == mock_pg.connect.call_count

            mock_conn.closed = 2

            reader.read_stats(limit=10)
            assert 2 == mock_pg.connect.call_count

    def test_retries_on_operational_error(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that OperationalError triggers retry with fresh connection."""
        fail_conn = _make_mock_connection(_STATS_DESCRIPTION, [])
        fail_cursor = fail_conn.cursor.return_value.__enter__.return_value
        fail_cursor.execute.side_effect = pb.OperationalError("connection reset")

        ok_conn = _make_mock_connection(_STATS_DESCRIPTION, [])

        with (
            patch("boto3.Session") as mock_session,
            patch.object(pb, "psycopg2") as mock_pg,
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            mock_pg.connect.side_effect = [fail_conn, ok_conn]

            rows, _pagination = reader.read_stats(limit=10)

        assert 2 == mock_pg.connect.call_count
        assert [] == rows

    def test_raises_after_retry_exhausted(self, reader: ReaderPostgres, pg_secret: dict[str, Any]) -> None:
        """Test that OperationalError on both attempts raises RuntimeError."""
        fail_conn = MagicMock()
        fail_conn.closed = 0
        fail_cursor = MagicMock()
        fail_cursor.execute.side_effect = pb.OperationalError("connection reset")
        fail_conn.cursor.return_value.__enter__ = MagicMock(return_value=fail_cursor)
        fail_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch("boto3.Session") as mock_session,
            patch.object(pb, "psycopg2") as mock_pg,
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            mock_pg.connect.return_value = fail_conn

            with pytest.raises(RuntimeError, match="Database connection failed after"):
                reader.read_stats(limit=10)

    def test_discards_connection_on_non_operational_error(
        self, reader: ReaderPostgres, pg_secret: dict[str, Any]
    ) -> None:
        """Test that a non-OperationalError PsycopgError discards the connection."""
        fail_conn = MagicMock()
        fail_conn.closed = 0
        fail_cursor = MagicMock()
        fail_cursor.execute.side_effect = pb.PsycopgError("integrity error")
        fail_conn.cursor.return_value.__enter__ = MagicMock(return_value=fail_cursor)
        fail_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch("boto3.Session") as mock_session,
            patch.object(pb, "psycopg2") as mock_pg,
        ):
            mock_client = MagicMock()
            mock_client.get_secret_value.return_value = {"SecretString": json.dumps(pg_secret)}
            mock_session.return_value.client.return_value = mock_client
            mock_pg.connect.return_value = fail_conn

            with pytest.raises(RuntimeError, match="Database query error"):
                reader.read_stats(limit=10)

        assert reader._connection is None
