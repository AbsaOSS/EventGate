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
from unittest.mock import MagicMock, patch

import pytest

import src.utils.postgres_base as pb
from src.utils.postgres_base import PostgresBase, _build_postgres_config


class _ConcreteBase(PostgresBase):
    """Minimal concrete subclass for testing PostgresBase directly."""


# _build_postgres_config


def test_build_postgres_config_full():
    raw = {"database": "mydb", "host": "localhost", "user": "admin", "password": "secret", "port": "5432"}
    result = _build_postgres_config(raw)
    assert "mydb" == result["database"]
    assert "localhost" == result["host"]
    assert "admin" == result["user"]
    assert "secret" == result["password"]
    assert 5432 == result["port"]


def test_build_postgres_config_defaults_for_missing_keys():
    result = _build_postgres_config({})
    assert "" == result["database"]
    assert "" == result["host"]
    assert "" == result["user"]
    assert "" == result["password"]
    assert 0 == result["port"]


# PostgresBase.__init__


def test_init_reads_env_vars(monkeypatch):
    monkeypatch.setenv("POSTGRES_SECRET_NAME", "my-secret")
    monkeypatch.setenv("POSTGRES_SECRET_REGION", "eu-west-1")
    instance = _ConcreteBase()
    assert "my-secret" == instance._secret_name
    assert "eu-west-1" == instance._secret_region
    assert instance._connection is None


def test_init_defaults_when_env_vars_absent(monkeypatch):
    monkeypatch.delenv("POSTGRES_SECRET_NAME", raising=False)
    monkeypatch.delenv("POSTGRES_SECRET_REGION", raising=False)
    instance = _ConcreteBase()
    assert "" == instance._secret_name
    assert "" == instance._secret_region


# _pg_config


def test_pg_config_is_cached():
    base = _ConcreteBase()
    secret = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    with patch("src.utils.postgres_base.load_postgres_config", return_value=secret) as mock_load:
        _ = base._pg_config
        _ = base._pg_config
    assert 1 == mock_load.call_count


def test_pg_config_builds_correct_values():
    base = _ConcreteBase()
    secret = {"database": "testdb", "host": "dbhost", "user": "dbuser", "password": "dbpass", "port": 5433}
    with patch("src.utils.postgres_base.load_postgres_config", return_value=secret):
        config = base._pg_config
    assert "testdb" == config["database"]
    assert 5433 == config["port"]


# _connect_options


def test_connect_options_returns_none():
    assert _ConcreteBase()._connect_options() is None


# _get_connection


def test_get_connection_returns_cached_open_connection():
    base = _ConcreteBase()
    mock_conn = MagicMock(closed=0)
    base._connection = mock_conn
    assert mock_conn is base._get_connection()


def test_get_connection_raises_when_psycopg2_not_installed(monkeypatch):
    monkeypatch.setattr(pb, "psycopg2", None)
    with pytest.raises(RuntimeError, match="psycopg2 is not installed"):
        _ConcreteBase()._get_connection()


def test_get_connection_creates_new_when_closed(monkeypatch):
    mock_conn = MagicMock(closed=0)
    mock_psycopg2 = MagicMock()
    mock_psycopg2.connect.return_value = mock_conn
    monkeypatch.setattr(pb, "psycopg2", mock_psycopg2)
    base = _ConcreteBase()
    base._connection = MagicMock(closed=1)
    secret = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    with patch("src.utils.postgres_base.load_postgres_config", return_value=secret):
        result = base._get_connection()
    assert mock_conn is result


def test_get_connection_passes_options_from_subclass(monkeypatch):
    class _BaseWithOptions(PostgresBase):
        def _connect_options(self) -> str | None:
            return "-c statement_timeout=5000"

    captured = {}
    mock_psycopg2 = MagicMock()
    mock_psycopg2.connect.side_effect = lambda **kw: captured.update(kw) or MagicMock(closed=0)
    monkeypatch.setattr(pb, "psycopg2", mock_psycopg2)
    secret = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    with patch("src.utils.postgres_base.load_postgres_config", return_value=secret):
        _BaseWithOptions()._get_connection()
    assert "-c statement_timeout=5000" == captured["options"]


# _close_connection


def test_close_connection_clears_reference_and_calls_close():
    base = _ConcreteBase()
    mock_conn = MagicMock()
    base._connection = mock_conn
    base._close_connection()
    mock_conn.close.assert_called_once()
    assert base._connection is None


def test_close_connection_is_noop_when_no_connection():
    base = _ConcreteBase()
    base._close_connection()
    assert base._connection is None


def test_close_connection_silences_psycopg_error():
    base = _ConcreteBase()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = pb.PsycopgError("gone")
    base._connection = mock_conn
    base._close_connection()
    assert base._connection is None


def test_close_connection_silences_os_error():
    base = _ConcreteBase()
    mock_conn = MagicMock()
    mock_conn.close.side_effect = OSError("socket closed")
    base._connection = mock_conn
    base._close_connection()
    assert base._connection is None


# _execute_with_retry


def test_execute_with_retry_returns_operation_result():
    base = _ConcreteBase()
    base._connection = MagicMock(closed=0)
    assert "ok" == base._execute_with_retry(lambda conn: "ok")


def test_execute_with_retry_reconnects_on_first_failure(monkeypatch):
    calls = []
    mock_conn = MagicMock(closed=0)
    mock_psycopg2 = MagicMock()
    mock_psycopg2.connect.return_value = mock_conn
    monkeypatch.setattr(pb, "psycopg2", mock_psycopg2)
    base = _ConcreteBase()
    base._connection = mock_conn
    secret = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}

    def op(conn):
        calls.append(len(calls))
        if len(calls) == 1:
            raise pb.OperationalError("timeout")
        return "recovered"

    with patch("src.utils.postgres_base.load_postgres_config", return_value=secret):
        result = base._execute_with_retry(op)

    assert "recovered" == result
    assert 2 == len(calls)


def test_execute_with_retry_raises_after_all_attempts_fail(monkeypatch):
    mock_conn = MagicMock(closed=0)
    mock_psycopg2 = MagicMock()
    mock_psycopg2.connect.return_value = mock_conn
    monkeypatch.setattr(pb, "psycopg2", mock_psycopg2)
    base = _ConcreteBase()
    base._connection = mock_conn
    secret = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    with patch("src.utils.postgres_base.load_postgres_config", return_value=secret):
        with pytest.raises(RuntimeError, match="Database connection failed after"):
            base._execute_with_retry(lambda conn: (_ for _ in ()).throw(pb.OperationalError("down")))
