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

import json
import os
import types

import pytest

from src.writers.writer_postgres import WriterPostgres
import src.utils.postgres_base as pb
import src.utils.utils as secrets_mod


class MockCursor:
    def __init__(self, store=None):
        self.executions = store if store is not None else []

    def execute(self, sql, params):
        self.executions.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class DummyConnection:
    def __init__(self, store):
        self.commit_called = False
        self.store = store
        self.closed = 0

    def cursor(self):
        return MockCursor(self.store)

    def commit(self):
        self.commit_called = True

    def close(self):
        pass


class DummyPsycopg:
    def __init__(self, store):
        self.store = store
        self.connect_count = 0

    def connect(self, **kwargs):
        self.connect_count += 1
        return DummyConnection(self.store)


@pytest.fixture
def reset_env():
    yield
    os.environ.pop("POSTGRES_SECRET_NAME", None)
    os.environ.pop("POSTGRES_SECRET_REGION", None)


def test_insert_dlchange_with_optional_fields():
    writer = WriterPostgres({})
    cur = MockCursor()
    message = {
        "event_id": "e1",
        "tenant_id": "t1",
        "source_app": "app",
        "source_app_version": "1.0.0",
        "environment": "dev",
        "timestamp_event": 111,
        "country": "za",
        "catalog_id": "db.tbl",
        "operation": "append",
        "location": "s3://bucket/path",
        "format": "parquet",
        "format_options": {"compression": "snappy"},
        "additional_info": {"foo": "bar"},
    }
    writer._insert_dlchange(cur, message)
    assert 1 == len(cur.executions)
    _sql, params = cur.executions[0]
    assert "e1" == params["event_id"]
    assert "za" == params["country"]
    assert "s3://bucket/path" == params["location"]
    assert "parquet" == params["format"]
    assert {"compression": "snappy"} == json.loads(params["format_options"])
    assert {"foo": "bar"} == json.loads(params["additional_info"])


def test_insert_dlchange_missing_optional():
    writer = WriterPostgres({})
    cur = MockCursor()
    message = {
        "event_id": "e2",
        "tenant_id": "t2",
        "source_app": "app",
        "source_app_version": "1.0.1",
        "environment": "dev",
        "timestamp_event": 222,
        "catalog_id": "db.tbl2",
        "operation": "overwrite",
        "format": "delta",
    }
    writer._insert_dlchange(cur, message)
    _sql, params = cur.executions[0]
    assert "" == params["country"]
    assert params["location"] is None
    assert "delta" == params["format"]
    assert params["format_options"] is None
    assert params["additional_info"] is None


def test_insert_run():
    writer = WriterPostgres({})
    cur = MockCursor()
    message = {
        "event_id": "r1",
        "job_ref": "job-123",
        "tenant_id": "ten",
        "source_app": "runapp",
        "source_app_version": "2.0.0",
        "environment": "dev",
        "timestamp_start": 1000,
        "timestamp_end": 2000,
        "jobs": [
            {"catalog_id": "c1", "status": "succeeded", "timestamp_start": 1100, "timestamp_end": 1200},
            {
                "country": "bw",
                "catalog_id": "c2",
                "status": "failed",
                "timestamp_start": 1300,
                "timestamp_end": 1400,
                "message": "err",
                "additional_info": {"k": "v"},
            },
        ],
    }
    writer._insert_run(cur, message)
    assert 3 == len(cur.executions)

    _run_sql, run_params = cur.executions[0]
    assert "runapp" == run_params["source_app"]

    _job1_sql, job1_params = cur.executions[1]
    assert "" == job1_params["country"]
    assert "c1" == job1_params["catalog_id"]

    _job2_sql, job2_params = cur.executions[2]
    assert "bw" == job2_params["country"]
    assert "c2" == job2_params["catalog_id"]
    assert "err" == job2_params["message"]
    assert {"k": "v"} == json.loads(job2_params["additional_info"])


def test_insert_test():
    writer = WriterPostgres({})
    cur = MockCursor()
    message = {
        "event_id": "t1",
        "tenant_id": "tenant-x",
        "source_app": "test",
        "environment": "dev",
        "timestamp": 999,
        "additional_info": {"a": 1},
    }
    writer._insert_test(cur, message)
    assert 1 == len(cur.executions)
    _sql, params = cur.executions[0]
    assert "t1" == params["event_id"]
    assert "tenant-x" == params["tenant_id"]
    assert {"a": 1} == json.loads(params["additional_info"])


def test_write_skips_when_no_database(reset_env):
    writer = WriterPostgres({})
    type(writer)._pg_config = property(lambda self: {"database": ""})
    ok, err = writer.write("public.cps.za.test", {})
    del type(writer)._pg_config
    assert ok and err is None


def test_write_fails_when_connection_field_missing(reset_env):
    writer = WriterPostgres({})
    type(writer)._pg_config = property(
        lambda self: {"database": "db", "host": "", "user": "u", "password": "p", "port": 5432}
    )
    ok, err = writer.write("public.cps.za.test", {})
    del type(writer)._pg_config
    assert not ok
    assert "host" in err and "not configured" in err


def test_write_skips_when_psycopg2_missing(reset_env, monkeypatch):
    writer = WriterPostgres({})
    type(writer)._pg_config = property(
        lambda self: {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    )
    monkeypatch.setattr(pb, "psycopg2", None)
    ok, err = writer.write("public.cps.za.test", {})
    del type(writer)._pg_config
    assert ok and err is None


def test_write_unknown_topic_returns_false(reset_env, monkeypatch):
    store = []
    monkeypatch.setattr(pb, "psycopg2", DummyPsycopg(store))
    writer = WriterPostgres({})
    type(writer)._pg_config = property(
        lambda self: {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    )
    ok, err = writer.write("public.cps.za.unknown", {})
    del type(writer)._pg_config
    assert not ok and "Unknown topic" in err


def test_write_success_known_topic(reset_env, monkeypatch):
    store = []
    monkeypatch.setattr(pb, "psycopg2", DummyPsycopg(store))
    writer = WriterPostgres({})
    type(writer)._pg_config = property(
        lambda self: {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    )
    message = {"event_id": "id", "tenant_id": "ten", "source_app": "app", "environment": "dev", "timestamp": 123}
    ok, err = writer.write("public.cps.za.test", message)
    del type(writer)._pg_config
    assert ok and err is None and 1 == len(store)


def test_write_exception_returns_false(reset_env, monkeypatch):
    class FailingPsycopg:
        def connect(self, **kwargs):
            raise RuntimeError("boom")

    monkeypatch.setattr(pb, "psycopg2", FailingPsycopg())
    writer = WriterPostgres({})
    type(writer)._pg_config = property(
        lambda self: {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    )
    ok, err = writer.write("public.cps.za.test", {})
    del type(writer)._pg_config
    assert not ok and "failed with unknown error" in err


def test_init_with_secret(monkeypatch, reset_env):
    secret_dict = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    os.environ["POSTGRES_SECRET_NAME"] = "mysecret"
    os.environ["POSTGRES_SECRET_REGION"] = "eu-west-1"
    mock_client = types.SimpleNamespace(get_secret_value=lambda SecretId: {"SecretString": json.dumps(secret_dict)})

    class MockSession:
        def client(self, service_name, region_name):
            return mock_client

    monkeypatch.setattr(secrets_mod.boto3, "Session", lambda: MockSession())
    writer = WriterPostgres({})
    assert "_pg_config" not in writer.__dict__
    writer.check_health()
    assert "db" == writer._pg_config["database"]


def test_write_dlchange_success(reset_env, monkeypatch):
    store = []
    monkeypatch.setattr(pb, "psycopg2", DummyPsycopg(store))
    writer = WriterPostgres({})
    type(writer)._pg_config = property(
        lambda self: {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    )
    message = {
        "event_id": "e1",
        "tenant_id": "t",
        "source_app": "app",
        "source_app_version": "1",
        "environment": "dev",
        "timestamp_event": 1,
        "catalog_id": "c",
        "operation": "append",
        "format": "parquet",
    }
    ok, err = writer.write("public.cps.za.dlchange", message)
    del type(writer)._pg_config
    assert ok and err is None and 1 == len(store)


def test_write_runs_success(reset_env, monkeypatch):
    store = []
    monkeypatch.setattr(pb, "psycopg2", DummyPsycopg(store))
    writer = WriterPostgres({})
    type(writer)._pg_config = property(
        lambda self: {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    )
    message = {
        "event_id": "r1",
        "job_ref": "job",
        "tenant_id": "t",
        "source_app": "app",
        "source_app_version": "1",
        "environment": "dev",
        "timestamp_start": 1,
        "timestamp_end": 2,
        "jobs": [{"catalog_id": "c", "status": "ok", "timestamp_start": 1, "timestamp_end": 2}],
    }
    ok, err = writer.write("public.cps.za.runs", message)
    del type(writer)._pg_config
    assert ok and err is None and 2 == len(store)  # run + job insert


def test_check_health_not_configured():
    writer = WriterPostgres({})
    healthy, msg = writer.check_health()
    assert healthy and "not configured" == msg


def test_check_health_success(reset_env, monkeypatch):
    secret_dict = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    os.environ["POSTGRES_SECRET_NAME"] = "mysecret"
    os.environ["POSTGRES_SECRET_REGION"] = "eu-west-1"
    mock_client = types.SimpleNamespace(get_secret_value=lambda SecretId: {"SecretString": json.dumps(secret_dict)})

    class MockSession:
        def client(self, service_name, region_name):
            return mock_client

    monkeypatch.setattr(secrets_mod.boto3, "Session", MockSession)
    writer = WriterPostgres({})
    healthy, msg = writer.check_health()
    assert healthy and "ok" == msg


def test_check_health_missing_host(reset_env, monkeypatch):
    secret_dict = {"database": "db", "host": "", "user": "u", "password": "p", "port": 5432}
    os.environ["POSTGRES_SECRET_NAME"] = "mysecret"
    os.environ["POSTGRES_SECRET_REGION"] = "eu-west-1"
    mock_client = types.SimpleNamespace(get_secret_value=lambda SecretId: {"SecretString": json.dumps(secret_dict)})

    class MockSession:
        def client(self, service_name, region_name):
            return mock_client

    monkeypatch.setattr(secrets_mod.boto3, "Session", MockSession)
    writer = WriterPostgres({})
    healthy, msg = writer.check_health()
    assert not healthy and "host not configured" in msg


def test_check_health_database_not_configured():
    """check_health returns (True, 'database not configured') when database field is empty."""
    writer = WriterPostgres({})
    writer._secret_name = "mysecret"
    writer._secret_region = "eu-west-1"
    type(writer)._pg_config = property(lambda self: {"database": ""})
    healthy, msg = writer.check_health()
    del type(writer)._pg_config
    assert healthy
    assert "database not configured" == msg


def test_check_health_load_config_exception(mocker):
    """check_health returns (False, error) when _pg_config raises."""
    writer = WriterPostgres({})
    writer._secret_name = "mysecret"
    writer._secret_region = "eu-west-1"

    mocker.patch.object(
        type(writer),
        "_pg_config",
        new_callable=lambda: property(lambda self: (_ for _ in ()).throw(ValueError("secret fetch failed"))),
    )
    healthy, msg = writer.check_health()
    assert not healthy
    assert "secret fetch failed" == msg


def test_write_reconnects_on_closed_connection(reset_env, monkeypatch):
    store = []
    psycopg = DummyPsycopg(store)
    monkeypatch.setattr(pb, "psycopg2", psycopg)
    writer = WriterPostgres({})
    type(writer)._pg_config = property(
        lambda self: {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    )
    message = {"event_id": "id", "tenant_id": "ten", "source_app": "app", "environment": "dev", "timestamp": 123}

    writer.write("public.cps.za.test", message)
    assert 1 == psycopg.connect_count

    writer._connection.closed = 2

    writer.write("public.cps.za.test", message)
    del type(writer)._pg_config
    assert 2 == psycopg.connect_count


def test_write_retries_on_operational_error(reset_env, monkeypatch):
    store = []
    fail_flag = [True]

    class RetryCursor:
        def execute(self, sql, params):
            if fail_flag[0]:
                fail_flag[0] = False
                raise pb.OperationalError("connection reset")
            store.append((sql, params))

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class RetryConnection:
        def __init__(self):
            self.closed = 0

        def cursor(self):
            return RetryCursor()

        def commit(self):
            pass

        def close(self):
            pass

    class RetryPsycopg:
        def __init__(self):
            self.connect_count = 0

        def connect(self, **kwargs):
            self.connect_count += 1
            return RetryConnection()

    psycopg = RetryPsycopg()
    monkeypatch.setattr(pb, "psycopg2", psycopg)
    writer = WriterPostgres({})
    type(writer)._pg_config = property(
        lambda self: {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    )
    message = {"event_id": "id", "tenant_id": "ten", "source_app": "app", "environment": "dev", "timestamp": 123}

    ok, err = writer.write("public.cps.za.test", message)
    del type(writer)._pg_config

    assert ok and err is None
    assert 2 == psycopg.connect_count
    assert 1 == len(store)


def test_write_fails_after_retry_exhausted(reset_env, monkeypatch):
    class AlwaysFailCursor:
        def execute(self, sql, params):
            raise pb.OperationalError("connection reset")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class AlwaysFailConnection:
        def __init__(self):
            self.closed = 0

        def cursor(self):
            return AlwaysFailCursor()

        def commit(self):
            pass

        def close(self):
            pass

    class AlwaysFailPsycopg:
        def connect(self, **kwargs):
            return AlwaysFailConnection()

    monkeypatch.setattr(pb, "psycopg2", AlwaysFailPsycopg())
    writer = WriterPostgres({})
    type(writer)._pg_config = property(
        lambda self: {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    )
    message = {"event_id": "id", "tenant_id": "ten", "source_app": "app", "environment": "dev", "timestamp": 123}

    ok, err = writer.write("public.cps.za.test", message)
    del type(writer)._pg_config

    assert not ok
    assert "failed with unknown error" in err


def test_write_discards_connection_on_non_operational_error(reset_env, monkeypatch):
    class FailCursor:
        def execute(self, sql, params):
            raise pb.PsycopgError("integrity error")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class FailConnection:
        def __init__(self):
            self.closed = 0

        def cursor(self):
            return FailCursor()

        def commit(self):
            pass

        def close(self):
            pass

    class FailPsycopg:
        def connect(self, **kwargs):
            return FailConnection()

    monkeypatch.setattr(pb, "psycopg2", FailPsycopg())
    writer = WriterPostgres({})
    type(writer)._pg_config = property(
        lambda self: {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    )
    message = {"event_id": "id", "tenant_id": "ten", "source_app": "app", "environment": "dev", "timestamp": 123}

    ok, _ = writer.write("public.cps.za.test", message)
    del type(writer)._pg_config

    assert not ok
    assert writer._connection is None
