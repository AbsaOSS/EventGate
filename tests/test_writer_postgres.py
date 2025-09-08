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
import logging
import os
import types
import pytest
from unittest.mock import patch

from src import writer_postgres

@pytest.fixture(scope="module", autouse=True)
def init_module_logger():
    writer_postgres.init(logging.getLogger("test"))

class MockCursor:
    def __init__(self):
        self.executions = []
    def execute(self, sql, params):
        self.executions.append((sql.strip(), params))

# --- Insert helpers ---

def test_postgres_edla_write_with_optional_fields():
    cur = MockCursor()
    message = {
        "event_id": "e1",
        "tenant_id": "t1",
        "source_app": "app",
        "source_app_version": "1.0.0",
        "environment": "dev",
        "timestamp_event": 111,
        "catalog_id": "db.tbl",
        "operation": "append",
        "location": "s3://bucket/path",
        "format": "parquet",
        "format_options": {"compression": "snappy"},
        "additional_info": {"foo": "bar"}
    }
    writer_postgres.postgres_edla_write(cur, "table_a", message)
    assert len(cur.executions) == 1
    _sql, params = cur.executions[0]
    assert len(params) == 12
    assert params[0] == "e1"
    assert params[8] == "s3://bucket/path"
    assert params[9] == "parquet"
    assert json.loads(params[10]) == {"compression": "snappy"}
    assert json.loads(params[11]) == {"foo": "bar"}

def test_postgres_edla_write_missing_optional():
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
        "format": "delta"
    }
    writer_postgres.postgres_edla_write(cur, "table_a", message)
    _sql, params = cur.executions[0]
    assert params[8] is None
    assert params[9] == "delta"
    assert params[10] is None
    assert params[11] is None

def test_postgres_run_write():
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
            {"catalog_id": "c2", "status": "failed", "timestamp_start": 1300, "timestamp_end": 1400, "message": "err", "additional_info": {"k": "v"}}
        ]
    }
    writer_postgres.postgres_run_write(cur, "runs_table", "jobs_table", message)
    assert len(cur.executions) == 3
    run_sql, run_params = cur.executions[0]
    assert "source_app_version" in run_sql
    assert run_params[3] == "runapp"
    _job2_sql, job2_params = cur.executions[2]
    assert job2_params[5] == "err"
    assert json.loads(job2_params[6]) == {"k": "v"}

def test_postgres_test_write():
    cur = MockCursor()
    message = {
        "event_id": "t1",
        "tenant_id": "tenant-x",
        "source_app": "test",
        "environment": "dev",
        "timestamp": 999,
        "additional_info": {"a": 1}
    }
    writer_postgres.postgres_test_write(cur, "table_test", message)
    assert len(cur.executions) == 1
    _sql, params = cur.executions[0]
    assert params[0] == "t1"
    assert params[1] == "tenant-x"
    assert json.loads(params[5]) == {"a": 1}

# --- write() behavioral paths ---

@pytest.fixture
def reset_state(monkeypatch):
    # Preserve psycopg2 ref
    original_psycopg2 = writer_postgres.psycopg2
    writer_postgres.POSTGRES = {"database": ""}
    yield
    writer_postgres.psycopg2 = original_psycopg2
    writer_postgres.POSTGRES = {"database": ""}
    os.environ.pop("POSTGRES_SECRET_NAME", None)
    os.environ.pop("POSTGRES_SECRET_REGION", None)

class DummyCursor:
    def __init__(self, store):
        self.store = store
    def execute(self, sql, params):
        self.store.append((sql, params))
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False

class DummyConnection:
    def __init__(self, store):
        self.commit_called = False
        self.store = store
    def cursor(self):
        return DummyCursor(self.store)
    def commit(self):
        self.commit_called = True
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False

class DummyPsycopg:
    def __init__(self, store):
        self.store = store
    def connect(self, **kwargs):
        return DummyConnection(self.store)

def test_write_skips_when_no_database(reset_state):
    writer_postgres.POSTGRES = {"database": ""}
    ok, err = writer_postgres.write("public.cps.za.test", {})
    assert ok and err is None

def test_write_skips_when_psycopg2_missing(reset_state, monkeypatch):
    writer_postgres.POSTGRES = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    monkeypatch.setattr(writer_postgres, "psycopg2", None)
    ok, err = writer_postgres.write("public.cps.za.test", {})
    assert ok and err is None

def test_write_unknown_topic_returns_false(reset_state, monkeypatch):
    store = []
    monkeypatch.setattr(writer_postgres, "psycopg2", DummyPsycopg(store))
    writer_postgres.POSTGRES = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    ok, err = writer_postgres.write("public.cps.za.unknown", {})
    assert not ok and "unknown topic" in err

def test_write_success_known_topic(reset_state, monkeypatch):
    store = []
    monkeypatch.setattr(writer_postgres, "psycopg2", DummyPsycopg(store))
    writer_postgres.POSTGRES = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    message = {"event_id": "id", "tenant_id": "ten", "source_app": "app", "environment": "dev", "timestamp": 123}
    ok, err = writer_postgres.write("public.cps.za.test", message)
    assert ok and err is None and len(store) == 1

def test_write_exception_returns_false(reset_state, monkeypatch):
    class FailingPsycopg:
        def connect(self, **kwargs):
            raise RuntimeError("boom")
    monkeypatch.setattr(writer_postgres, "psycopg2", FailingPsycopg())
    writer_postgres.POSTGRES = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    ok, err = writer_postgres.write("public.cps.za.test", {})
    assert not ok and "failed unknown error" in err

def test_init_with_secret(monkeypatch, reset_state):
    secret_dict = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    os.environ["POSTGRES_SECRET_NAME"] = "mysecret"
    os.environ["POSTGRES_SECRET_REGION"] = "eu-west-1"
    mock_client = types.SimpleNamespace(get_secret_value=lambda SecretId: {"SecretString": json.dumps(secret_dict)})
    class MockSession:
        def client(self, service_name, region_name):
            return mock_client
    monkeypatch.setattr(writer_postgres.boto3, "Session", lambda: MockSession())
    writer_postgres.init(logging.getLogger("test"))
    assert writer_postgres.POSTGRES == secret_dict

def test_write_dlchange_success(reset_state, monkeypatch):
    store = []
    monkeypatch.setattr(writer_postgres, "psycopg2", DummyPsycopg(store))
    writer_postgres.POSTGRES = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    message = {
        "event_id":"e1","tenant_id":"t","source_app":"app","source_app_version":"1","environment":"dev",
        "timestamp_event":1,"catalog_id":"c","operation":"append","format":"parquet"
    }
    ok, err = writer_postgres.write("public.cps.za.dlchange", message)
    assert ok and err is None and len(store) == 1

def test_write_runs_success(reset_state, monkeypatch):
    store = []
    monkeypatch.setattr(writer_postgres, "psycopg2", DummyPsycopg(store))
    writer_postgres.POSTGRES = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}
    message = {
        "event_id":"r1","job_ref":"job","tenant_id":"t","source_app":"app","source_app_version":"1","environment":"dev",
        "timestamp_start":1,"timestamp_end":2,"jobs":[{"catalog_id":"c","status":"ok","timestamp_start":1,"timestamp_end":2}]
    }
    ok, err = writer_postgres.write("public.cps.za.runs", message)
    assert ok and err is None and len(store) == 2  # run + job insert
