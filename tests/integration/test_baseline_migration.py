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

import time
from typing import Generator

import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

from tests.integration.conftest import _convert_dsn, _run_flyway_migrate

# Mimics the complete hand-created production schema before Flyway is introduced.
LEGACY_SCHEMA_SQL = """
CREATE TABLE public_cps_za_runs (
    event_id VARCHAR(255) NOT NULL,
    job_ref VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    source_app VARCHAR(255) NOT NULL,
    source_app_version VARCHAR(255) NOT NULL,
    environment VARCHAR(255) NOT NULL,
    timestamp_start BIGINT,
    timestamp_end BIGINT
);

CREATE TABLE public_cps_za_runs_jobs (
    internal_id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    country VARCHAR(255),
    catalog_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    timestamp_start BIGINT,
    timestamp_end BIGINT,
    message TEXT,
    additional_info JSONB
);

CREATE TABLE public_cps_za_dlchange (
    event_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    source_app VARCHAR(255) NOT NULL,
    source_app_version VARCHAR(255) NOT NULL,
    environment VARCHAR(255) NOT NULL,
    timestamp_event BIGINT,
    country VARCHAR(255),
    catalog_id VARCHAR(255) NOT NULL,
    operation VARCHAR(255),
    "location" TEXT,
    "format" VARCHAR(255),
    format_options JSONB,
    additional_info JSONB
);

CREATE TABLE public_cps_za_test (
    event_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    source_app VARCHAR(255) NOT NULL,
    environment VARCHAR(255) NOT NULL,
    timestamp_event BIGINT,
    additional_info JSONB
);

CREATE TABLE public_cps_za_status_change_aggregated_job (
    job_id UUID PRIMARY KEY,
    job_group_id UUID,
    parent_job_id UUID,
    initial_job_id UUID,
    job_ref TEXT,
    job_name TEXT,
    definition_id TEXT,
    definition_version TEXT,
    tenant_id TEXT,
    country TEXT,
    source_app TEXT,
    source_app_version TEXT,
    environment TEXT,
    platform TEXT,
    platform_metadata JSONB,
    input_arguments JSONB,
    additional_context JSONB,
    attempt_number INTEGER NOT NULL DEFAULT 1 CHECK (attempt_number > 0),
    status_type TEXT CHECK (status_type IN ('WAITING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'KILLED')),
    status_subtype TEXT,
    status_detail TEXT,
    created_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    last_updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO public_cps_za_test
    (event_id, tenant_id, source_app, environment, timestamp_event)
VALUES ('preexisting-row', 'tenant', 'app', 'env', 42);
"""


@pytest.fixture(scope="module")
def preseeded_dsn() -> Generator[str, None, None]:
    container = PostgresContainer("postgres:16", dbname="eventgate")
    container.start()
    dsn = _convert_dsn(container.get_connection_url())

    conn = None
    for attempt in range(1, 6):
        try:
            conn = psycopg2.connect(dsn)
            break
        except psycopg2.OperationalError:
            if attempt < 5:
                time.sleep(2)
    if conn is None:
        raise TimeoutError("Timed out waiting for Postgres to become available.")

    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute(LEGACY_SCHEMA_SQL)
    conn.close()

    yield dsn

    container.stop()


def test_migrate_on_preseeded_db_preserves_existing_data(preseeded_dsn: str) -> None:
    _run_flyway_migrate(preseeded_dsn)

    conn = psycopg2.connect(preseeded_dsn)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT timestamp_event FROM public_cps_za_test WHERE event_id = 'preexisting-row'")
            row = cursor.fetchone()
    finally:
        conn.close()

    assert row is not None
    assert 42 == row[0]


def test_migrate_on_preseeded_db_records_baseline(preseeded_dsn: str) -> None:
    _run_flyway_migrate(preseeded_dsn)

    conn = psycopg2.connect(preseeded_dsn)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT version FROM flyway_schema_history WHERE type = 'BASELINE'")
            baseline_versions = {row[0] for row in cursor.fetchall()}
    finally:
        conn.close()

    assert "1.4.0.0" in baseline_versions
