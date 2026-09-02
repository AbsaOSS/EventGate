/*
 * Copyright 2026 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- Initial EventGate schema.

-- Run header rows for the runs topic.
CREATE TABLE IF NOT EXISTS public_cps_za_runs (
    event_id VARCHAR(255) NOT NULL,
    job_ref VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    source_app VARCHAR(255) NOT NULL,
    source_app_version VARCHAR(255) NOT NULL,
    environment VARCHAR(255) NOT NULL,
    timestamp_start BIGINT,
    timestamp_end BIGINT
);

-- Per-job rows belonging to a run.
CREATE TABLE IF NOT EXISTS public_cps_za_runs_jobs (
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

-- Data lake change events.
CREATE TABLE IF NOT EXISTS public_cps_za_dlchange (
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

-- Test topic events.
CREATE TABLE IF NOT EXISTS public_cps_za_test (
    event_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    source_app VARCHAR(255) NOT NULL,
    environment VARCHAR(255) NOT NULL,
    timestamp_event BIGINT,
    additional_info JSONB
);

-- Aggregated latest status per job (see ADR 001).
CREATE TABLE IF NOT EXISTS public_cps_za_status_change_aggregated_job (
    job_id               UUID PRIMARY KEY,
    job_group_id         UUID,
    parent_job_id        UUID,
    initial_job_id       UUID,
    job_ref              TEXT,
    job_name             TEXT,
    definition_id        TEXT,
    definition_version   TEXT,
    tenant_id            TEXT,
    country              TEXT,
    source_app           TEXT,
    source_app_version   TEXT,
    environment          TEXT,
    platform             TEXT,
    platform_metadata    JSONB,
    input_arguments      JSONB,
    additional_context   JSONB,
    attempt_number       INTEGER NOT NULL DEFAULT 1 CHECK (attempt_number > 0),
    status_type          TEXT CHECK (status_type IN ('WAITING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'KILLED')),
    status_subtype       TEXT,
    status_detail        TEXT,
    created_at           TIMESTAMPTZ,
    started_at           TIMESTAMPTZ,
    finished_at          TIMESTAMPTZ,
    last_updated_at      TIMESTAMPTZ NOT NULL
);
