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

-- Object ownership and least-privilege grants for the application roles.

-- Owner: owns every table (and its sequences) in the public schema.
ALTER TABLE public_cps_za_runs OWNER TO eventgate_owner;
ALTER TABLE public_cps_za_runs_jobs OWNER TO eventgate_owner;
ALTER TABLE public_cps_za_dlchange OWNER TO eventgate_owner;
ALTER TABLE public_cps_za_test OWNER TO eventgate_owner;
ALTER TABLE public_cps_za_status_change_aggregated_job OWNER TO eventgate_owner;

-- Both application roles (writer and reader) need to access the public schema.
GRANT USAGE ON SCHEMA public TO eventgate_writer, eventgate_reader;

-- Reader: read-only access to EventGate data tables.
GRANT SELECT ON TABLE
    public_cps_za_runs,
    public_cps_za_runs_jobs,
    public_cps_za_dlchange,
    public_cps_za_test,
    public_cps_za_status_change_aggregated_job
TO eventgate_reader;

-- Writer: read and write EventGate data, but no DDL or migration metadata.
GRANT SELECT, INSERT, UPDATE ON TABLE
    public_cps_za_runs,
    public_cps_za_runs_jobs,
    public_cps_za_dlchange,
    public_cps_za_test,
    public_cps_za_status_change_aggregated_job
TO eventgate_writer;

-- Writer needs the SERIAL sequence (public_cps_za_runs_jobs.internal_id) to insert.
GRANT USAGE, SELECT ON SEQUENCE public_cps_za_runs_jobs_internal_id_seq TO eventgate_writer;
