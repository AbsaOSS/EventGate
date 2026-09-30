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

-- Forgotten columns (PKs) from the previous DDLs.
ALTER TABLE public.public_cps_za_runs ADD COLUMN IF NOT EXISTS internal_id SERIAL PRIMARY KEY;
ALTER TABLE public.public_cps_za_dlchange ADD COLUMN IF NOT EXISTS internal_id SERIAL PRIMARY KEY;

-- Writer needs the SERIAL sequence permissions to insert into the relevant tables.
GRANT USAGE, SELECT ON SEQUENCE public.public_cps_za_runs_internal_id_seq TO eventgate_writer;
GRANT USAGE, SELECT ON SEQUENCE public.public_cps_za_dlchange_internal_id_seq TO eventgate_writer;
