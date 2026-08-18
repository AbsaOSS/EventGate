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

-- Application database roles.
--
-- eventgate_owner  - owns the schema objects and may run DDL.
-- eventgate_writer - inserts/updates event data (main EventGate Lambda).
-- eventgate_reader - read-only access (EventStats Lambda).

DO
$do$
    BEGIN
        IF EXISTS (
                SELECT FROM pg_catalog.pg_roles
                WHERE rolname = 'eventgate_owner') THEN

            RAISE NOTICE 'Role "eventgate_owner" already exists. Skipping.';
        ELSE
            CREATE ROLE eventgate_owner WITH
                LOGIN
                NOSUPERUSER
                INHERIT
                NOCREATEDB
                NOCREATEROLE
                NOREPLICATION
                PASSWORD '${eventgate_owner_password}';
        END IF;
    END
$do$;

DO
$do$
    BEGIN
        IF EXISTS (
                SELECT FROM pg_catalog.pg_roles
                WHERE rolname = 'eventgate_writer') THEN
            RAISE NOTICE 'Role "eventgate_writer" already exists. Skipping.';
        ELSE
            CREATE ROLE eventgate_writer WITH
                LOGIN
                NOSUPERUSER
                INHERIT
                NOCREATEDB
                NOCREATEROLE
                NOREPLICATION
                PASSWORD '${eventgate_writer_password}';
        END IF;
    END
$do$;

DO
$do$
    BEGIN
        IF EXISTS (
                SELECT FROM pg_catalog.pg_roles
                WHERE rolname = 'eventgate_reader') THEN
            RAISE NOTICE 'Role "eventgate_reader" already exists. Skipping.';
        ELSE
            CREATE ROLE eventgate_reader WITH
                LOGIN
                NOSUPERUSER
                INHERIT
                NOCREATEDB
                NOCREATEROLE
                NOREPLICATION
                PASSWORD '${eventgate_reader_password}';
        END IF;
    END
$do$;
