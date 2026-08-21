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

from urllib.parse import urlparse

import psycopg2
import pytest
from psycopg2 import errors

from tests.integration.conftest import TEST_ROLE_PASSWORD

SELECTABLE_TABLE = "public_cps_za_test"
OWNED_TABLES = (
    "public_cps_za_runs",
    "public_cps_za_runs_jobs",
    "public_cps_za_dlchange",
    "public_cps_za_test",
    "public_cps_za_status_change_aggregated_job",
)


def _connect_as(dsn: str, role: str) -> "psycopg2.extensions.connection":
    parsed = urlparse(dsn)
    return psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port,
        dbname=parsed.path.lstrip("/"),
        user=role,
        password=TEST_ROLE_PASSWORD,
    )


class TestReaderRole:
    def test_reader_can_select(self, postgres_container: str) -> None:
        conn = _connect_as(postgres_container, "eventgate_reader")
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT 1 FROM {SELECTABLE_TABLE} LIMIT 1")
        finally:
            conn.close()

    def test_reader_cannot_insert(self, postgres_container: str) -> None:
        conn = _connect_as(postgres_container, "eventgate_reader")
        try:
            with conn.cursor() as cursor, pytest.raises(errors.InsufficientPrivilege):
                cursor.execute(
                    f"INSERT INTO {SELECTABLE_TABLE} "
                    "(event_id, tenant_id, source_app, environment, timestamp_event) "
                    "VALUES ('e', 't', 'app', 'env', 1)"
                )
        finally:
            conn.close()

    def test_reader_cannot_read_flyway_history(self, postgres_container: str) -> None:
        conn = _connect_as(postgres_container, "eventgate_reader")
        try:
            with conn.cursor() as cursor, pytest.raises(errors.InsufficientPrivilege):
                cursor.execute("SELECT version FROM flyway_schema_history")
        finally:
            conn.close()


class TestWriterRole:
    def test_writer_can_insert_and_select(self, postgres_container: str) -> None:
        conn = _connect_as(postgres_container, "eventgate_writer")
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO {SELECTABLE_TABLE} "
                    "(event_id, tenant_id, source_app, environment, timestamp_event) "
                    "VALUES ('writer-e', 't', 'app', 'env', 1)"
                )
                cursor.execute(f"SELECT 1 FROM {SELECTABLE_TABLE} LIMIT 1")
            conn.commit()
        finally:
            conn.close()

    def test_writer_cannot_drop_table(self, postgres_container: str) -> None:
        conn = _connect_as(postgres_container, "eventgate_writer")
        try:
            with conn.cursor() as cursor, pytest.raises(errors.InsufficientPrivilege):
                cursor.execute(f"DROP TABLE {SELECTABLE_TABLE}")
        finally:
            conn.close()

    def test_writer_cannot_modify_flyway_history(self, postgres_container: str) -> None:
        conn = _connect_as(postgres_container, "eventgate_writer")
        try:
            with conn.cursor() as cursor, pytest.raises(errors.InsufficientPrivilege):
                cursor.execute("UPDATE flyway_schema_history SET description = description")
        finally:
            conn.close()


class TestOwnerRole:
    def test_owner_owns_tables(self, postgres_container: str) -> None:
        conn = _connect_as(postgres_container, "eventgate_owner")
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT tablename FROM pg_tables " "WHERE schemaname = 'public' AND tableowner = 'eventgate_owner'"
                )
                owned = {row[0] for row in cursor.fetchall()}
        finally:
            conn.close()
        assert set(OWNED_TABLES) == owned

    def test_owner_can_alter_table(self, postgres_container: str) -> None:
        conn = _connect_as(postgres_container, "eventgate_owner")
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"ALTER TABLE {SELECTABLE_TABLE} ADD COLUMN tmp_col TEXT")
            conn.rollback()
        finally:
            conn.close()
