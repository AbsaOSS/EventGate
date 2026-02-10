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

"""PostgreSQL schema for integration tests."""

SCHEMA_SQL = """
-- Test topic table matching WriterPostgres test configuration
CREATE TABLE IF NOT EXISTS public_cps_za_test (
    event_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    source_app VARCHAR(255) NOT NULL,
    environment VARCHAR(255) NOT NULL,
    timestamp_event BIGINT,
    additional_info JSONB
);
"""
