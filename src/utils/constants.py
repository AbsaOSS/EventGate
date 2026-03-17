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

"""Constants and enums used across the project."""

from typing import Any

# Configuration keys
TOKEN_PROVIDER_URL_KEY = "token_provider_url"
TOKEN_PUBLIC_KEY_URL_KEY = "token_public_key_url"
TOKEN_PUBLIC_KEYS_URL_KEY = "token_public_keys_url"
SSL_CA_BUNDLE_KEY = "ssl_ca_bundle"

# Postgres stats defaults
POSTGRES_DEFAULT_LIMIT = 50
POSTGRES_MAX_LIMIT = 1000
POSTGRES_DEFAULT_WINDOW_MS = 7 * 24 * 60 * 60 * 1000  # 7 days in milliseconds

SUPPORTED_TOPICS: list[str] = ["public.cps.za.runs"]

# Maps topic names to their PostgreSQL table(s)
TOPIC_TABLE_MAP: dict[str, dict[str, Any]] = {
    "public.cps.za.runs": {
        "main": "public_cps_za_runs",
        "jobs": "public_cps_za_runs_jobs",
        "columns": {
            "main": [
                "event_id",
                "job_ref",
                "tenant_id",
                "source_app",
                "source_app_version",
                "environment",
                "timestamp_start",
                "timestamp_end",
            ],
            "jobs": [
                "internal_id",
                "event_id",
                "country",
                "catalog_id",
                "status",
                "timestamp_start",
                "timestamp_end",
                "message",
                "additional_info",
            ],
        },
    },
    "public.cps.za.dlchange": {
        "main": "public_cps_za_dlchange",
        "columns": {
            "main": [
                "event_id",
                "tenant_id",
                "source_app",
                "source_app_version",
                "environment",
                "timestamp_event",
                "country",
                "catalog_id",
                "operation",
                "location",
                "format",
                "format_options",
                "additional_info",
            ],
        },
    },
    "public.cps.za.test": {
        "main": "public_cps_za_test",
        "columns": {
            "main": [
                "event_id",
                "tenant_id",
                "source_app",
                "environment",
                "timestamp_event",
                "additional_info",
            ],
        },
    },
}
