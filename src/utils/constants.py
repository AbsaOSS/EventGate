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

# Configuration keys
TOKEN_PROVIDER_URL_KEY = "token_provider_url"
TOKEN_PUBLIC_KEY_URL_KEY = "token_public_key_url"
TOKEN_PUBLIC_KEYS_URL_KEY = "token_public_keys_url"
SSL_CA_BUNDLE_KEY = "ssl_ca_bundle"

# Postgres connection
POSTGRES_CONNECT_TIMEOUT_SECONDS = 5
POSTGRES_STATEMENT_TIMEOUT_MS = 30000
POSTGRES_MAX_RETRIES = 2
REQUIRED_CONNECTION_FIELDS = ("host", "user", "password", "port")

# Postgres stats defaults
POSTGRES_DEFAULT_LIMIT = 50
POSTGRES_MAX_LIMIT = 1000
POSTGRES_DEFAULT_WINDOW_MS = 7 * 24 * 60 * 60 * 1000  # 7 days in milliseconds

# Topic name constants
TOPIC_RUNS = "public.cps.za.runs"
TOPIC_DLCHANGE = "public.cps.za.dlchange"
TOPIC_TEST = "public.cps.za.test"

SUPPORTED_WRITE_TOPICS: frozenset[str] = frozenset({TOPIC_RUNS, TOPIC_DLCHANGE, TOPIC_TEST})
SUPPORTED_STATS_TOPICS: frozenset[str] = frozenset({TOPIC_RUNS})
