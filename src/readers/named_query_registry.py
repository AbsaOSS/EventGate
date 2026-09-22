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

"""Registry of predefined, server-executed named queries.

Each named query maps a stable `query_name` to the aiosql query keys used to
run it (with and without a keyset-pagination cursor). Unknown query names are
rejected at the handler layer with a `400` response. New aggregation queries are
added here without introducing new routes.
"""

from dataclasses import dataclass

from src.utils.constants import QUERY_RUNS_JOBS_DETAIL


@dataclass(frozen=True)
class NamedQuery:
    """Definition of a single named query.

    Attributes:
        name: Stable identifier used in the request path.
        sql_key: aiosql query name for the non-cursor variant.
        sql_key_with_cursor: aiosql query name for the keyset-cursor variant.
    """

    name: str
    sql_key: str
    sql_key_with_cursor: str


SUPPORTED_QUERIES: dict[str, NamedQuery] = {
    QUERY_RUNS_JOBS_DETAIL: NamedQuery(
        name=QUERY_RUNS_JOBS_DETAIL,
        sql_key="get_runs_jobs_detail",
        sql_key_with_cursor="get_runs_jobs_detail_with_cursor",
    ),
}
