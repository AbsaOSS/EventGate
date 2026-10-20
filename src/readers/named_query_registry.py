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

`SUPPORTED_QUERIES` maps each paginated `query_name` to its aiosql query keys, with and
without a keyset-pagination cursor. `SUPPORTED_AGGREGATION_QUERIES` holds window-wide
aggregation queries instead, which have no pagination and a different response shape.
Names absent from both are rejected as a `400` at the handler layer. New named queries
are added here, not as new routes.
"""

from dataclasses import dataclass

from src.utils.constants import QUERY_RUNS_JOBS_DETAIL, QUERY_RUNS_JOBS_DETAIL_AGGREGATION


@dataclass(frozen=True)
class NamedQuery:
    """Definition of a single paginated named query.

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


@dataclass(frozen=True)
class AggregationQuery:
    """Definition of a single window-wide aggregation named query.

    Attributes:
        name: Stable identifier used in the request path.
        summary_sql_key: aiosql query name for the single-row summary aggregates.
        timeline_hourly_sql_key: aiosql query name for hourly timeline buckets.
        timeline_daily_sql_key: aiosql query name for daily timeline buckets.
    """

    name: str
    summary_sql_key: str
    timeline_hourly_sql_key: str
    timeline_daily_sql_key: str


SUPPORTED_AGGREGATION_QUERIES: dict[str, AggregationQuery] = {
    QUERY_RUNS_JOBS_DETAIL_AGGREGATION: AggregationQuery(
        name=QUERY_RUNS_JOBS_DETAIL_AGGREGATION,
        summary_sql_key="get_runs_jobs_aggregations",
        timeline_hourly_sql_key="get_runs_jobs_timeline_hourly",
        timeline_daily_sql_key="get_runs_jobs_timeline_daily",
    ),
}
