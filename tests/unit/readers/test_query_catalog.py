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

import aiosql
from aiosql.queries import Queries
import pytest

from src.readers.query_catalog import query_family, validate_query_groups


def _queries(detail: tuple[str, ...], aggregation: tuple[str, ...]) -> Queries:
    queries = aiosql.from_str("-- name: get_stats()\nSELECT 1;", "psycopg2")
    for family, names in (("jobs_detail", detail), ("aggregation", aggregation)):
        sql = "\n".join(f"-- name: {name}()\nSELECT 1;" for name in names)
        queries.add_child_queries(family, aiosql.from_str(sql, "psycopg2"))
    return queries


def test_discovers_additional_groups_without_python_registration() -> None:
    queries = _queries(
        ("first", "first_with_cursor", "second", "second_with_cursor"),
        ("totals_summary", "totals_timeline_hourly", "totals_timeline_daily"),
    )

    validate_query_groups(queries)

    assert "jobs_detail" == query_family(queries, "first")
    assert "jobs_detail" == query_family(queries, "second")
    assert "aggregation" == query_family(queries, "totals")


@pytest.mark.parametrize(
    "name",
    [
        "get_stats",
        "detail_with_cursor",
        "totals_summary",
        "unknown",
    ],
)
def test_private_methods_are_not_public_queries(name: str) -> None:
    queries = _queries(
        ("detail", "detail_with_cursor"),
        ("totals_summary", "totals_timeline_hourly", "totals_timeline_daily"),
    )
    validate_query_groups(queries)

    assert query_family(queries, name) is None


@pytest.mark.parametrize(
    "detail, aggregation",
    [
        (("detail",), ()),
        ((), ("totals_summary", "totals_timeline_hourly")),
        (("Detail", "Detail_with_cursor"), ()),
    ],
)
def test_rejects_incomplete_or_unexpected_groups(detail: tuple[str, ...], aggregation: tuple[str, ...]) -> None:
    queries = _queries(detail, aggregation)

    with pytest.raises(ValueError, match="Invalid SQL query groups"):
        validate_query_groups(queries)


def test_rejects_names_shared_by_both_families() -> None:
    queries = _queries(
        ("shared", "shared_with_cursor"),
        ("shared_summary", "shared_timeline_hourly", "shared_timeline_daily"),
    )

    with pytest.raises(ValueError, match="Conflicting public SQL query names: shared"):
        validate_query_groups(queries)
