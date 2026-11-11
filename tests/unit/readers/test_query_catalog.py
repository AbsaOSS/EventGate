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

from pathlib import Path

from aiosql.queries import Queries
import pytest
from pytest_mock import MockerFixture

from src.readers.query_catalog import load_queries, query_family


def _queries(tmp_path: Path, mocker: MockerFixture, detail: tuple[str, ...], aggregation: tuple[str, ...]) -> Queries:
    (tmp_path / "stats.sql").write_text("-- name: get_stats()\nSELECT 1;", encoding="utf-8")
    for filename, names in (("jobs_detail_queries.sql", detail), ("aggregation_queries.sql", aggregation)):
        sql = "\n".join(f"-- name: {name}()\nSELECT 1;" for name in names)
        (tmp_path / filename).write_text(sql, encoding="utf-8")
    mocker.patch("src.readers.query_catalog._SQL_DIR", tmp_path)
    return load_queries()


def test_discovers_additional_groups_without_python_registration(tmp_path: Path, mocker: MockerFixture) -> None:
    queries = _queries(
        tmp_path,
        mocker,
        ("first", "first_with_cursor", "second", "second_with_cursor"),
        ("totals_summary", "totals_timeline_hourly", "totals_timeline_daily"),
    )

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
def test_private_methods_are_not_public_queries(name: str, tmp_path: Path, mocker: MockerFixture) -> None:
    queries = _queries(
        tmp_path,
        mocker,
        ("detail", "detail_with_cursor"),
        ("totals_summary", "totals_timeline_hourly", "totals_timeline_daily"),
    )

    assert query_family(queries, name) is None


@pytest.mark.parametrize(
    "detail, aggregation",
    [
        (("detail",), ()),
        ((), ("totals_summary", "totals_timeline_hourly")),
        (("Detail", "Detail_with_cursor"), ()),
    ],
)
def test_rejects_incomplete_or_unexpected_groups(
    detail: tuple[str, ...], aggregation: tuple[str, ...], tmp_path: Path, mocker: MockerFixture
) -> None:
    with pytest.raises(RuntimeError, match="SQL query configuration error: Invalid SQL query groups") as exc:
        _queries(tmp_path, mocker, detail, aggregation)

    assert isinstance(exc.value.__cause__, ValueError)


def test_rejects_names_shared_by_both_families(tmp_path: Path, mocker: MockerFixture) -> None:
    with pytest.raises(RuntimeError, match="Conflicting public SQL query names: shared"):
        _queries(
            tmp_path,
            mocker,
            ("shared", "shared_with_cursor"),
            ("shared_summary", "shared_timeline_hourly", "shared_timeline_daily"),
        )
