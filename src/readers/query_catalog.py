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

"""Load and discover public query groups from aiosql's family namespaces.

Detail groups contain `<name>` and `<name>_with_cursor`; aggregation groups contain
`<name>_summary`, `<name>_timeline_hourly`, and `<name>_timeline_daily`.
Only complete groups are public. Root stats methods and generated cursor helpers are private.
"""

from pathlib import Path
from typing import Literal

import aiosql
from aiosql.queries import Queries
from aiosql.utils import SQLLoadException, SQLParseException

type QueryFamily = Literal["jobs_detail", "aggregation"]
type QueryFamilyDefinition = tuple[str, tuple[str, ...]]

_SQL_DIR = Path(__file__).parent / "sql"

_QUERY_FAMILIES: dict[QueryFamily, QueryFamilyDefinition] = {
    "jobs_detail": ("jobs_detail_queries.sql", ("_with_cursor", "")),
    "aggregation": ("aggregation_queries.sql", ("_summary", "_timeline_hourly", "_timeline_daily")),
}


def load_queries() -> Queries:
    """Load and validate the SQL query tree, wrapping configuration failures."""
    try:
        queries = aiosql.from_path(_SQL_DIR / "stats.sql", "psycopg2")
        for family, (filename, _) in _QUERY_FAMILIES.items():
            queries.add_child_queries(family, aiosql.from_path(_SQL_DIR / filename, "psycopg2"))
        _validate_query_groups(queries)
    except (OSError, ValueError, SQLLoadException, SQLParseException) as exc:
        raise RuntimeError(f"SQL query configuration error: {exc}") from exc
    return queries


def _validate_query_groups(queries: Queries) -> None:
    """Reject incomplete, unexpected, or conflicting public SQL groups."""
    public_names: set[str] = set()
    for family, (_, suffixes) in _QUERY_FAMILIES.items():
        available = set(getattr(queries, family).available_queries)
        names = {name.removesuffix(suffixes[0]) for name in available if name.endswith(suffixes[0])}
        # Every authored SELECT also has an aiosql-generated `<statement>_cursor` method.
        expected = {f"{name}{suffix}{helper}" for name in names for suffix in suffixes for helper in ("", "_cursor")}
        if available != expected or any(not name or name.endswith("_cursor") or name != name.lower() for name in names):
            raise ValueError(f"Invalid SQL query groups in {family}: {', '.join(sorted(available ^ expected))}.")
        if conflicts := public_names & names:
            raise ValueError(f"Conflicting public SQL query names: {', '.join(sorted(conflicts))}.")
        public_names.update(names)


def load_query_family(queries: Queries, query_name: str) -> QueryFamily | None:
    """Classify a public name in a validated query tree without exposing helper methods."""
    for family, (_, suffixes) in _QUERY_FAMILIES.items():
        if f"{query_name}{suffixes[0]}" in getattr(queries, family).available_queries:
            return family
    return None
