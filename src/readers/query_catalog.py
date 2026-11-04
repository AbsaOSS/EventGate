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

"""Discover public query groups from aiosql's family namespaces.

Detail groups contain `<name>` and `<name>_with_cursor`; aggregation groups contain
`<name>_summary`, `<name>_timeline_hourly`, and `<name>_timeline_daily`.
Only complete groups are public. Root stats methods and generated cursor helpers are private.
"""

from typing import Literal

from aiosql.queries import Queries

type QueryFamily = Literal["jobs_detail", "aggregation"]

_FAMILY_SUFFIXES: dict[QueryFamily, tuple[str, ...]] = {
    "jobs_detail": ("_with_cursor", ""),
    "aggregation": ("_summary", "_timeline_hourly", "_timeline_daily"),
}


def validate_query_groups(queries: Queries) -> None:
    """Reject incomplete, unexpected, or conflicting public SQL groups."""
    public_names: set[str] = set()
    for family, suffixes in _FAMILY_SUFFIXES.items():
        available = set(getattr(queries, family).available_queries)
        names = {name.removesuffix(suffixes[0]) for name in available if name.endswith(suffixes[0])}
        # Every authored SELECT also has an aiosql-generated `<statement>_cursor` method.
        expected = {f"{name}{suffix}{helper}" for name in names for suffix in suffixes for helper in ("", "_cursor")}
        if available != expected or any(not name or name.endswith("_cursor") or name != name.lower() for name in names):
            raise ValueError(f"Invalid SQL query groups in {family}: {', '.join(sorted(available ^ expected))}.")
        if conflicts := public_names & names:
            raise ValueError(f"Conflicting public SQL query names: {', '.join(sorted(conflicts))}.")
        public_names.update(names)


def query_family(queries: Queries, query_name: str) -> QueryFamily | None:
    """Classify a public name in a validated query tree without exposing helper methods."""
    for family, suffixes in _FAMILY_SUFFIXES.items():
        if f"{query_name}{suffixes[0]}" in getattr(queries, family).available_queries:
            return family
    return None
