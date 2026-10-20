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

"""Typed result shapes for window-wide named-query aggregations.

These dataclasses are built by `ReaderPostgres.read_aggregations` and converted to a plain
dict (via `dataclasses.asdict`) only at the handler boundary, so the reader's internal
aggregation logic stays fully typed end to end.
"""

from dataclasses import dataclass, field

from src.utils.constants import TIMELINE_BUCKET_HOUR


@dataclass(frozen=True)
class AggregationsWindow:
    """The time window an aggregation result covers.

    Attributes:
        timestamp_start: Start of the window in epoch milliseconds.
        timestamp_end: End of the window in epoch milliseconds.
    """

    timestamp_start: int
    timestamp_end: int


@dataclass(frozen=True)
class TimelineEntry:
    """A single non-empty timeline bucket.

    Attributes:
        bucket_start: Start of the bucket in epoch milliseconds.
        total: Number of rows in the bucket.
        succeeded: Number of rows with raw status `succeeded`.
        failed: Number of rows with raw status `failed`.
    """

    bucket_start: int
    total: int
    succeeded: int
    failed: int


@dataclass(frozen=True)
class Timeline:
    """Bucketed heatmap data for a window, oldest first, empty buckets skipped.

    Attributes:
        bucket: Bucket width, `"hour"` or `"day"`.
        entries: Non-empty buckets ordered oldest-first.
    """

    bucket: str
    entries: list[TimelineEntry]


@dataclass(frozen=True)
class RunsJobsAggregations:
    """Window-wide summary and timeline aggregations for `runs_jobs_detail_aggregation`.

    Attributes:
        window: The time window the aggregations cover.
        total_runs: Count of job rows in the window (same grain as `runs_jobs_detail`).
        total_tenants: Number of distinct tenants in the window.
        total_catalogs: Number of distinct catalogs in the window.
        succeeded: Count of rows with raw status `succeeded`.
        failed: Count of rows with raw status `failed`.
        success_rate: `succeeded / total_runs * 100`, rounded to 1 decimal, `0.0` when empty.
        avg_duration_ms: Average job duration in milliseconds, or `None` when no row has
            both `timestamp_start` and `timestamp_end`.
        timeline: Heatmap-ready bucketed data for the window.
    """

    window: AggregationsWindow
    total_runs: int = 0
    total_tenants: int = 0
    total_catalogs: int = 0
    succeeded: int = 0
    failed: int = 0
    success_rate: float = 0.0
    avg_duration_ms: int | None = None
    timeline: Timeline = field(default_factory=lambda: Timeline(bucket=TIMELINE_BUCKET_HOUR, entries=[]))
