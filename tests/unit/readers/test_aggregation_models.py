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


from src.readers.aggregation_models import AggregationsWindow, RunsJobsAggregations


def test_defaults_are_zeroed_not_missing() -> None:
    aggregations = RunsJobsAggregations(window=AggregationsWindow(timestamp_start=1000, timestamp_end=2000))

    assert 0 == aggregations.total_runs
    assert 0 == aggregations.total_tenants
    assert 0 == aggregations.total_catalogs
    assert 0 == aggregations.succeeded
    assert 0 == aggregations.failed
    assert 0.0 == aggregations.success_rate
    assert aggregations.avg_duration_ms is None
    assert "hour" == aggregations.timeline.bucket
    assert [] == aggregations.timeline.entries


def test_default_timeline_instances_are_independent() -> None:
    """Each default `RunsJobsAggregations` gets its own `Timeline`/`entries` list, not a shared mutable default."""
    first = RunsJobsAggregations(window=AggregationsWindow(timestamp_start=1000, timestamp_end=2000))
    second = RunsJobsAggregations(window=AggregationsWindow(timestamp_start=3000, timestamp_end=4000))

    assert first.timeline is not second.timeline
    assert first.timeline.entries is not second.timeline.entries
