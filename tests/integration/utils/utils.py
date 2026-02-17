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

"""Utility functions for the integration testing."""
import time
from typing import Dict, Any


def create_runs_event(
    event_id: str,
    job_ref: str,
    tenant_id: str,
    source_app: str,
    catalog_id: str,
) -> Dict[str, Any]:
    """Create a runs event with standard structure."""
    now = int(time.time() * 1000)
    return {
        "event_id": event_id,
        "job_ref": job_ref,
        "tenant_id": tenant_id,
        "source_app": source_app,
        "source_app_version": "1.0.0",
        "environment": "test",
        "timestamp_start": now - 60000,
        "timestamp_end": now,
        "jobs": [
            {
                "catalog_id": catalog_id,
                "status": "succeeded",
                "timestamp_start": now - 60000,
                "timestamp_end": now,
            }
        ],
    }
