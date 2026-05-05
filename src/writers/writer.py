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

"""Abstract base class for all EventGate writers."""

from abc import ABC, abstractmethod
from typing import Any


class WriteError(Exception):
    """Raised when a writer fails to publish a message."""


class HealthCheckError(Exception):
    """Raised when a health check detects a failure."""


class Writer(ABC):
    """Abstract base class for EventGate writers.
    All writers inherit from this class and implement the write() method.
    Writers use lazy initialization.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    @abstractmethod
    def write(self, topic_name: str, message: dict[str, Any]) -> None:
        """Publish a message to the target system.
        Args:
            topic_name: Target writer topic (destination) name.
            message: JSON-serializable payload to publish.
        Raises:
            WriteError: If publishing fails.
        """

    @abstractmethod
    def check_health(self) -> str | None:
        """Check writer health and connectivity.
        Returns:
            `None` when the writer is configured and healthy.
            A descriptive string (e.g. `"not configured"`) when intentionally disabled.
        Raises:
            HealthCheckError: If the writer is configured but failing.
        """
