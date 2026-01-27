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

"""
This module provides abstract base class for all EventGate writers.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple


class Writer(ABC):
    """
    Abstract base class for EventGate writers.
    All writers inherit from this class and implement the write() method. Writers use lazy initialization.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

    @abstractmethod
    def write(self, topic_name: str, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Publish a message to the target system.

        Args:
            topic_name: Target writer topic (destination) name.
            message: JSON-serializable payload to publish.

        Returns:
            Tuple of (success: bool, error_message: Optional[str]).
            - (True, None) on success or when writer is disabled/skipped.
            - (False, "error description") on failure.
        """

    @abstractmethod
    def check_health(self) -> Tuple[bool, str]:
        """
        Check writer health and connectivity.

        Returns:
            Tuple of (is_healthy: bool, message: str).
            - (True, "ok") - configured and working.
            - (True, "not configured") - not configured, skipped.
            - (False, "error message") - configured but failing.
        """
