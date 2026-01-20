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
This module provides the HandlerApi class for serving the OpenAPI specification.
"""

import logging
import os
from typing import Dict, Any

logger = logging.getLogger(__name__)


class HandlerApi:
    """
    HandlerApi manages the OpenAPI specification endpoint.
    """

    def __init__(self):
        self.api_spec: str = ""

    def load_api_definition(self) -> "HandlerApi":
        """
        Load the OpenAPI specification from api.yaml file.

        Returns:
            HandlerApi: The current instance with loaded API definition.
        """
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        api_path = os.path.join(project_root, "api.yaml")

        with open(api_path, "r", encoding="utf-8") as file:
            self.api_spec = file.read()
        logger.debug("Loaded API definition from %s", api_path)
        return self

    def get_api(self) -> Dict[str, Any]:
        """
        Return the OpenAPI specification.

        Returns:
            Dict[str, Any]: API Gateway response with OpenAPI spec.
        """
        logger.debug("Handling GET API")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/yaml"},
            "body": self.api_spec,
        }
