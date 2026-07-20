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

"""OpenAPI specification and Swagger UI documentation endpoint handler."""

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Vendored Swagger UI assets inlined into the /docs page, keeping it self-hosted.
SWAGGER_UI_CSS_FILE = "swagger-ui.css"
SWAGGER_UI_JS_FILE = "swagger-ui-bundle.js"


class HandlerApi:
    """Manages the OpenAPI specification and Swagger UI documentation endpoints."""

    def __init__(self):
        self.api_spec: str = ""
        self._static_dir: str = os.path.abspath(
            os.path.join(os.path.dirname(__file__), os.pardir, "static", "swagger-ui")
        )
        self._docs_html: str = ""

    def with_api_definition_loaded(self) -> "HandlerApi":
        """Load the OpenAPI specification from api.yaml file.
        Returns:
            The current instance with loaded API definition.
        Raises:
            RuntimeError: If loading or reading the API specification fails.
        """
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
        api_path = os.path.join(project_root, "api.yaml")

        try:
            with open(api_path, "r", encoding="utf-8") as file:
                self.api_spec = file.read()

            if not self.api_spec:
                raise ValueError("API specification file is empty")

            logger.debug("Loaded API definition from %s.", api_path)
            return self
        except (FileNotFoundError, PermissionError, ValueError) as exc:
            logger.exception("Failed to load or read API specification from %s.", api_path)
            raise RuntimeError("API specification initialization failed") from exc

    def get_api(self) -> dict[str, Any]:
        """Return the OpenAPI specification.
        Returns:
            API Gateway response with OpenAPI spec.
        """
        logger.debug("Handling GET API.")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/yaml"},
            "body": self.api_spec,
        }

    def get_docs(self) -> dict[str, Any]:
        """Return a self-contained Swagger UI HTML page for browsing the API spec.
        The Swagger UI stylesheet and script are inlined from the vendored assets
        so the page renders with no external requests. The page is built once and
        cached for subsequent requests.
        Returns:
            API Gateway response with the Swagger UI HTML page.
        Raises:
            RuntimeError: If a vendored Swagger UI asset cannot be read from disk.
        """
        logger.debug("Handling GET docs.")
        if not self._docs_html:
            css = self._read_static_asset(SWAGGER_UI_CSS_FILE)
            java_script = self._read_static_asset(SWAGGER_UI_JS_FILE)
            self._docs_html = (
                "<!DOCTYPE html>\n"
                '<html lang="en">\n'
                "<head>\n"
                '  <meta charset="UTF-8">\n'
                "  <title>EventGate API Docs</title>\n"
                f"  <style>{css}</style>\n"
                "</head>\n"
                "<body>\n"
                '  <div id="swagger-ui"></div>\n'
                f"  <script>{java_script}</script>\n"
                "  <script>\n"
                "    window.ui = SwaggerUIBundle({\n"
                '      url: "./api",\n'
                '      dom_id: "#swagger-ui"\n'
                "    });\n"
                "  </script>\n"
                "</body>\n"
                "</html>"
            )
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "text/html"},
            "body": self._docs_html,
        }

    def _read_static_asset(self, filename: str) -> str:
        """Read a vendored Swagger UI asset from disk.
        Args:
            filename: Name of the asset file under the vendored Swagger UI directory.
        Returns:
            The asset file contents.
        Raises:
            RuntimeError: If the asset file cannot be read from disk.
        """
        asset_path = os.path.join(self._static_dir, filename)
        try:
            with open(asset_path, "r", encoding="utf-8") as file:
                content = file.read()
            logger.debug("Loaded static asset from %s.", asset_path)
            return content
        except (FileNotFoundError, PermissionError) as exc:
            logger.exception("Failed to read static asset from %s.", asset_path)
            raise RuntimeError("Static asset read failed") from exc
