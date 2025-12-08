#
# Copyright 2025 ABSA Group Limited
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

import json

from src.utils.utils import build_error_response

## build_error_response()
def test_build_error_response_structure():
    """Test that build_error_response returns correct response structure."""
    resp = build_error_response(404, "topic", "Topic not found")

    assert resp["statusCode"] == 404
    assert resp["headers"]["Content-Type"] == "application/json"

    body = json.loads(resp["body"])
    assert body["success"] is False
    assert body["statusCode"] == 404
    assert len(body["errors"]) == 1
    assert body["errors"][0]["type"] == "topic"
    assert body["errors"][0]["message"] == "Topic not found"
