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


import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.utils.config_loader import _normalize_access_config, load_access_config, load_config, load_topic_names


@pytest.fixture
def conf_dir(tmp_path: Path) -> str:
    """Create a temporary config directory with topic schemas."""
    # Write config.json.
    config_data = {"token_provider_url": "http://test", "access_config": str(tmp_path / "access.json")}
    (tmp_path / "config.json").write_text(json.dumps(config_data), encoding="utf-8")

    # Write access.json.
    (tmp_path / "access.json").write_text(
        json.dumps({"public.cps.za.runs": ["UserA"], "public.cps.za.test": ["UserB"]}),
        encoding="utf-8",
    )

    # Write topic_schemas.
    schemas_dir = tmp_path / "topic_schemas"
    schemas_dir.mkdir()
    (schemas_dir / "runs.json").write_text('{"type": "object"}', encoding="utf-8")
    (schemas_dir / "dlchange.json").write_text('{"type": "object"}', encoding="utf-8")
    (schemas_dir / "test.json").write_text('{"type": "object"}', encoding="utf-8")

    return str(tmp_path)


class TestLoadConfig:
    """Tests for load_config()."""

    def test_loads_config_from_json(self, conf_dir: str) -> None:
        """Test that config.json is parsed correctly."""
        result = load_config(conf_dir)

        assert "http://test" == result["token_provider_url"]

    def test_missing_config_file_raises(self, tmp_path: Path) -> None:
        """Test that missing config.json raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_config(str(tmp_path))


class TestLoadAccessConfig:
    """Tests for load_access_config()."""

    def test_loads_from_local_file(self, conf_dir: str) -> None:
        """Test loading access config from a local file path."""
        config = load_config(conf_dir)
        aws_s3 = MagicMock()

        result = load_access_config(config, aws_s3)

        assert {"UserA": {}} == result["public.cps.za.runs"]
        assert {"UserB": {}} == result["public.cps.za.test"]
        aws_s3.Bucket.assert_not_called()

    def test_loads_from_s3(self) -> None:
        """Test loading access config from S3 path."""
        config = {"access_config": "s3://my-bucket/conf/access.json"}
        access_data = {"public.cps.za.runs": ["S3User"]}

        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(access_data).encode("utf-8")

        mock_s3 = MagicMock()
        mock_s3.Bucket.return_value.Object.return_value.get.return_value = {"Body": mock_body}

        result = load_access_config(config, mock_s3)

        assert {"S3User": {}} == result["public.cps.za.runs"]
        mock_s3.Bucket.assert_called_once_with("my-bucket")
        mock_s3.Bucket.return_value.Object.assert_called_once_with("conf/access.json")


class TestLoadTopicNames:
    """Tests for load_topic_names()."""

    def test_discovers_all_topics(self, conf_dir: str) -> None:
        """Test that all three topics are discovered from schema files."""
        result = load_topic_names(conf_dir)

        assert 3 == len(result)
        assert "public.cps.za.runs" in result
        assert "public.cps.za.dlchange" in result
        assert "public.cps.za.test" in result

    def test_missing_schema_file_excluded(self, conf_dir: str) -> None:
        """Test that a missing schema file excludes that topic."""
        os.remove(os.path.join(conf_dir, "topic_schemas", "test.json"))

        result = load_topic_names(conf_dir)

        assert 2 == len(result)
        assert "public.cps.za.test" not in result

    def test_empty_schemas_dir(self, tmp_path: Path) -> None:
        """Test that empty topic_schemas returns empty list."""
        (tmp_path / "topic_schemas").mkdir()

        result = load_topic_names(str(tmp_path))

        assert [] == result


class TestNormalizeAccessConfig:
    """Tests for _normalize_access_config()."""

    def test_normalize_list_format(self) -> None:
        """Test that list format is converted to dict with empty permissions."""
        raw = {"topic": ["user1", "user2"]}

        result = _normalize_access_config(raw)

        assert {"user1": {}, "user2": {}} == result["topic"]

    def test_normalize_dict_format(self) -> None:
        """Test that dict format with permissions is preserved as-is."""
        raw = {"topic": {"user1": {"source_app": ["app1"]}, "user2": {}}}

        result = _normalize_access_config(raw)

        assert {"source_app": ["app1"]} == result["topic"]["user1"]
        assert {} == result["topic"]["user2"]

    def test_normalize_mixed_format(self) -> None:
        """Test that mixed list and dict formats are handled correctly."""
        raw = {
            "list.topic": ["userA"],
            "dict.topic": {"userB": {"environment": ["prod"]}},
        }

        result = _normalize_access_config(raw)

        assert {"userA": {}} == result["list.topic"]
        assert {"environment": ["prod"]} == result["dict.topic"]["userB"]

    @pytest.mark.parametrize(
        "raw,error_fragment",
        [
            ({"t": "bad"}, "expected list or dict"),
            ({"t": {"u": "not-a-dict"}}, "constraints must be a dict"),
            ({"t": {"u": {"field": "not-a-list"}}}, "patterns must be a list"),
        ],
        ids=["invalid-topic-value", "invalid-constraints", "invalid-patterns"],
    )
    def test_normalize_rejects_malformed_config(self, raw: dict, error_fragment: str) -> None:
        """Test that malformed access config raises ValueError."""
        with pytest.raises(ValueError, match=error_fragment):
            _normalize_access_config(raw)
