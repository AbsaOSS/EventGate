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

"""Shared configuration loading utilities."""

import json
import logging
import os
import re
from typing import Any

from boto3.resources.base import ServiceResource

from src.utils.constants import TOPIC_DLCHANGE, TOPIC_RUNS, TOPIC_STATUS_CHANGE, TOPIC_TEST

logger = logging.getLogger(__name__)

FieldPatterns = dict[str, list[re.Pattern[str]]]
TopicAccessMap = dict[str, dict[str, FieldPatterns]]
TopicKeyMap = dict[str, str]


def load_config(conf_dir: str) -> dict[str, Any]:
    """Load the main configuration from config.json.
    Args:
        conf_dir: Path to the configuration directory.
    Returns:
        Parsed configuration dictionary.
    """
    config_path = os.path.join(conf_dir, "config.json")
    with open(config_path, "r", encoding="utf-8") as file:
        config: dict[str, Any] = json.load(file)
    logger.debug("Loaded main configuration from %s.", config_path)
    return config


def _load_json_from_path(path: str, aws_s3: ServiceResource) -> dict[str, Any]:
    """Load JSON data from either S3 (`s3://...`) or local file path.
    Args:
        path: JSON source path.
        aws_s3: Boto3 S3 resource used for S3-backed paths.
    Returns:
        Parsed JSON object.
    """
    if path.startswith("s3://"):
        name_parts = path.split("/")
        bucket_name = name_parts[2]
        bucket_object_key = "/".join(name_parts[3:])
        return json.loads(aws_s3.Bucket(bucket_name).Object(bucket_object_key).get()["Body"].read().decode("utf-8"))

    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def _compile_topic_patterns(
    topic: str,
    user_constraints: dict[str, dict[str, list[str]]],
) -> dict[str, FieldPatterns]:
    """Compile regex pattern strings into `re.Pattern` objects.
    Args:
        topic: Topic name.
        user_constraints: Per-user field constraint mapping with raw pattern strings.
    Returns:
        Same structure with pattern strings replaced by compiled `re.Pattern` objects.
    Raises:
        ValueError: If a pattern string is not a valid regular expression.
    """
    compiled: dict[str, FieldPatterns] = {}
    for user, constraints in user_constraints.items():
        compiled_fields: FieldPatterns = {}
        for field, patterns in constraints.items():
            compiled_patterns: list[re.Pattern[str]] = []
            for pattern in patterns:
                try:
                    compiled_patterns.append(re.compile(pattern))
                except re.error as exc:
                    raise ValueError(
                        f"Topic '{topic}', user '{user}', field '{field}': "
                        f"invalid regex pattern '{pattern}': {exc}."
                    ) from exc
            compiled_fields[field] = compiled_patterns
        compiled[user] = compiled_fields
    return compiled


def _normalize_access_config(access_data: dict[str, Any]) -> TopicAccessMap:
    """Normalize access config to unified internal format.
    Converts the legacy list format (`["user1", "user2"]`) to the dict
    format (`{"user1": {}, "user2": {}}`) so that all downstream code
    can rely on a single structure.
    Args:
        access_data: Parsed JSON from access config file (mixed list/dict values).
    Returns:
        Normalized mapping: `{topic: {user: {restricted_field: [compiled_patterns]}}}` .
    """
    result: TopicAccessMap = {}
    for topic, value in access_data.items():
        if isinstance(value, list):
            # Legacy format: plain user list with no field restrictions
            result[topic] = {user: {} for user in value}
        elif isinstance(value, dict):
            # New format: per-user field constraints already in the expected structure
            for user, constraints in value.items():
                if not isinstance(constraints, dict):
                    raise ValueError(
                        f"Topic '{topic}', user '{user}': constraints must be a dict, got {type(constraints).__name__}."
                    )
                for field, patterns in constraints.items():
                    if not isinstance(patterns, list):
                        raise ValueError(
                            f"Topic '{topic}', user '{user}', field '{field}':"
                            f" patterns must be a list, got {type(patterns).__name__}."
                        )
            result[topic] = _compile_topic_patterns(topic, value)
        else:
            raise ValueError(f"Topic '{topic}': expected list or dict, got {type(value).__name__}.")
    return result


def load_access_config(config: dict[str, Any], aws_s3: ServiceResource) -> TopicAccessMap:
    """Load access control configuration from S3 or a local file.
    Args:
        config: Main configuration dict (must contain `access_config` key).
        aws_s3: Boto3 S3 resource for loading from S3 paths.
    Returns:
        Normalized mapping of topic names to per-user permission constraints.
    """
    access_path: str = config["access_config"]
    logger.debug("Loading access configuration from %s.", access_path)
    access_data = _load_json_from_path(access_path, aws_s3)

    logger.debug("Loaded access configuration.")
    return _normalize_access_config(access_data)


def _validate_topic_keys_config(topic_key_data: dict[str, Any]) -> None:
    """Validate topic key mapping config.
    Args:
        topic_key_data: Parsed JSON mapping of topic to event property name.
    Returns:
        None
    Raises:
        ValueError: If any topic key mapping is not a string.
    """
    for topic, field_name in topic_key_data.items():
        if not isinstance(field_name, str):
            raise ValueError(f"Topic '{topic}': expected key field name as string, got {type(field_name).__name__}.")
    return


def load_topic_keys_config(config: dict[str, Any], aws_s3: ServiceResource) -> TopicKeyMap:
    """Load topic key configuration from S3 or a local file.
    Args:
        config: Main configuration dict. If `topic_keys_config` is absent, returns empty mapping.
        aws_s3: Boto3 S3 resource for loading from S3 paths.
    Returns:
        Mapping of topic name to message property used as Kafka key.
    """
    topic_keys_path = config.get("topic_keys_config")
    if not topic_keys_path:
        logger.debug("No topic_keys_config configured. Using empty topic key mapping.")
        return {}

    logger.debug("Loading topic key configuration from %s.", topic_keys_path)
    topic_key_data = _load_json_from_path(topic_keys_path, aws_s3)

    logger.debug("Loaded topic key configuration.")
    _validate_topic_keys_config(topic_key_data)
    return topic_key_data


def load_topic_names(conf_dir: str) -> list[str]:
    """Discover topic names from the topic_schemas directory.
    Args:
        conf_dir: Path to the configuration directory.
    Returns:
        List of topic name strings.
    """
    filename_to_topic = {
        "runs.json": TOPIC_RUNS,
        "dlchange.json": TOPIC_DLCHANGE,
        "test.json": TOPIC_TEST,
        "status_change.json": TOPIC_STATUS_CHANGE,
    }
    schemas_dir = os.path.join(conf_dir, "topic_schemas")
    topics: list[str] = []

    for filename, topic_name in filename_to_topic.items():
        schema_path = os.path.join(schemas_dir, filename)
        if os.path.isfile(schema_path):
            topics.append(topic_name)

    logger.debug("Discovered %d topic(s) from %s.", len(topics), schemas_dir)
    return topics
