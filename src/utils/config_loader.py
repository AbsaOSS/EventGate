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
from typing import Any

from boto3.resources.base import ServiceResource

logger = logging.getLogger(__name__)

# {topic: {user: {restricted_field: [allowed_values]}}}
TopicAccessMap = dict[str, dict[str, dict[str, list[str]]]]


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


def _normalize_access_config(access_data: dict[str, Any]) -> TopicAccessMap:
    """Normalize access config to unified internal format.
    Converts the legacy list format (`["user1", "user2"]`) to the dict
    format (`{"user1": {}, "user2": {}}`) so that all downstream code
    can rely on a single structure.
    Args:
        access_data: Parsed JSON from access config file (mixed list/dict values).
    Returns:
        Normalized mapping: `{topic: {user: {restricted_field: [allowed_values]}}}` .
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
                            f"Topic '{topic}', user '{user}', field '{field}': patterns must be a list, got {type(patterns).__name__}."
                        )
            result[topic] = value
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

    access_data: dict[str, Any] = {}

    if access_path.startswith("s3://"):
        name_parts = access_path.split("/")
        bucket_name = name_parts[2]
        bucket_object_key = "/".join(name_parts[3:])
        access_data = json.loads(
            aws_s3.Bucket(bucket_name).Object(bucket_object_key).get()["Body"].read().decode("utf-8")
        )
    else:
        with open(access_path, "r", encoding="utf-8") as file:
            access_data = json.load(file)

    logger.debug("Loaded access configuration.")
    return _normalize_access_config(access_data)


def load_topic_names(conf_dir: str) -> list[str]:
    """Discover topic names from the topic_schemas directory.
    Args:
        conf_dir: Path to the configuration directory.
    Returns:
        List of topic name strings.
    """
    filename_to_topic = {
        "runs.json": "public.cps.za.runs",
        "dlchange.json": "public.cps.za.dlchange",
        "test.json": "public.cps.za.test",
    }
    schemas_dir = os.path.join(conf_dir, "topic_schemas")
    topics: list[str] = []

    for filename, topic_name in filename_to_topic.items():
        schema_path = os.path.join(schemas_dir, filename)
        if os.path.isfile(schema_path):
            topics.append(topic_name)

    logger.debug("Discovered %d topic(s) from %s.", len(topics), schemas_dir)
    return topics
