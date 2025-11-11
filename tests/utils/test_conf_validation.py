import os

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
from glob import glob
import pytest

CONF_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "conf")

REQUIRED_CONFIG_KEYS = {
    "access_config",
    "token_provider_url",
    "token_public_key_url",
    "kafka_bootstrap_server",
    "event_bus_arn",
}


def load_json(path):
    with open(path, "r") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def config_files():
    files = [f for f in glob(os.path.join(CONF_DIR, "config*.json")) if os.path.basename(f) not in {"access.json"}]
    assert files, "No config files found matching pattern config*.json"
    return files


@pytest.mark.parametrize("key", sorted(REQUIRED_CONFIG_KEYS))
def test_config_files_have_required_keys(config_files, key):
    for path in config_files:
        data = load_json(path)
        assert key in data, f"Config {path} missing key: {key}"


def test_access_json_structure():
    path = os.path.join(CONF_DIR, "access.json")
    data = load_json(path)
    assert isinstance(data, dict), "access.json must contain an object mapping topic -> list[user]"
    for topic, users in data.items():
        assert isinstance(topic, str)
        assert isinstance(users, list), f"Topic {topic} value must be a list of users"
        assert all(isinstance(u, str) for u in users), f"All users for topic {topic} must be strings"


@pytest.mark.parametrize("topic_file", glob(os.path.join(CONF_DIR, "topic_*.json")))
def test_topic_json_schemas_basic(topic_file):
    assert topic_file, "No topic_*.json files found"
    schema = load_json(topic_file)
    assert schema.get("type") == "object", "Schema root 'type' must be 'object'"
    props = schema.get("properties")
    assert isinstance(props, dict), "Schema must define 'properties' object"
    required = schema.get("required")
    assert isinstance(required, list), "Schema must define 'required' list"
    missing_props = [r for r in required if r not in props]
    assert not missing_props, f"Required fields missing in properties: {missing_props}"
    for name, definition in props.items():
        assert isinstance(definition, dict), f"Property {name} definition must be an object"
        assert "type" in definition, f"Property {name} must specify a 'type'"
