import os
import json
import unittest
from glob import glob

CONF_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "conf")

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

class TestConfigFiles(unittest.TestCase):
    def test_config_files_have_required_keys(self):
        # Pick up any config*.json (excluding access and topics)
        config_files = [
            f for f in glob(os.path.join(CONF_DIR, "config*.json"))
            if os.path.basename(f) not in {"access.json"}
        ]
        self.assertTrue(config_files, "No config files found matching pattern config*.json")
        for path in config_files:
            with self.subTest(config=path):
                data = load_json(path)
                missing = REQUIRED_CONFIG_KEYS - data.keys()
                self.assertFalse(missing, f"Config {path} missing keys: {missing}")

    def test_access_json_structure(self):
        path = os.path.join(CONF_DIR, "access.json")
        data = load_json(path)
        self.assertIsInstance(data, dict, "access.json must contain an object mapping topic -> list[user]")
        for topic, users in data.items():
            with self.subTest(topic=topic):
                self.assertIsInstance(topic, str)
                self.assertIsInstance(users, list, f"Topic {topic} value must be a list of users")
                self.assertTrue(all(isinstance(u, str) for u in users), f"All users for topic {topic} must be strings")

    def test_topic_json_schemas_basic(self):
        topic_files = glob(os.path.join(CONF_DIR, "topic_*.json"))
        self.assertTrue(topic_files, "No topic_*.json files found")
        for path in topic_files:
            with self.subTest(topic_file=path):
                schema = load_json(path)
                # Basic required structure
                self.assertEqual(schema.get("type"), "object", "Schema root 'type' must be 'object'")
                props = schema.get("properties")
                self.assertIsInstance(props, dict, "Schema must define 'properties' object")
                required = schema.get("required")
                self.assertIsInstance(required, list, "Schema must define 'required' list")
                # Each required field is present in properties
                missing_props = [r for r in required if r not in props]
                self.assertFalse(missing_props, f"Required fields missing in properties: {missing_props}")
                # Ensure property entries themselves have at least a type
                for name, definition in props.items():
                    self.assertIsInstance(definition, dict, f"Property {name} definition must be an object")
                    self.assertIn("type", definition, f"Property {name} must specify a 'type'")

if __name__ == "__main__":  # pragma: no cover
    unittest.main()

