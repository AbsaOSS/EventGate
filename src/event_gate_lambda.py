#
# Copyright 2024 ABSA Group Limited
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
import base64
import json
import logging
import os
import sys
import urllib3

import boto3
import jwt
import requests
from cryptography.hazmat.primitives import serialization
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from . import conf_path  # new import for CONF_DIR resolution
try:  # fallback if relative import fails (e.g., executed as a script)
    from . import conf_path as _conf_mod
except Exception:  # pragma: no cover
    import conf_path as _conf_mod
conf_path = _conf_mod

# Remove old resolution logic, use module instead
_CONF_DIR = conf_path.CONF_DIR
_INVALID_CONF_ENV = conf_path.INVALID_CONF_ENV

sys.path.append(os.path.join(os.path.dirname(__file__)))

import writer_eventbridge
import writer_kafka
import writer_postgres

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)
logger.addHandler(logging.StreamHandler())
logger.debug("Initialized LOGGER")
logger.debug(f"Using CONF_DIR={_CONF_DIR}")
if _INVALID_CONF_ENV:
    logger.warning(
        f"CONF_DIR env var set to non-existent path: {_INVALID_CONF_ENV}; fell back to {_CONF_DIR}"
    )

with open(os.path.join(_CONF_DIR, "api.yaml"), "r") as file:
    API = file.read()
logger.debug("Loaded API definition")

TOPICS = {}
with open(os.path.join(_CONF_DIR, "topic_runs.json"), "r") as file:
    TOPICS["public.cps.za.runs"] = json.load(file)
with open(os.path.join(_CONF_DIR, "topic_dlchange.json"), "r") as file:
    TOPICS["public.cps.za.dlchange"] = json.load(file)
with open(os.path.join(_CONF_DIR, "topic_test.json"), "r") as file:
    TOPICS["public.cps.za.test"] = json.load(file)
logger.debug("Loaded TOPICS")

with open(os.path.join(_CONF_DIR, "config.json"), "r") as file:
    CONFIG = json.load(file)
logger.debug("Loaded main CONFIG")

aws_s3 = boto3.Session().resource("s3", verify=False)
logger.debug("Initialized AWS S3 Client")

if CONFIG["access_config"].startswith("s3://"):
    name_parts = CONFIG["access_config"].split("/")
    bucket_name = name_parts[2]
    bucket_object = "/".join(name_parts[3:])
    ACCESS = json.loads(
        aws_s3.Bucket(bucket_name)
        .Object(bucket_object)
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
else:
    with open(CONFIG["access_config"], "r") as file:
        ACCESS = json.load(file)
logger.debug("Loaded ACCESS definitions")

TOKEN_PROVIDER_URL = CONFIG["token_provider_url"]
token_public_key_encoded = requests.get(
    CONFIG["token_public_key_url"], verify=False
).json()["key"]
TOKEN_PUBLIC_KEY = serialization.load_der_public_key(
    base64.b64decode(token_public_key_encoded)
)
logger.debug("Loaded TOKEN_PUBLIC_KEY")

writer_eventbridge.init(logger, CONFIG)
writer_kafka.init(logger, CONFIG)
writer_postgres.init(logger)


def _error_response(status, err_type, message):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(
            {
                "success": False,
                "statusCode": status,
                "errors": [{"type": err_type, "message": message}],
            }
        ),
    }


def get_api():
    return {"statusCode": 200, "body": API}


def get_token():
    logger.debug("Handling GET Token")
    return {"statusCode": 303, "headers": {"Location": TOKEN_PROVIDER_URL}}


def get_topics():
    logger.debug("Handling GET Topics")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps([topicName for topicName in TOPICS]),
    }


def get_topic_schema(topicName):
    logger.debug(f"Handling GET TopicSchema({topicName})")
    if topicName not in TOPICS:
        return _error_response(404, "topic", f"Topic '{topicName}' not found")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(TOPICS[topicName]),
    }


def post_topic_message(topicName, topicMessage, tokenEncoded):
    logger.debug(f"Handling POST {topicName}")
    try:
        token = jwt.decode(tokenEncoded, TOKEN_PUBLIC_KEY, algorithms=["RS256"])
    except Exception:
        return _error_response(401, "auth", "Invalid or missing token")

    if topicName not in TOPICS:
        return _error_response(404, "topic", f"Topic '{topicName}' not found")

    user = token["sub"]
    if topicName not in ACCESS or user not in ACCESS[topicName]:
        return _error_response(403, "auth", "User not authorized for topic")

    try:
        validate(instance=topicMessage, schema=TOPICS[topicName])
    except ValidationError as e:
        return _error_response(400, "validation", e.message)

    # Run all writers independently (avoid short-circuit so failures in one don't skip others)
    kafka_ok, kafka_err = writer_kafka.write(topicName, topicMessage)
    eventbridge_ok, eventbridge_err = writer_eventbridge.write(topicName, topicMessage)
    postgres_ok, postgres_err = writer_postgres.write(topicName, topicMessage)

    errors = []
    if not kafka_ok:
        errors.append({"type": "kafka", "message": kafka_err})
    if not eventbridge_ok:
        errors.append({"type": "eventbridge", "message": eventbridge_err})
    if not postgres_ok:
        errors.append({"type": "postgres", "message": postgres_err})

    if errors:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"success": False, "statusCode": 500, "errors": errors}),
        }

    return {
        "statusCode": 202,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"success": True, "statusCode": 202}),
    }


def extract_token(eventHeaders):
    # Initial implementation used bearer header directly
    if "bearer" in eventHeaders:
        return eventHeaders["bearer"]

    if "Authorization" in eventHeaders and eventHeaders["Authorization"].startswith(
        "Bearer "
    ):
        return eventHeaders["Authorization"][len("Bearer ") :]

    return ""  # Will result in 401


def lambda_handler(event, context):
    try:
        if event["resource"].lower() == "/api":
            return get_api()
        if event["resource"].lower() == "/token":
            return get_token()
        if event["resource"].lower() == "/topics":
            return get_topics()
        if event["resource"].lower() == "/topics/{topic_name}":
            if event["httpMethod"] == "GET":
                return get_topic_schema(event["pathParameters"]["topic_name"].lower())
            if event["httpMethod"] == "POST":
                return post_topic_message(
                    event["pathParameters"]["topic_name"].lower(),
                    json.loads(event["body"]),
                    extract_token(event["headers"]),
                )
        if event["resource"].lower() == "/terminate":
            sys.exit("TERMINATING")
        return _error_response(404, "route", "Resource not found")
    except Exception as e:
        logger.error(f"Unexpected exception: {e}")
        return _error_response(500, "internal", "Unexpected server error")
