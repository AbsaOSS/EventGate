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

with open("conf/api.yaml", "r") as file:
    API = file.read()
logger.debug("Loaded API definition")

TOPICS = {}
with open("conf/topic_runs.json", "r") as file:
    TOPICS["public.cps.za.runs"] = json.load(file)
with open("conf/topic_dlchange.json", "r") as file:
    TOPICS["public.cps.za.dlchange"] = json.load(file)
with open("conf/topic_test.json", "r") as file:
    TOPICS["public.cps.za.test"] = json.load(file)
logger.debug("Loaded TOPICS")

with open("conf/config.json", "r") as file:
    CONFIG = json.load(file)
logger.debug("Loaded main CONFIG")

aws_s3 = boto3.Session().resource('s3', verify=False)
logger.debug("Initialized AWS S3 Client")

if CONFIG["access_config"].startswith("s3://"):
    name_parts = CONFIG["access_config"].split('/')
    bucket_name = name_parts[2]
    bucket_object = "/".join(name_parts[3:])
    ACCESS = json.loads(aws_s3.Bucket(bucket_name).Object(bucket_object).get()["Body"].read().decode("utf-8"))
else:
    with open(CONFIG["access_config"], "r") as file:
        ACCESS = json.load(file)
logger.debug("Loaded ACCESS definitions")

TOKEN_PROVIDER_URL = CONFIG["token_provider_url"]
token_public_key_encoded = requests.get(CONFIG["token_public_key_url"], verify=False).json()["key"]
TOKEN_PUBLIC_KEY = serialization.load_der_public_key(base64.b64decode(token_public_key_encoded))
logger.debug("Loaded TOKEN_PUBLIC_KEY")

writer_eventbridge.init(logger, CONFIG)
writer_kafka.init(logger, CONFIG)
writer_postgres.init(logger)

def get_api():
    return {
        "statusCode": 200,
        "body": API
    }

def get_token():
    logger.debug("Handling GET Token")
    return {
        "statusCode": 303,
        "headers": {"Location": TOKEN_PROVIDER_URL}
    }
    
def get_topics():
    logger.debug("Handling GET Topics")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps([topicName for topicName in TOPICS])
    }
    
def get_topic_schema(topicName):
    logger.debug(f"Handling GET TopicSchema({topicName})")
    if topicName not in TOPICS:
        return { "statusCode": 404 }    
        
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(TOPICS[topicName])
    }

def post_topic_message(topicName, topicMessage, tokenEncoded):
    logger.debug(f"Handling POST {topicName}")
    try:
        token = jwt.decode(tokenEncoded, TOKEN_PUBLIC_KEY, algorithms=["RS256"])
    except Exception as e:
         return {
            "statusCode": 401,
            "headers": {"Content-Type": "text/plain"},
            "body": str(e)
         }

    if topicName not in TOPICS:
        return { "statusCode": 404 } 

    user = token["sub"]
    if topicName not in ACCESS or user not in ACCESS[topicName]:
        return { "statusCode": 403 } 

    try:
        validate(instance=topicMessage, schema=TOPICS[topicName])
    except ValidationError as e:
         return {
            "statusCode": 400,
            "headers": {"Content-Type": "text/plain"},
            "body": e.message
         }
    
    success = (
        writer_kafka.write(topicName, topicMessage) and 
        writer_eventbridge.write(topicName, topicMessage) and 
        writer_postgres.write(topicName, topicMessage)
    )
    return {"statusCode": 202} if success else {"statusCode": 500}

def extract_token(eventHeaders):
    # Initial implementation used bearer header directly
    if "bearer" in eventHeaders:
        return eventHeaders["bearer"]
        
    if "Authorization" in eventHeaders and eventHeaders["Authorization"].startswith("Bearer "):
        return eventHeaders["Authorization"][len("Bearer "):]
        
    return "" # Will result in 401

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
                return post_topic_message(event["pathParameters"]["topic_name"].lower(), json.loads(event["body"]), extract_token(event["headers"]))  
        if event["resource"].lower() == "/terminate":
            sys.exit("TERMINATING")
        return {"statusCode": 404}
    except Exception as e:
        logger.error(f"Unexpected exception: {e}")
        return {"statusCode": 500}
