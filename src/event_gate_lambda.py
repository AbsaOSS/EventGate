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
import sys
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from cryptography.hazmat.primitives import serialization
from jsonschema import validate
from jsonschema.exceptions import ValidationError
import jwt
import requests

from confluent_kafka import Producer

with open("conf/config.json", "r") as file:
    CONFIG = json.load(file)

with open(CONFIG["topicsConfig"], "r") as file:
    TOPICS = json.load(file)

with open(CONFIG["accessConfig"], "r") as file:
    ACCESS = json.load(file)
    
TOKEN_PROVIDER_URL = CONFIG["tokenProviderUrl"]
print("Loaded config")

token_public_key_encoded = requests.get(CONFIG["tokenPublicKeyUrl"], verify=False).json()["key"]
TOKEN_PUBLIC_KEY = serialization.load_der_public_key(base64.b64decode(token_public_key_encoded))
print("Loaded token public key")

kafka_producer = Producer({"bootstrap.servers": CONFIG["kafkaBootstrapServer"]})
print("Initialized kafka producer")

def kafkaWrite(topicName, message):
    print(f"Sending to kafka {topicName}")
    error = []
    kafka_producer.produce(topicName, 
                           key="", 
                           value=json.dumps(message).encode("utf-8"),
                           callback = lambda err, msg: error.append(err) if err is not None else None)
    kafka_producer.flush()
    if error:
        print(error)
        return 500
    else:
        print("OK")
        return 202

def getToken():
    print("Handling GET Token")
    return {
        "statusCode": 303,
        "headers": {"Location": TOKEN_PROVIDER_URL}
    }
    
def getTopics():
    print("Handling GET Topics")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps([topicName for topicName in TOPICS])
    }
    
def getTopicSchema(topicName):
    print(f"Handling GET TopicSchema({topicName})")
    if topicName not in TOPICS:
        return { "statusCode": 404 }    
        
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(TOPICS[topicName])
    }

def postTopicMessage(topicName, topicMessage, tokenEncoded):
    print(f"Handling POST {topicName}")
    token = jwt.decode(tokenEncoded, TOKEN_PUBLIC_KEY, algorithms=["RS256"])

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
    
    return {"statusCode": kafkaWrite(topicName, topicMessage)}

def lambda_handler(event, context):
    if event["resource"] == "/Token":
        return getToken()
    if event["resource"] == "/Topics":
        return getTopics()
    if event["resource"] == "/Topics/{topicName}":
        if event["httpMethod"] == "GET":
            return getTopicSchema(event["pathParameters"]["topicName"])
        if event["httpMethod"] == "POST":
            return postTopicMessage(event["pathParameters"]["topicName"], json.loads(event["body"]), event["headers"]["bearer"])    
    if event["resource"] == "/Terminate":
        sys.exit("TERMINATING")
    return {"statusCode": 404}