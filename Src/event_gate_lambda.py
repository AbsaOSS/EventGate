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
import json
from jsonschema import validate
from jsonschema.exceptions import ValidationError

TOKEN_PROVIDER_URL = "https://<redacted>"

with open("conf/topics.json", "r") as file:
    TOPICS = json.load(file)

with open("conf/access.json", "r") as file:
    ACCESS = json.load(file)

def getToken():
    return {
        "statusCode": 303,
        "headers": {"Location": TOKEN_PROVIDER_URL}
    }
    
def getTopics():
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps([topicName for topicName in TOPICS])
    }
    
def getTopicSchema(topicName):
    if topicName not in TOPICS:
        return { "statusCode": 404 }    
        
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(TOPICS[topicName])
    }

def postTopicMessage(topicName, topicMessage, jwt):
    # TODO: JWT INVALID

    # TODO: JWT EXPIRED
    
    if topicName not in TOPICS:
        return { "statusCode": 404 } 

    user = "FooUser" # TODO: PICK USER FROM JWT
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
    

    # TODO: SEND MESSAGE
    return {"statusCode": 202}

def lambda_handler(event, context):
    if event["resource"] == "/Token":
        return getToken()
    if event["resource"] == "/Topics":
        return getTopics()
    if event["resource"] == "/Topics/{topicName}":
        if event["httpMethod"] == "GET":
            return getTopicSchema(event["pathParameters"]["topicName"])
        if event["httpMethod"] == "POST":
            return postTopicMessage(event["pathParameters"]["topicName"], event["body"], event["headers"]["bearer"])    
    return {"statusCode": 404}