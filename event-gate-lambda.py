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

def getToken():
    return {
        "statusCode": 200,
        "headers": {
            "auth-token": "no token for you",
            "refresh-token": "no refreshToken for you either"
        }
    }
    
def getTopics():
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(["GreatTopic", "SuperTopic", "YetAnotherTopic"])
    }
    
def getTopicSchema(topicName):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": """ 
        "root": 
        {
            "item1" = "value",
            .... and other schema marvels here
        }
        """
    }

def postTopicMessage(topicName, topicMessage, jwt):
    return {"statusCode": 202}

def unknown():
    return {"statusCode": 404}

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
    return unknown()