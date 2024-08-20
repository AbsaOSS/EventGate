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

TOKEN_PROVIDER_URL = "https://login-service-dev.npintdebdtools.aws.dsarena.com/token/generate"
TOPICS = {
    "RunTopic": {
        "properties": {
            "app_id_snow": {
                "description": "Application ID or ServiceNow identifier",
                "type": "string"
            },
            "data_definition_id": {
                "description": "Identifier for the data definition",
                "type": "string"
            },
            "environment": {
                "description": "Environment",
                "type": "string"
            },
            "id": {
                "description": "Unique identifier for the event (GUID)",
                "type": "string"
            },
            "job_ref": {
                "description": "Identifier of the job in it’s respective system.",
                "type": "string"
            },
            "message": {
                "description": "Pipeline status message.",
                "type": "string"
            },
            "source_app": {
                "description": "Source application name",
                "type": "string"
            },
            "status": {
                "description": "Status of the run. Does not speak of the quality.",
                "enum": [
                    "Finished",
                    "Failed",
                    "Killed"
                ],
                "type": "string"
            },
            "timestamp_end": {
                "description": "End timestamp of the run in epoch milliseconds",
                "type": "number"
            },
            "timestamp_start": {
                "description": "Start timestamp of the run in epoch milliseconds",
                "type": "number"
            }
        },
        "required": [
            "guid",
            "app_id_snow",
            "source_app",
            "timestamp_start",
            "timestamp_end",
            "data_definition_id",
            "status"
        ],
        "type": "object"
    },
    "EdlaChangeTopic": {
        "properties": {
            "app_id_snow": {
                "description": "Application ID or ServiceNow identifier",
                "type": "string"
            },
            "data_definition_id": {
                "description": "Identifier for the data definition",
                "type": "string"
            },
            "environment": {
                "description": "Environment",
                "type": "string"
            },
            "format": {
                "description": "Format of the data",
                "type": "string"
            },
            "id": {
                "description": "Unique identifier for the event (GUID)",
                "type": "string"
            },
            "location": {
                "description": "Location of the data",
                "type": "string"
            },
            "operation": {
                "description": "Operation performed",
                "enum": [
                    "CREATE",
                    "UPDATE",
                    "ARCHIVE"
                ],
                "type": "string"
            },
            "schema_link": {
                "description": "Link to the data schema",
                "type": "string"
            },
            "source_app": {
                "description": "Source application name",
                "type": "string"
            },
            "timestamp_event": {
                "description": "Timestamp of the event in epoch milliseconds",
                "type": "number"
            }
        },
        "required": [
            "guid",
            "app_id_snow",
            "source_app",
            " timestamp_event ",
            "data_definition_id",
            "operation",
            "location",
            "format",
            "schema_link"
        ],
        "type": "object"
    }
}


def getToken():
    return {
        "statusCode": 303,
        "headers": {
            "Location": TOKEN_PROVIDER_URL,
        }
    }
    
def getTopics():
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps([topicName for topicName in TOPICS])
    }
    
def getTopicSchema(topicName):
    if topicName not in TOPICS:
        return { "statusCode": 404 }    
        
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(TOPICS[topicName])
    }

def postTopicMessage(topicName, topicMessage, jwt):
    # TODO: JWT INVALID

    # TODO: JWT EXPIRED
    
    if topicName not in TOPICS:
        return { "statusCode": 404 } 

    # TODO: USER UNATHORIZED

    # TODO: JSON NOT ACCORDING TO SCHEMA

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