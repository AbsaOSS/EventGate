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
import os
import base64
import json
import logging
import sys
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from cryptography.hazmat.primitives import serialization
from jsonschema import validate
from jsonschema.exceptions import ValidationError
import jwt
import requests
import psycopg2

import boto3
from confluent_kafka import Producer

logger = logging.getLogger(__name__)
log_level = os.environ.get('LOG_LEVEL', 'INFO')
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
logger.debug("Loaded TOPICS")

with open("conf/config.json", "r") as file:
    CONFIG = json.load(file)
logger.debug("Loaded main CONFIG")

aws_session = boto3.Session()
aws_s3 = aws_session.resource('s3', verify=False)
aws_eventbridge = boto3.client('events')
logger.debug("Initialized AWS Clients")

if CONFIG["access_config"].startswith("s3://"):
    name_parts = CONFIG["access_config"].split('/')
    bucket_name = name_parts[2]
    bucket_object = "/".join(name_parts[3:])
    ACCESS = json.loads(aws_s3.Bucket(bucket_name).Object(bucket_object).get()["Body"].read().decode("utf-8"))
else:
    with open(CONFIG["access_config"], "r") as file:
        ACCESS = json.load(file)
logger.debug("Loaded ACCESS definitions")
    
if "event_bus_arn" in CONFIG:
    EVENT_BUS_ARN = CONFIG["event_bus_arn"]
else:
    EVENT_BUS_ARN = ""

TOKEN_PROVIDER_URL = CONFIG["token_provider_url"]
token_public_key_encoded = requests.get(CONFIG["token_public_key_url"], verify=False).json()["key"]
TOKEN_PUBLIC_KEY = serialization.load_der_public_key(base64.b64decode(token_public_key_encoded))
logger.debug("Loaded TOKEN_PUBLIC_KEY")

producer_config = {"bootstrap.servers": CONFIG["kafka_bootstrap_server"]}
if "kafka_sasl_kerberos_principal" in CONFIG and "kafka_ssl_key_path" in CONFIG:
    producer_config.update({
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "GSSAPI",
        "sasl.kerberos.service.name": "kafka",
        "sasl.kerberos.keytab": CONFIG["kafka_sasl_kerberos_keytab_path"],
        "sasl.kerberos.principal": CONFIG["kafka_sasl_kerberos_principal"],
        "ssl.ca.location": CONFIG["kafka_ssl_ca_path"],
        "ssl.certificate.location": CONFIG["kafka_ssl_cert_path"],
        "ssl.key.location": CONFIG["kafka_ssl_key_path"],
        "ssl.key.password": CONFIG["kafka_ssl_key_password"]
    })
    logger.debug("producer will use SASL_SSL")

kafka_producer = Producer(producer_config)
logger.debug("Initialized KAFKA producer")

def kafka_write(topicName, message):
    logger.debug(f"Sending to kafka {topicName}")
    error = []
    kafka_producer.produce(topicName, 
                           key="", 
                           value=json.dumps(message).encode("utf-8"),
                           callback = lambda err, msg: error.append(err) if err is not None else None)
    kafka_producer.flush()
    if error:
        raise Exception(error)

def event_bridge_write(topicName, message):
    if not EVENT_BUS_ARN:
        logger.debug("No EventBus Arn - skipping")
        return

    logger.debug(f"Sending to eventBridge {topicName}")
    response = aws_eventbridge.put_events(
        Entries=[
            {
                "Source": topicName,
                'DetailType': 'JSON',
                'Detail': json.dumps(message),
                'EventBusName': EVENT_BUS_ARN,
            }
        ]
    )
    if response["FailedEntryCount"] > 0:
        raise Exception(response)

def postgres_edla_write(cursor, table, message):
    cursor.execute(f"""
        INSERT INTO {table} 
        (
            event_id, 
            tenant_id, 
            source_app, 
            source_app_version, 
            environment, 
            timestamp_event, 
            catalog_id, 
            operation, 
            "location", 
            "format", 
            format_options, 
            additional_info
        ) 
        VALUES
        (
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s,
            %s, 
            %s, 
            %s
        )""", (
            message["event_id"],
            message["tenant_id"],
            message["source_app"],
            message["source_app_version"],
            message["environment"],
            message["timestamp_event"],
            message["catalog_id"],
            message["operation"],
            message["location"] if "location" in message else None,
            message["format"],
            json.dumps(message["format_options"]) if "format_options" in message else None,
            json.dumps(message["additional_info"] if "additional_info" in message else None)
        )
    )

def postgres_run_write(message):
    pass
    
def postgres_write(topicName, message):
    if not POSTGRES:
        logger.debug("No Postgress - skipping")
        return
        
    with psycopg2.connect(
        database=POSTGRES["database"],
        host=POSTGRES["host"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
        port=POSTGRESS["port"]
    ) as connection:
        with connection.cursor() as cursor:
            if topicName == "public.cps.za.dlchange":
                postgres_edla_write(cursor, "public_cps_za_dlchange", message)
            elif topic == "public.cps.za.runs":
                postgres_run_write(cursor, "public_cps_za_runs", message)
            else:
                raise Exception(f"unknown topic for postgres {topicName}")
                
        connection.commit()

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
    
    wasError = False
    try:
        kafka_write(topicName, topicMessage)
    except Exception as e:
        logger.error(str(e))
        wasError = True
    try:
        event_bridge_write(topicName, topicMessage)
    except Exception as e:
        logger.error(str(e))
        wasError = True
    try:
        postgres_write(topicName, topicMessage)
    except Exception as e:
        logger.error(str(e))
        wasError = True
    if wasError:
        return {"statusCode": 500}
    else:
        return {"statusCode": 202}

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
                return post_topic_message(event["pathParameters"]["topic_name"].lower(), json.loads(event["body"]), event["headers"]["bearer"])  
        if event["resource"].lower() == "/terminate":
            sys.exit("TERMINATING")
        return {"statusCode": 404}
    except Exception as e:
        logger.error(f"Unexpected exception: {e}")
        return {"statusCode": 500}
