import json

import boto3

def init():
    aws_eventbridge = boto3.client('events')
    EVENT_BUS_ARN = CONFIG["event_bus_arn"] if "event_bus_arn" in CONFIG else ""
    logger.debug("Initialized EVENTBRIDGE writer")

def write(topicName, message):
    if not EVENT_BUS_ARN:
        logger.debug("No EventBus Arn - skipping")
        return True

    try:
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
            logger.error(str(response))
            return False
    except Exception as e:
        logger.error(str(e))
        return False
        
    return True
