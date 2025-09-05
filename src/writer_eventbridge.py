import json

import boto3

def init(logger, CONFIG):
    global _logger
    global EVENT_BUS_ARN
    global aws_eventbridge
    
    _logger = logger
    
    aws_eventbridge = boto3.client('events')
    EVENT_BUS_ARN = CONFIG["event_bus_arn"] if "event_bus_arn" in CONFIG else ""
    _logger.debug("Initialized EVENTBRIDGE writer")

def write(topicName, message):
    if not EVENT_BUS_ARN:
        _logger.debug("No EventBus Arn - skipping")
        return True

    try:
        _logger.debug(f"Sending to eventBridge {topicName}")
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
            _logger.error(str(response))
            return False
    except Exception as e:
        _logger.error(f'The EventBridge writer with unknown error: {str(e)}')
        return False
        
    return True
