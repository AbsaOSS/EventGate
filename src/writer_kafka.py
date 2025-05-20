import json

import boto3
from confluent_kafka import Producer

def init(logger, CONFIG):
    global _logger
    global kafka_producer
    
    _logger = logger
    
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
        _logger.debug("producer will use SASL_SSL")
    kafka_producer = Producer(producer_config)
    _logger.debug("Initialized KAFKA writer")

def write(topicName, message):
    try:
        _logger.debug(f"Sending to kafka {topicName}")
        error = []
        kafka_producer.produce(topicName, 
                               key="", 
                               value=json.dumps(message).encode("utf-8"),
                               callback = lambda err, msg: error.append(err) if err is not None else None)
        kafka_producer.flush()
        if error:
            _logger.error(str(error))
            return False
    except Exception as e:
        _logger.error(str(e))
        return False
        
    return True
