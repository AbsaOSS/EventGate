"""Kafka writer module.

Initializes a Confluent Kafka Producer and publishes messages for a topic.
"""

import json
import logging
from typing import Any, Dict, Optional, Tuple

from confluent_kafka import Producer

STATE: Dict[str, Any] = {"logger": logging.getLogger(__name__), "producer": None}

# Module globals for typing
_logger: logging.Logger = logging.getLogger(__name__)
kafka_producer: Optional[Producer] = None


def init(logger: logging.Logger, config: Dict[str, Any]) -> None:
    """Initialize Kafka producer.

    Args:
        logger: Shared application logger.
        config: Configuration dictionary (expects 'kafka_bootstrap_server' plus optional SASL/SSL fields).
    """
    STATE["logger"] = logger

    producer_config: Dict[str, Any] = {"bootstrap.servers": config["kafka_bootstrap_server"]}
    if "kafka_sasl_kerberos_principal" in config and "kafka_ssl_key_path" in config:
        producer_config.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "GSSAPI",
                "sasl.kerberos.service.name": "kafka",
                "sasl.kerberos.keytab": config["kafka_sasl_kerberos_keytab_path"],
                "sasl.kerberos.principal": config["kafka_sasl_kerberos_principal"],
                "ssl.ca.location": config["kafka_ssl_ca_path"],
                "ssl.certificate.location": config["kafka_ssl_cert_path"],
                "ssl.key.location": config["kafka_ssl_key_path"],
                "ssl.key.password": config["kafka_ssl_key_password"],
            }
        )
        STATE["logger"].debug("Kafka producer will use SASL_SSL")

    STATE["producer"] = Producer(producer_config)
    STATE["logger"].debug("Initialized KAFKA writer")


def write(topic_name: str, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Publish a message to Kafka.

    Args:
        topic_name: Kafka topic to publish to.
        message: JSON-serializable payload.
    Returns:
        Tuple[success flag, optional error message].
    """
    logger = STATE["logger"]
    producer: Optional[Producer] = STATE.get("producer")  # type: ignore[assignment]

    try:
        if producer is None:
            logger.debug("Kafka producer not initialized - skipping")
            return True, None
        _logger.debug(f"Sending to kafka {topic_name}")
        errors: list[Any] = []
        kafka_producer.produce(
            topic_name,
            key="",
            value=json.dumps(message).encode("utf-8"),
            callback=lambda err, msg: (errors.append(str(err)) if err is not None else None),
        )
        kafka_producer.flush()
        if errors:
            msg = "; ".join(errors)
            _logger.error(msg)
            return False, msg
    except Exception as e:
        err_msg = f"The Kafka writer failed with unknown error: {str(e)}"
        _logger.error(err_msg)
        return False, err_msg

    return True, None
