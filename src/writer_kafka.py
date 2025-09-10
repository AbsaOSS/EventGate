"""Kafka writer module.

Initializes a Confluent Kafka Producer and publishes messages for a topic.
"""

import json
import logging
from typing import Any, Dict, Optional, Tuple

from confluent_kafka import Producer

try:  # KafkaException may not exist in stubbed test module
    from confluent_kafka import KafkaException  # type: ignore
except (ImportError, ModuleNotFoundError):  # pragma: no cover - fallback for test stub

    class KafkaException(Exception):  # type: ignore
        pass


STATE: Dict[str, Any] = {"logger": logging.getLogger(__name__), "producer": None}


def init(logger: logging.Logger, config: Dict[str, Any]) -> None:
    """Initialize Kafka producer.

    Args:
        logger: Shared application logger.
        config: Configuration dictionary (expects 'kafka_bootstrap_server' plus optional SASL/SSL fields).
    Raises:
        ValueError: if required 'kafka_bootstrap_server' is missing or empty.
    """
    STATE["logger"] = logger

    if "kafka_bootstrap_server" not in config or not config.get("kafka_bootstrap_server"):
        raise ValueError("Missing required config: kafka_bootstrap_server")
    bootstrap = config["kafka_bootstrap_server"]

    producer_config: Dict[str, Any] = {"bootstrap.servers": bootstrap}
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

    if producer is None:
        logger.debug("Kafka producer not initialized - skipping")
        return True, None

    errors: list[Any] = []
    try:
        logger.debug(f"Sending to kafka {topic_name}")
        producer.produce(
            topic_name,
            key="",
            value=json.dumps(message).encode("utf-8"),
            callback=lambda err, msg: (errors.append(str(err)) if err is not None else None),
        )
        producer.flush()
    except KafkaException as e:  # narrow exception capture
        err_msg = f"The Kafka writer failed with unknown error: {str(e)}"
        logger.exception(err_msg)
        return False, err_msg

    if errors:
        msg = "; ".join(errors)
        logger.error(msg)
        return False, msg

    return True, None
