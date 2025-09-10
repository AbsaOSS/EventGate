#
# Copyright 2025 ABSA Group Limited
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

"""Kafka writer module.

Initializes a Confluent Kafka Producer and publishes messages for a topic.
"""

import json
import logging
import os
from typing import Any, Dict, Optional, Tuple

from confluent_kafka import Producer

try:  # KafkaException may not exist in stubbed test module
    from confluent_kafka import KafkaException  # type: ignore
except (ImportError, ModuleNotFoundError):  # pragma: no cover - fallback for test stub

    class KafkaException(Exception):  # type: ignore
        """Fallback KafkaException if confluent_kafka is not installed."""


STATE: Dict[str, Any] = {"logger": logging.getLogger(__name__), "producer": None}
# Configurable flush timeout (seconds) to avoid hanging indefinitely
_KAFKA_FLUSH_TIMEOUT_SEC = float(os.environ.get("KAFKA_FLUSH_TIMEOUT", "5"))


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
        logger.debug("Sending to kafka %s", topic_name)
        producer.produce(
            topic_name,
            key="",
            value=json.dumps(message).encode("utf-8"),
            callback=lambda err, msg: (errors.append(str(err)) if err is not None else None),
        )
        try:
            remaining = producer.flush(_KAFKA_FLUSH_TIMEOUT_SEC)  # type: ignore[arg-type]
        except TypeError:  # Fallback for stub producers without timeout parameter
            remaining = producer.flush()  # type: ignore[call-arg]
        # remaining can be number of undelivered messages (confluent_kafka returns int)
        if not errors and isinstance(remaining, int) and remaining > 0:
            timeout_msg = f"Kafka flush timeout after {_KAFKA_FLUSH_TIMEOUT_SEC}s: {remaining} message(s) still pending"
            logger.error(timeout_msg)
            return False, timeout_msg
    except KafkaException as e:  # narrow exception capture
        err_msg = f"The Kafka writer failed with unknown error: {str(e)}"
        logger.exception(err_msg)
        return False, err_msg

    if errors:
        msg = "; ".join(errors)
        logger.error(msg)
        return False, msg

    return True, None
