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
import time
from typing import Any, Dict, Optional, Tuple
from confluent_kafka import Producer

from src.utils.trace_logging import log_payload_at_trace

try:  # KafkaException may not exist in stubbed test module
    from confluent_kafka import KafkaException  # type: ignore
except (ImportError, ModuleNotFoundError):  # pragma: no cover - fallback for test stub

    class KafkaException(Exception):  # type: ignore
        """Fallback KafkaException if confluent_kafka is not installed."""


STATE: Dict[str, Any] = {"logger": logging.getLogger(__name__), "producer": None}
# Configurable flush timeouts and retries via env variables to avoid hanging indefinitely
_KAFKA_FLUSH_TIMEOUT_SEC = float(os.environ.get("KAFKA_FLUSH_TIMEOUT", "7"))
_MAX_RETRIES = int(os.environ.get("KAFKA_FLUSH_RETRIES", "3"))
_RETRY_BACKOFF_SEC = float(os.environ.get("KAFKA_RETRY_BACKOFF", "0.5"))


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

    log_payload_at_trace(logger, "Kafka", topic_name, message)

    errors: list[str] = []
    has_exception = False

    # Produce step
    try:
        logger.debug("Sending to kafka %s", topic_name)
        producer.produce(
            topic_name,
            key="",
            value=json.dumps(message).encode("utf-8"),
            callback=lambda err, msg: (errors.append(str(err)) if err is not None else None),
        )
    except KafkaException as e:
        errors.append(f"Produce exception: {e}")
        has_exception = True

    # Flush step (always attempted)
    remaining: Optional[int] = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            remaining = flush_with_timeout(producer, _KAFKA_FLUSH_TIMEOUT_SEC)
        except KafkaException as e:
            errors.append(f"Flush exception: {e}")
            has_exception = True

        # Treat None (flush returns None in some stubs) as success equivalent to 0 pending
        if (remaining is None or remaining == 0) and not errors:
            break
        if attempt < _MAX_RETRIES:
            logger.warning("Kafka flush pending (%s message(s) remain) attempt %d/%d", remaining, attempt, _MAX_RETRIES)
            time.sleep(_RETRY_BACKOFF_SEC)

    # Warn if messages still pending after retries
    if isinstance(remaining, int) and remaining > 0:
        logger.warning(
            "Kafka flush timeout after %ss: %d message(s) still pending", _KAFKA_FLUSH_TIMEOUT_SEC, remaining
        )

    if errors:
        failure_text = "Kafka writer failed: " + "; ".join(errors)
        (logger.exception if has_exception else logger.error)(failure_text)
        return False, failure_text

    return True, None


def flush_with_timeout(producer, timeout: float) -> Optional[int]:
    """Flush the Kafka producer with a timeout, handling TypeError for stubs.

    Args:
        producer: Kafka Producer instance.
        timeout: Timeout in seconds.
    Returns:
        Number of messages still pending after the flush call (0 all messages delivered).
        None is returned only if the underlying (stub/mock) producer.flush() does not provide a count.
    """
    try:
        return producer.flush(timeout)
    except TypeError:  # Fallback for stub producers without timeout parameter
        return producer.flush()
