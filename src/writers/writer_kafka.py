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

"""
Kafka writer module.
Provides functionality for publishing messages to Kafka topics.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple
from confluent_kafka import Producer, KafkaException

from src.utils.trace_logging import log_payload_at_trace
from src.writers.writer import Writer

logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)

# Configurable flush timeouts and retries via env variables to avoid hanging indefinitely
_KAFKA_FLUSH_TIMEOUT_SEC = float(os.environ.get("KAFKA_FLUSH_TIMEOUT", "7"))
_MAX_RETRIES = int(os.environ.get("KAFKA_FLUSH_RETRIES", "3"))
_RETRY_BACKOFF_SEC = float(os.environ.get("KAFKA_RETRY_BACKOFF", "0.5"))


class WriterKafka(Writer):
    """
    Kafka writer for publishing messages to Kafka topics.
    The Kafka producer is created on the first write() call.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self._producer: Optional["Producer"] = None
        logger.debug("Initialized Kafka writer")

    def _create_producer(self) -> Optional[Producer]:
        """
        Create Kafka producer from config.

        Returns:
            None if bootstrap server not configured else Producer instance.
        """
        if "kafka_bootstrap_server" not in self.config or not self.config.get("kafka_bootstrap_server"):
            return None

        bootstrap = self.config["kafka_bootstrap_server"]
        producer_config: Dict[str, Any] = {"bootstrap.servers": bootstrap}

        if "kafka_sasl_kerberos_principal" in self.config and "kafka_ssl_key_path" in self.config:
            producer_config.update(
                {
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "GSSAPI",
                    "sasl.kerberos.service.name": "kafka",
                    "sasl.kerberos.keytab": self.config["kafka_sasl_kerberos_keytab_path"],
                    "sasl.kerberos.principal": self.config["kafka_sasl_kerberos_principal"],
                    "ssl.ca.location": self.config["kafka_ssl_ca_path"],
                    "ssl.certificate.location": self.config["kafka_ssl_cert_path"],
                    "ssl.key.location": self.config["kafka_ssl_key_path"],
                    "ssl.key.password": self.config["kafka_ssl_key_password"],
                }
            )
            logger.debug("Kafka producer will use SASL_SSL")

        return Producer(producer_config)

    def _flush_with_timeout(self, timeout: float) -> Optional[int]:
        """
        Flush the Kafka producer with a timeout.

        Args:
            timeout: Timeout in seconds.
        Returns:
            Number of messages still pending after flush (0 = all delivered).
            None if the producer stub doesn't provide a count.
        """
        if self._producer is None:
            return 0
        try:
            return self._producer.flush(timeout)
        except TypeError:
            return self._producer.flush()

    def write(self, topic_name: str, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Publish a message to Kafka.

        Args:
            topic_name: Kafka topic to publish to.
            message: JSON-serializable payload.
        Returns:
            Tuple of (success: bool, error_message: Optional[str]).
        """
        # Lazy initialization of Kafka producer
        if self._producer is None:
            self._producer = self._create_producer()

            # If no bootstrap server configured, skipping Kafka writer
            if self._producer is None:
                logger.debug("Kafka producer not initialized - skipping Kafka writer")
                return True, None

        log_payload_at_trace(logger, "Kafka", topic_name, message)

        errors: List[str] = []
        has_exception = False

        # Produce step
        try:
            logger.debug("Sending to Kafka %s", topic_name)
            self._producer.produce(
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
                remaining = self._flush_with_timeout(_KAFKA_FLUSH_TIMEOUT_SEC)
            except KafkaException as e:
                errors.append(f"Flush exception: {e}")
                has_exception = True

            # Treat None (flush returns None in some stubs) as success equivalent to 0 pending
            if (remaining is None or remaining == 0) and not errors:
                break
            if attempt < _MAX_RETRIES:
                logger.warning(
                    "Kafka flush pending (%s message(s) remain) attempt %d/%d", remaining, attempt, _MAX_RETRIES
                )
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

    def check_health(self) -> Tuple[bool, str]:
        """
        Check Kafka writer health.

        Returns:
            Tuple of (is_healthy: bool, message: str).
        """
        if not self.config.get("kafka_bootstrap_server"):
            return True, "not configured"

        try:
            if self._producer is None:
                self._producer = self._create_producer()
                logger.debug("Kafka producer initialized during health check")
            if self._producer is None:
                return False, "producer initialization failed"
            return True, "ok"
        except KafkaException as err:
            return False, str(err)
