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

"""Kafka writer for publishing messages to Kafka topics."""

import json
import logging
import os
import time
from typing import Any, Optional
from confluent_kafka import Producer, KafkaException

from src.utils.trace_logging import log_payload_at_trace
from src.writers.writer import HealthCheckError, WriteError, Writer

logger = logging.getLogger(__name__)

# Configurable flush timeouts and retries via env variables to avoid hanging indefinitely
_KAFKA_FLUSH_TIMEOUT_SEC = float(os.environ.get("KAFKA_FLUSH_TIMEOUT", "7"))
_MAX_RETRIES = int(os.environ.get("KAFKA_FLUSH_RETRIES", "3"))
_RETRY_BACKOFF_SEC = float(os.environ.get("KAFKA_RETRY_BACKOFF", "0.5"))


class WriterKafka(Writer):
    """Kafka writer for publishing messages to Kafka topics.
    The Kafka producer is created on the first write() call.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._producer: Optional["Producer"] = None
        logger.debug("Initialized Kafka writer.")

    def _create_producer(self) -> Producer | None:
        """Create Kafka producer from config.
        Returns:
            None if bootstrap server not configured else Producer instance.
        """
        if "kafka_bootstrap_server" not in self.config or not self.config.get("kafka_bootstrap_server"):
            return None

        bootstrap = self.config["kafka_bootstrap_server"]
        producer_config: dict[str, Any] = {"bootstrap.servers": bootstrap}

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
            logger.debug("Kafka producer will use SASL_SSL.")

        return Producer(producer_config)

    def _flush_with_timeout(self, timeout: float) -> int | None:
        """Flush the Kafka producer with a timeout.
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

    def write(self, topic_name: str, message: dict[str, Any], message_key: str = "") -> None:
        """Publish a message to Kafka.
        Args:
            topic_name: Kafka topic to publish to.
            message: JSON-serializable payload.
            message_key: Optional Kafka key used for partitioning.
        Raises:
            WriteError: If publishing fails.
        """
        # Lazy initialization of Kafka producer
        if self._producer is None:
            self._producer = self._create_producer()

            # If no bootstrap server configured, skipping Kafka writer
            if self._producer is None:
                logger.debug("Kafka producer not initialized - skipping Kafka writer.")
                return

        log_payload_at_trace(logger, "Kafka", message)

        errors: list[str] = []
        captured_exception: KafkaException | None = None
        delivery: dict[str, Any] = {}
        started_at = time.perf_counter()

        def delivery_report(err: Any, msg: Any) -> None:
            """Collect the Kafka delivery outcome for logging and error reporting."""
            if err is not None:
                errors.append(str(err))
                return
            try:
                delivery.update({"kafka_partition": msg.partition(), "kafka_offset": msg.offset()})
            except (AttributeError, TypeError):
                # The delivery succeeded; only the partition/offset detail is unavailable, because
                # not every producer implementation exposes message metadata. Swallowing this keeps
                # a successful write from being reported as a failure over a missing log field.
                logger.debug("Kafka delivery metadata unavailable.", exc_info=True)

        # Produce step
        try:
            logger.debug("Sending message to Kafka.", extra={"message_key": message_key})
            self._producer.produce(
                topic_name,
                key=message_key,
                value=json.dumps(message).encode("utf-8"),
                callback=delivery_report,
            )
        except KafkaException as e:
            errors.append(f"Produce exception: {e}")
            captured_exception = e

        # Flush step (always attempted)
        remaining: int | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                remaining = self._flush_with_timeout(_KAFKA_FLUSH_TIMEOUT_SEC)
            except KafkaException as e:
                errors.append(f"Flush exception: {e}")
                captured_exception = e

            # Treat None (flush returns None in some stubs) as success equivalent to 0 pending
            if remaining is None or remaining == 0:
                break
            if attempt < _MAX_RETRIES:
                logger.warning(
                    "Kafka flush pending, retrying.",
                    extra={
                        "pending_messages": remaining,
                        "attempt": attempt,
                        "max_attempts": _MAX_RETRIES,
                    },
                )
                time.sleep(_RETRY_BACKOFF_SEC)

        # Warn if messages still pending after retries
        if isinstance(remaining, int) and remaining > 0:
            logger.warning(
                "Kafka flush timed out with messages still pending.",
                extra={
                    "pending_messages": remaining,
                    "flush_timeout_sec": _KAFKA_FLUSH_TIMEOUT_SEC,
                },
            )

        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)

        if errors:
            failure_text = "Kafka writer failed: " + "; ".join(errors)
            # logger.exception() is only valid inside an except block; outside it the traceback is
            # taken from sys.exc_info() and would be empty, so pass the captured exception instead.
            logger.error(
                "Kafka writer failed.",
                exc_info=captured_exception,
                extra={"writer_duration_ms": duration_ms, "writer_errors": errors},
            )
            raise WriteError(failure_text)

        logger.debug(
            "Kafka accepted the message.",
            extra={"writer_duration_ms": duration_ms, **delivery},
        )

    def check_health(self) -> str | None:
        """Check Kafka writer health.
        Returns:
            `None` when healthy, `"not configured"` when intentionally disabled.
        Raises:
            HealthCheckError: If the Kafka producer cannot be initialized.
        """
        if not self.config.get("kafka_bootstrap_server"):
            return "not configured"

        try:
            if self._producer is None:
                self._producer = self._create_producer()
                logger.debug("Kafka producer initialized during health check.")
            if self._producer is None:
                raise HealthCheckError("producer initialization failed")
        except KafkaException as err:
            raise HealthCheckError(str(err)) from err

        return None
