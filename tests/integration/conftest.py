#
# Copyright 2026 ABSA Group Limited
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

"""Pytest fixtures for EventGate integration tests using testcontainers."""

import json
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from threading import Thread
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
import docker
import psycopg2
import pytest
import requests as req_lib
from testcontainers.kafka import KafkaContainer
from testcontainers.localstack import LocalStackContainer
from testcontainers.postgres import PostgresContainer

from tests.integration.schemas.postgres_schema import SCHEMA_SQL
from tests.integration.utils.jwt_helper import create_test_jwt_keypair, generate_token

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent


# Mock JWT Provider (runs in-process via threading)
class MockJWTHandler(BaseHTTPRequestHandler):
    """HTTP handler for mock JWT provider."""

    private_key_pem: bytes = b""
    public_key_b64: str = ""

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path == "/keys":
            self._json_response(200, {"keys": [{"key": self.public_key_b64}]})
        elif self.path == "/health":
            self._json_response(200, {"status": "ok"})
        elif self.path == "/private-key":
            self.send_response(200)
            self.send_header("Content-Type", "application/x-pem-file")
            self.end_headers()
            self.wfile.write(self.private_key_pem)
        elif self.path == "/":
            self.send_response(303)
            self.send_header("Location", "http://localhost/login")
            self.end_headers()
        else:
            self._json_response(404, {"error": "Not found"})

    def _json_response(self, status: int, data: dict) -> None:
        """Send JSON response."""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def log_message(self, fmt: str, *args: Any) -> None:
        """Suppress default logging."""


def _start_mock_jwt_server(port: int, private_key_pem: bytes, public_key_b64: str) -> HTTPServer:
    """Start mock JWT HTTP server in background thread."""
    MockJWTHandler.private_key_pem = private_key_pem
    MockJWTHandler.public_key_b64 = public_key_b64
    server = HTTPServer(("127.0.0.1", port), MockJWTHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.debug("Mock JWT server started on port %s.", server.server_address[1])
    return server


# Docker image pre-pull
CONTAINER_IMAGES: List[str] = [
    "testcontainers/ryuk:0.8.1",
    "postgres:16",
    "confluentinc/cp-kafka:7.6.0",
    "localstack/localstack:latest",
]

_PULL_MAX_ATTEMPTS = 2
_PULL_BACKOFF_SECONDS = [10, 30]


def _pull_image_with_retry(client: docker.DockerClient, image: str) -> Tuple[str, bool, str]:
    """
    Pull a single Docker image with exponential-backoff retries.

    Returns:
        Tuple of (image_name, success, message).
    """
    for attempt in range(1, _PULL_MAX_ATTEMPTS + 1):
        try:
            logger.info("Pulling image %s (attempt %d/%d).", image, attempt, _PULL_MAX_ATTEMPTS)
            client.images.pull(image)
            logger.info("Image %s pulled successfully.", image)
            return image, True, "ok"
        except docker.errors.APIError as exc:
            backoff = _PULL_BACKOFF_SECONDS[min(attempt - 1, len(_PULL_BACKOFF_SECONDS) - 1)]
            logger.warning(
                "Failed to pull %s (attempt %d/%d): %s. Retrying in %ds.",
                image,
                attempt,
                _PULL_MAX_ATTEMPTS,
                exc,
                backoff,
            )
            time.sleep(backoff)
    return image, False, f"Failed to pull {image} after {_PULL_MAX_ATTEMPTS} attempts."


@pytest.fixture(scope="session", autouse=True)
def _prepull_images() -> None:
    """Pre-pull all required Docker images in parallel before starting containers."""
    client = docker.from_env(timeout=300)
    images_to_pull: List[str] = []
    for image in CONTAINER_IMAGES:
        try:
            client.images.get(image)
            logger.debug("Image %s already available locally.", image)
        except docker.errors.ImageNotFound:
            images_to_pull.append(image)

    if not images_to_pull:
        logger.debug("All container images already available.")
        return

    logger.info("Pre-pulling %d Docker image(s) in parallel: %s.", len(images_to_pull), images_to_pull)
    failures: List[str] = []
    with ThreadPoolExecutor(max_workers=len(images_to_pull)) as executor:
        pull_tasks = {executor.submit(_pull_image_with_retry, client, img): img for img in images_to_pull}
        for task in as_completed(pull_tasks):
            image_name, success, message = task.result()
            if not success:
                failures.append(message)

    if failures:
        pytest.exit("\n".join(["Docker image pre-pull failed:"] + failures), returncode=1)


# Container fixtures
def _convert_dsn(dsn: str) -> str:
    """Convert SQLAlchemy DSN to psycopg2 format."""
    return dsn.replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(scope="session")
def postgres_container() -> Generator[str, None, None]:
    """PostgreSQL container with initialized schema."""
    logger.debug("Starting PostgreSQL container.")
    container = PostgresContainer("postgres:16", dbname="eventgate")

    container.start()
    dsn = _convert_dsn(container.get_connection_url())
    logger.debug("PostgreSQL started, initializing schema.")

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute(SCHEMA_SQL)
    conn.close()
    logger.debug("PostgreSQL schema initialized.")

    yield dsn

    container.stop()
    logger.debug("PostgreSQL container stopped.")


@pytest.fixture(scope="session")
def kafka_container() -> Generator[str, None, None]:
    """Kafka container with embedded Zookeeper."""
    logger.debug("Starting Kafka container.")
    container = KafkaContainer()

    container.start()
    bootstrap_server = container.get_bootstrap_server()
    logger.debug("Kafka started at %s.", bootstrap_server)

    yield bootstrap_server

    container.stop()
    logger.debug("Kafka container stopped.")


@pytest.fixture(scope="session")
def localstack_container() -> Generator[dict, None, None]:
    """LocalStack container for EventBridge and Secrets Manager."""
    logger.debug("Starting LocalStack container.")
    container = LocalStackContainer("localstack/localstack:latest")
    container.with_services("events,secretsmanager")

    container.start()
    url = container.get_url()
    logger.debug("LocalStack started at %s.", url)

    yield {"url": url, "region": "us-east-1"}

    container.stop()
    logger.debug("LocalStack container stopped.")


@pytest.fixture(scope="session")
def jwt_keypair() -> Dict[str, Any]:
    """Generate RSA keypair for JWT signing."""
    return create_test_jwt_keypair()


@pytest.fixture(scope="session")
def mock_jwt_server(jwt_keypair: Dict[str, Any]) -> Generator[str, None, None]:
    """In-process mock JWT provider server."""
    server = _start_mock_jwt_server(
        0,
        jwt_keypair["private_key_pem"],
        jwt_keypair["public_key_b64"],
    )
    port = server.server_address[1]
    url = f"http://127.0.0.1:{port}"

    # Wait for server to be ready.
    for _ in range(10):
        try:
            req_lib.get(f"{url}/health", timeout=1)
            break
        except (ConnectionError, OSError):
            time.sleep(0.1)
    else:
        pytest.fail("Mock JWT server failed to start within 1 second.")

    yield url

    server.shutdown()
    logger.debug("Mock JWT server stopped.")


# Lambda handler fixture
@pytest.fixture(scope="session")
def lambda_handler_factory(
    kafka_container: str,
    postgres_container: str,
    localstack_container: dict,
    mock_jwt_server: str,
) -> Generator[Callable[[Dict[str, Any]], Dict[str, Any]], None, None]:
    """Create lambda_handler with real container backends."""
    # Set environment variables for the Lambda.
    os.environ["LOG_LEVEL"] = "DEBUG"
    os.environ["AWS_ENDPOINT_URL"] = localstack_container["url"]
    os.environ["AWS_DEFAULT_REGION"] = localstack_container["region"]
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

    # Store PostgreSQL credentials in LocalStack Secrets Manager so WriterPostgres can find them.
    parsed_dsn = urlparse(postgres_container)
    pg_secret = {
        "database": parsed_dsn.path.lstrip("/"),
        "host": parsed_dsn.hostname,
        "port": parsed_dsn.port,
        "user": parsed_dsn.username,
        "password": parsed_dsn.password,
    }
    sm_client = boto3.client(
        "secretsmanager",
        endpoint_url=localstack_container["url"],
        region_name=localstack_container["region"],
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    sm_client.create_secret(Name="eventgate/postgres", SecretString=json.dumps(pg_secret))
    os.environ["POSTGRES_SECRET_NAME"] = "eventgate/postgres"
    os.environ["POSTGRES_SECRET_REGION"] = localstack_container["region"]
    logger.debug("PostgreSQL secret stored in LocalStack Secrets Manager.")

    # Create test config with container URLs.
    test_config_dir = PROJECT_ROOT / "tests" / "integration" / ".tmp_conf"
    test_config_dir.mkdir(parents=True, exist_ok=True)

    # Copy access.json to test config dir.
    access_config_src = PROJECT_ROOT / "conf" / "access.json"
    access_config_dst = test_config_dir / "access.json"
    access_config_dst.write_text(access_config_src.read_text(encoding="utf-8"), encoding="utf-8")

    # Copy topic_schemas to test config dir.
    topic_schemas_src = PROJECT_ROOT / "conf" / "topic_schemas"
    topic_schemas_dst = test_config_dir / "topic_schemas"
    if topic_schemas_dst.exists():
        shutil.rmtree(topic_schemas_dst)
    shutil.copytree(topic_schemas_src, topic_schemas_dst)

    test_config = {
        "access_config": str(access_config_dst),
        "token_provider_url": mock_jwt_server,
        "token_public_keys_url": f"{mock_jwt_server}/keys",
        "kafka_bootstrap_server": kafka_container,
        "event_bus_arn": "arn:aws:events:us-east-1:000000000000:event-bus/default",
    }

    test_config_file = test_config_dir / "config.json"
    test_config_file.write_text(json.dumps(test_config, indent=2), encoding="utf-8")

    # Point CONF_DIR to our test config.
    os.environ["CONF_DIR"] = str(test_config_dir)

    logger.debug("Test config written to %s.", test_config_file)

    # Import the lambda handler (this triggers initialization).
    from src.event_gate_lambda import lambda_handler

    yield lambda_handler

    # Cleanup environment variables.
    for key in (
        "LOG_LEVEL",
        "AWS_ENDPOINT_URL",
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "POSTGRES_SECRET_NAME",
        "POSTGRES_SECRET_REGION",
        "CONF_DIR",
    ):
        os.environ.pop(key, None)
    if test_config_file.exists():
        test_config_file.unlink()
    if access_config_dst.exists():
        access_config_dst.unlink()
    topic_schemas_dst = test_config_dir / "topic_schemas"
    if topic_schemas_dst.exists():
        shutil.rmtree(topic_schemas_dst)
    if test_config_dir.exists() and not any(test_config_dir.iterdir()):
        test_config_dir.rmdir()


@pytest.fixture(scope="session")
def eventgate_client(
    lambda_handler_factory: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> "EventGateTestClient":
    """EventGate test client that invokes lambda_handler directly."""
    return EventGateTestClient(lambda_handler_factory)


@pytest.fixture(scope="session")
def stats_lambda_handler(
    lambda_handler_factory: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """Import stats Lambda after env is set up by lambda_handler_factory.

    The main handler factory must run first to set env vars, create
    config files, and store Secrets Manager entries.  We access
    ``lambda_handler_factory`` to trigger that setup, then import the
    stats Lambda module.
    """
    # lambda_handler_factory has already set up the env; import stats Lambda.
    from src.event_stats_lambda import lambda_handler as stats_handler

    return stats_handler


@pytest.fixture(scope="session")
def stats_client(
    stats_lambda_handler: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> "EventStatsTestClient":
    """EventStats test client that invokes stats lambda_handler directly."""
    return EventStatsTestClient(stats_lambda_handler)


@pytest.fixture(scope="session")
def valid_token(jwt_keypair: Dict[str, Any]) -> str:
    """Valid JWT token for IntegrationTestUser."""
    return generate_token(jwt_keypair["private_key_pem"], "IntegrationTestUser")


class LambdaTestClient:
    """Base test client that invokes a lambda_handler directly."""

    _label: str = "Lambda"

    def __init__(self, handler: Callable[[Dict[str, Any]], Dict[str, Any]]):
        """Initialize with lambda handler function."""
        self._handler = handler

    def invoke(
        self,
        resource: str,
        method: str = "GET",
        body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        path_parameters: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Invoke lambda_handler with API Gateway proxy event format."""
        event = {
            "resource": resource,
            "httpMethod": method,
            "headers": headers or {},
            "body": json.dumps(body) if body else None,
            "pathParameters": path_parameters,
        }
        logger.debug("Invoking %s: %s %s.", self._label, method, resource)
        result = self._handler(event)
        logger.debug("%s response: statusCode=%s.", self._label, result.get("statusCode"))
        return result


class EventGateTestClient(LambdaTestClient):
    """Test client for the EventGate write Lambda."""

    _label = "EventGate"

    def get_api(self) -> Dict[str, Any]:
        """Get OpenAPI specification."""
        return self.invoke("/api", "GET")

    def get_token(self) -> Dict[str, Any]:
        """Get token provider info."""
        return self.invoke("/token", "GET")

    def get_health(self) -> Dict[str, Any]:
        """Get health status."""
        return self.invoke("/health", "GET")

    def get_topics(self) -> Dict[str, Any]:
        """Get list of topics."""
        return self.invoke("/topics", "GET")

    def get_topic_schema(self, topic_name: str) -> Dict[str, Any]:
        """Get schema for a specific topic."""
        return self.invoke(
            "/topics/{topic_name}",
            "GET",
            path_parameters={"topic_name": topic_name},
        )

    def post_event(
        self,
        topic_name: str,
        event_data: Dict[str, Any],
        token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Post an event to a topic."""
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return self.invoke(
            "/topics/{topic_name}",
            "POST",
            body=event_data,
            headers=headers,
            path_parameters={"topic_name": topic_name},
        )


class EventStatsTestClient(LambdaTestClient):
    """Test client for the EventStats read Lambda."""

    _label = "EventStats"

    def get_health(self) -> Dict[str, Any]:
        """Get stats service health status."""
        return self.invoke("/health", "GET")

    def post_stats(
        self,
        topic_name: str,
        body: Dict[str, Any],
        token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Query stats for a topic."""
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return self.invoke(
            "/stats/{topic_name}",
            "POST",
            body=body,
            headers=headers,
            path_parameters={"topic_name": topic_name},
        )
