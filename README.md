# EventGate

Python AWS Lambda that exposes a simple HTTP API (via API Gateway) for validating and forwarding well-defined JSON messages to multiple backends (Kafka, EventBridge, Postgres). Designed for centralized, schema-governed event ingestion with pluggable writers.

> Status: Internal prototype / early version

<!-- toc -->
- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [API](#api)
- [Configuration](#configuration)
- [Deployment](#deployment)
  - [Zip Lambda Package](#zip-lambda-package)
  - [Container Image Lambda](#container-image-lambda)
- [Local Development & Testing](#local-development--testing)
- [Security & Authorization](#security--authorization)
- [Writers](#writers)
  - [Kafka Writer](#kafka-writer)
  - [EventBridge Writer](#eventbridge-writer)
  - [Postgres Writer](#postgres-writer)
- [Scripts](#scripts)
- [Troubleshooting](#troubleshooting)
- [License](#license)
<!-- tocstop -->

## Overview
EventGate receives JSON payloads for registered topics, authorizes the caller via JWT, validates the payload against a JSON Schema, and then forwards the payload to one or more configured sinks (Kafka, EventBridge, Postgres). Schemas and access control are externally configurable (local file or S3) to allow runtime evolution without code changes.

## Features
- Topic registry with per-topic JSON Schema validation
- Multiple parallel writers (Kafka / EventBridge / Postgres) — failure in one does not block the others; aggregated error reporting
- JWT-based per-topic authorization (RS256 public key fetched remotely)
- Runtime-configurable access rules (local or S3)
- API-discoverable schema catalogue
- Pluggable writer initialization via `config.json`
- Terraform IaC examples for AWS deployment (API Gateway + Lambda)
- Supports both Zip-based and Container Image Lambda packaging (Container path enables custom `librdkafka` / SASL_SSL / Kerberos builds)

## Architecture
High-level flow:
1. Client requests a JWT from an external token provider (link exposed via `/token`).
2. Client submits `POST /topics/{topicName}` with `Authorization: Bearer <JWT>` header and JSON body.
3. Lambda resolves topic schema, validates payload, authorizes subject (`sub`) against access map.
4. Writers invoked (Kafka, EventBridge, Postgres). Each returns success/failure.
5. Aggregated response returned: `202 Accepted` if all succeed; `500` with per-writer error list otherwise.

Key files:
- `src/event_gate_lambda.py` – main Lambda handler and routing
- `conf/*.json` – configuration and topic schemas
- `conf/api.yaml` – OpenAPI 3 definition served at `/api`
- `writer_*.py` – individual sink implementations

## API
All responses are JSON unless otherwise noted. The POST endpoint requires a valid JWT.

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api` | none | Returns OpenAPI 3 definition (raw YAML) |
| GET | `/token` | none | 303 redirect to external token provider |
| GET | `/topics` | none | Lists available topic names |
| GET | `/topics/{topicName}` | none | Returns JSON Schema for the topic |
| POST | `/topics/{topicName}` | JWT | Validates + forwards message to configured sinks |
| POST | `/terminate` | (internal) | Forces Lambda process exit (used to trigger cold start & config reload) |

Status codes:
- 202 – Accepted (all writers succeeded)
- 400 – Schema validation failure
- 401 – Token missing/invalid
- 403 – Subject unauthorized for topic
- 404 – Unknown topic or route
- 500 – One or more writers failed / internal error

## Configuration
All core runtime configuration is driven by JSON files located in `conf/` unless S3 paths are specified.

Primary file: `conf/config.json`
Example (sanitized):
```json
{
  "access_config": "s3://<bucket>/access.json",
  "token_provider_url": "https://<token-ui.example>",
  "token_public_key_url": "https://<token-api.example>/public-key",
  "kafka_bootstrap_server": "broker1:9092,broker2:9092",
  "event_bus_arn": "arn:aws:events:region:acct:event-bus/your-bus"
}
```
Supporting configs:
- `access.json` – map: topicName -> array of authorized subjects (JWT `sub`). May reside locally or at S3 path referenced by `access_config`.
- `topic_*.json` – each file contains a JSON Schema for a topic. In the current code these are explicitly loaded inside `event_gate_lambda.py`. (Future enhancement: auto-discover or index file.)
- `api.yaml` – OpenAPI spec served verbatim at runtime.

Environment variables:
- `LOG_LEVEL` (optional) – defaults to `INFO`.

## Deployment
Infrastructure-as-Code examples live in the `terraform/` directory. Variables are supplied via a `*.tfvars` file or CLI.

### Zip Lambda Package
Use when no custom native libraries are needed.
1. Run packaging script: `scripts/prepare.deplyoment.sh` (downloads deps + zips sources & config)
2. Upload resulting zip to S3
3. Provide Terraform variables:
   - `aws_region`
   - `vpc_id`
   - `vpc_endpoint`
   - `resource_prefix` (prepended to created resource names)
   - `lambda_role_arn`
   - `lambda_vpc_subnet_ids`
   - `lambda_package_type = "Zip"`
   - `lambda_src_s3_bucket`
   - `lambda_src_s3_key`
4. `terraform apply`

### Container Image Lambda
Use when Kafka access needs Kerberos / SASL_SSL or custom `librdkafka` build.
1. Build image (see comments at top of `Dockerfile`)
2. Push to ECR
3. Terraform variables:
   - Same networking / role vars as above
   - `lambda_package_type = "Image"`
   - `lambda_src_ecr_image` (ECR image reference)
4. `terraform apply`

## Local Development & Testing
Install Python tooling (Python 3.11 suggested) then:
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
Run full test suite (quiet):
```
pytest -q
```
Run with coverage (terminal + per-file missing lines) enforcing 80% minimum:
```
pytest --cov=. --cov-report=term-missing --cov-fail-under=80
```
Run a single test file or filtered tests:
```
pytest tests/test_writer_kafka.py -q
pytest -k kafka -q
```
Type checking & lint:
```
mypy .
pylint $(git ls-files '*.py')
```
Format:
```
black $(git ls-files '*.py')
```
Key environment variables for local runs / tests:
- LOG_LEVEL: Override logging level (default INFO)
- KAFKA_FLUSH_TIMEOUT: Seconds for Kafka producer.flush(timeout) (default 5)

Kafka tests stub the real producer; EventBridge & S3 interactions are patched to avoid network calls. Tests that exercise module import side-effects (like loading config from S3) first patch boto3 / requests before importing the module to ensure deterministic behavior.

To simulate local (non-S3) access config branch, see tests/test_event_gate_lambda_local_access.py which reloads the module with patched open().

To manually invoke the handler:
```python
from src import event_gate_lambda as m
resp = m.lambda_handler({"resource": "/topics"}, None)
print(resp)
```

## Security & Authorization
- JWT tokens must be RS256 signed; the public key is fetched at cold start from `token_public_key_url` (DER base64 inside JSON `{ "key": "..." }`).
- Subject claim (`sub`) is matched against `ACCESS[topicName]`.
- Authorization header forms accepted:
  - `Authorization: Bearer <token>` (preferred)
  - Legacy: `bearer: <token>` custom header
- No token introspection beyond signature & standard claim extraction.

## Writers
Each writer is initialized during cold start. Failures are isolated; aggregated errors returned in a single `500` response if any writer fails.

### Kafka Writer
Configured via `kafka_bootstrap_server`. (Future: support auth properties / TLS configuration.)

### EventBridge Writer
Publishes events to the configured `event_bus_arn` using put events API.

### Postgres Writer
Example writer (currently a placeholder if no DSN present) demonstrating extensibility pattern.

## Scripts
- `scripts/prepare.deplyoment.sh` – build Zip artifact for Lambda (typo in name retained for now; may rename later)
- `scripts/notebook.ipynb` – exploratory invocation cells per endpoint
- `scripts/get_token.http` – sample HTTP request for tooling (e.g., VSCode REST client)

## Troubleshooting
| Symptom | Possible Cause | Action |
|---------|----------------|--------|
| 401 Unauthorized | Missing / malformed token header | Ensure `Authorization: Bearer` present |
| 403 Forbidden | Subject not listed in access map | Update `access.json` and redeploy / reload |
| 404 Topic not found | Wrong casing or not loaded in code | Verify loaded topics & file names |
| 500 Writer failure | Downstream (Kafka / EventBridge / DB) unreachable | Check network / VPC endpoints / security groups |
| Lambda keeps old config | Warm container | Call `/terminate` (internal) to force cold start |

## License
Licensed under the Apache License, Version 2.0. See the [LICENSE](./LICENSE) file for full text.

Copyright 2025 ABSA Group Limited.

You may not use this project except in compliance with the License. Unless required by law or agreed in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied.

---
Contributions & enhancements welcome (subject to project guidelines).
