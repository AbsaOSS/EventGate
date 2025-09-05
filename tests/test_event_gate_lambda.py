import base64
import json
import unittest
import importlib
import sys
import types
from unittest.mock import patch, MagicMock

# Inject dummy confluent_kafka if not installed so patching works
if 'confluent_kafka' not in sys.modules:
    dummy_ck = types.ModuleType('confluent_kafka')
    class DummyProducer:  # minimal interface
        def __init__(self, *a, **kw):
            pass
        def produce(self, *a, **kw):
            cb = kw.get('callback')
            if cb:
                cb(None, None)
        def flush(self):
            return None
    dummy_ck.Producer = DummyProducer
    sys.modules['confluent_kafka'] = dummy_ck

# Inject dummy psycopg2 (not needed but prevents optional import noise)
if 'psycopg2' not in sys.modules:
    dummy_pg = types.ModuleType('psycopg2')
    sys.modules['psycopg2'] = dummy_pg

def load_event_gate_module():
    # Start patches before import to intercept side effects
    mock_requests_get = patch('requests.get').start()
    mock_requests_get.return_value.json.return_value = {"key": base64.b64encode(b'dummy_der').decode('utf-8')}

    mock_load_key = patch('cryptography.hazmat.primitives.serialization.load_der_public_key').start()
    mock_load_key.return_value = object()

    # Mock S3 access_config retrieval
    class MockS3ObjectBody:
        def read(self):
            return json.dumps({
                "public.cps.za.runs": ["FooBarUser"],
                "public.cps.za.dlchange": ["FooUser", "BarUser"],
                "public.cps.za.test": ["TestUser"]
            }).encode('utf-8')
    class MockS3Object:
        def get(self):
            return {"Body": MockS3ObjectBody()}
    class MockS3Bucket:
        def Object(self, key):
            return MockS3Object()
    class MockS3Resource:
        def Bucket(self, name):
            return MockS3Bucket()
    mock_session = patch('boto3.Session').start()
    mock_session.return_value.resource.return_value = MockS3Resource()

    # Mock EventBridge client
    mock_boto_client = patch('boto3.client').start()
    mock_events_client = MagicMock()
    mock_events_client.put_events.return_value = {"FailedEntryCount": 0}
    mock_boto_client.return_value = mock_events_client

    # Allow kafka producer patching (already stubbed) but still patch to inspect if needed
    patch('confluent_kafka.Producer').start()

    module = importlib.import_module('src.event_gate_lambda')
    return module

class TestEventGateLambda(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.event_gate_lambda = load_event_gate_module()

    @classmethod
    def tearDownClass(cls):
        patch.stopall()

    # Helper to construct API Gateway like event
    def make_event(self, resource, method='GET', body=None, topic=None, headers=None):
        return {
            'resource': resource,
            'httpMethod': method,
            'headers': headers or {},
            'pathParameters': {'topic_name': topic} if topic else {},
            'body': json.dumps(body) if isinstance(body, dict) else body
        }

    def valid_payload(self):
        return {"event_id":"e1","tenant_id":"t1","source_app":"app","environment":"dev","timestamp":123}

    # --- GET flows ---
    def test_get_topics(self):
        event = self.make_event('/topics')
        resp = self.event_gate_lambda.lambda_handler(event, None)
        self.assertEqual(resp['statusCode'], 200)
        body = json.loads(resp['body'])
        self.assertIn('public.cps.za.test', body)

    def test_get_topic_schema_found(self):
        event = self.make_event('/topics/{topic_name}', method='GET', topic='public.cps.za.test')
        resp = self.event_gate_lambda.lambda_handler(event, None)
        self.assertEqual(resp['statusCode'], 200)
        schema = json.loads(resp['body'])
        self.assertEqual(schema['type'], 'object')

    def test_get_topic_schema_not_found(self):
        event = self.make_event('/topics/{topic_name}', method='GET', topic='no.such.topic')
        resp = self.event_gate_lambda.lambda_handler(event, None)
        self.assertEqual(resp['statusCode'], 404)

    # --- POST auth / validation failures ---
    def test_post_missing_token(self):
        payload = self.valid_payload()
        event = self.make_event('/topics/{topic_name}', method='POST', topic='public.cps.za.test', body=payload, headers={})
        resp = self.event_gate_lambda.lambda_handler(event, None)
        self.assertEqual(resp['statusCode'], 401)
        body = json.loads(resp['body'])
        self.assertFalse(body['success'])
        self.assertEqual(body['errors'][0]['type'], 'auth')

    def test_post_unauthorized_user(self):
        payload = self.valid_payload()
        with patch.object(self.event_gate_lambda.jwt, 'decode', return_value={'sub': 'NotAllowed'}, create=True):
            event = self.make_event('/topics/{topic_name}', method='POST', topic='public.cps.za.test', body=payload, headers={'Authorization':'Bearer token'})
            resp = self.event_gate_lambda.lambda_handler(event, None)
            self.assertEqual(resp['statusCode'], 403)
            body = json.loads(resp['body'])
            self.assertEqual(body['errors'][0]['type'], 'auth')

    def test_post_schema_validation_error(self):
        payload = {"event_id":"e1","tenant_id":"t1","source_app":"app","environment":"dev"}  # missing timestamp
        with patch.object(self.event_gate_lambda.jwt, 'decode', return_value={'sub': 'TestUser'}, create=True):
            event = self.make_event('/topics/{topic_name}', method='POST', topic='public.cps.za.test', body=payload, headers={'Authorization':'Bearer token'})
            resp = self.event_gate_lambda.lambda_handler(event, None)
            self.assertEqual(resp['statusCode'], 400)
            body = json.loads(resp['body'])
            self.assertEqual(body['errors'][0]['type'], 'validation')

    # --- POST success & failure aggregation ---
    def test_post_success_all_writers(self):
        payload = self.valid_payload()
        with patch.object(self.event_gate_lambda.jwt, 'decode', return_value={'sub': 'TestUser'}, create=True), \
             patch('src.event_gate_lambda.writer_kafka.write', return_value=(True, None)), \
             patch('src.event_gate_lambda.writer_eventbridge.write', return_value=(True, None)), \
             patch('src.event_gate_lambda.writer_postgres.write', return_value=(True, None)):
            event = self.make_event('/topics/{topic_name}', method='POST', topic='public.cps.za.test', body=payload, headers={'Authorization':'Bearer token'})
            resp = self.event_gate_lambda.lambda_handler(event, None)
            self.assertEqual(resp['statusCode'], 202)
            body = json.loads(resp['body'])
            self.assertTrue(body['success'])
            self.assertEqual(body['statusCode'], 202)

    def test_post_single_writer_failure(self):
        payload = self.valid_payload()
        with patch.object(self.event_gate_lambda.jwt, 'decode', return_value={'sub': 'TestUser'}, create=True), \
             patch('src.event_gate_lambda.writer_kafka.write', return_value=(False, 'Kafka boom')), \
             patch('src.event_gate_lambda.writer_eventbridge.write', return_value=(True, None)), \
             patch('src.event_gate_lambda.writer_postgres.write', return_value=(True, None)):
            event = self.make_event('/topics/{topic_name}', method='POST', topic='public.cps.za.test', body=payload, headers={'Authorization':'Bearer token'})
            resp = self.event_gate_lambda.lambda_handler(event, None)
            self.assertEqual(resp['statusCode'], 500)
            body = json.loads(resp['body'])
            self.assertFalse(body['success'])
            self.assertEqual(len(body['errors']), 1)
            self.assertEqual(body['errors'][0]['type'], 'kafka')

    def test_post_multiple_writer_failures(self):
        payload = self.valid_payload()
        with patch.object(self.event_gate_lambda.jwt, 'decode', return_value={'sub': 'TestUser'}, create=True), \
             patch('src.event_gate_lambda.writer_kafka.write', return_value=(False, 'Kafka A')), \
             patch('src.event_gate_lambda.writer_eventbridge.write', return_value=(False, 'EB B')), \
             patch('src.event_gate_lambda.writer_postgres.write', return_value=(True, None)):
            event = self.make_event('/topics/{topic_name}', method='POST', topic='public.cps.za.test', body=payload, headers={'Authorization':'Bearer token'})
            resp = self.event_gate_lambda.lambda_handler(event, None)
            self.assertEqual(resp['statusCode'], 500)
            body = json.loads(resp['body'])
            self.assertEqual(len(body['errors']), 2)
            types = sorted(e['type'] for e in body['errors'])
            self.assertEqual(types, ['eventbridge', 'kafka'])

    def test_token_extraction_lowercase_bearer_header(self):
        payload = self.valid_payload()
        with patch.object(self.event_gate_lambda.jwt, 'decode', return_value={'sub': 'TestUser'}, create=True), \
             patch('src.event_gate_lambda.writer_kafka.write', return_value=(True, None)), \
             patch('src.event_gate_lambda.writer_eventbridge.write', return_value=(True, None)), \
             patch('src.event_gate_lambda.writer_postgres.write', return_value=(True, None)):
            event = self.make_event('/topics/{topic_name}', method='POST', topic='public.cps.za.test', body=payload, headers={'bearer':'token'})
            resp = self.event_gate_lambda.lambda_handler(event, None)
            self.assertEqual(resp['statusCode'], 202)

    def test_unknown_resource(self):
        event = self.make_event('/unknown')
        resp = self.event_gate_lambda.lambda_handler(event, None)
        self.assertEqual(resp['statusCode'], 404)
        body = json.loads(resp['body'])
        self.assertEqual(body['errors'][0]['type'], 'route')

    def test_get_api_endpoint(self):
        event = self.make_event('/api')
        resp = self.event_gate_lambda.lambda_handler(event, None)
        self.assertEqual(resp['statusCode'], 200)
        self.assertIn('openapi', resp['body'].lower())  # crude check

    def test_get_token_endpoint(self):
        event = self.make_event('/token')
        resp = self.event_gate_lambda.lambda_handler(event, None)
        self.assertEqual(resp['statusCode'], 303)
        self.assertIn('Location', resp['headers'])

    def test_internal_error_path(self):
        # Force exception by patching get_topics to raise
        with patch('src.event_gate_lambda.get_topics', side_effect=RuntimeError('boom')):
            event = self.make_event('/topics')
            resp = self.event_gate_lambda.lambda_handler(event, None)
            self.assertEqual(resp['statusCode'], 500)
            body = json.loads(resp['body'])
            self.assertEqual(body['errors'][0]['type'], 'internal')

if __name__ == '__main__':
    unittest.main()
