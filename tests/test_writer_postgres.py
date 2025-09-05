import json
import logging
import unittest

from src import writer_postgres


class MockCursor:
    def __init__(self):
        self.executions = []
    def execute(self, sql, params):
        self.executions.append((sql.strip(), params))

class TestWriterPostgres(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize logger and module (will skip DB because no env vars)
        writer_postgres.init(logging.getLogger("test"))

    def test_postgres_edla_write_with_optional_fields(self):
        cur = MockCursor()
        message = {
            "event_id": "e1",
            "tenant_id": "t1",
            "source_app": "app",
            "source_app_version": "1.0.0",
            "environment": "dev",
            "timestamp_event": 111,
            "catalog_id": "db.tbl",
            "operation": "append",
            "location": "s3://bucket/path",
            "format": "parquet",
            "format_options": {"compression": "snappy"},
            "additional_info": {"foo": "bar"}
        }
        writer_postgres.postgres_edla_write(cur, "table_a", message)
        self.assertEqual(len(cur.executions), 1)
        sql, params = cur.executions[0]
        # Ensure we inserted 12 params per columns list
        self.assertEqual(len(params), 12)
        self.assertEqual(params[0], "e1")
        self.assertEqual(params[8], "s3://bucket/path")  # location
        self.assertEqual(params[9], "parquet")
        self.assertEqual(json.loads(params[10]), {"compression": "snappy"})
        self.assertEqual(json.loads(params[11]), {"foo": "bar"})

    def test_postgres_edla_write_missing_optional(self):
        cur = MockCursor()
        message = {
            "event_id": "e2",
            "tenant_id": "t2",
            "source_app": "app",
            "source_app_version": "1.0.1",
            "environment": "dev",
            "timestamp_event": 222,
            "catalog_id": "db.tbl2",
            "operation": "overwrite",
            "format": "delta"
        }
        writer_postgres.postgres_edla_write(cur, "table_a", message)
        sql, params = cur.executions[0]
        # location, format_options, additional_info -> None
        self.assertIsNone(params[8])
        self.assertEqual(params[9], "delta")
        self.assertIsNone(params[10])
        self.assertIsNone(params[11])

    def test_postgres_run_write(self):
        cur = MockCursor()
        message = {
            "event_id": "r1",
            "job_ref": "job-123",
            "tenant_id": "ten",
            "source_app": "runapp",
            "source_app_version": "2.0.0",
            "environment": "dev",
            "timestamp_start": 1000,
            "timestamp_end": 2000,
            "jobs": [
                {"catalog_id": "c1", "status": "succeeded", "timestamp_start": 1100, "timestamp_end": 1200},
                {"catalog_id": "c2", "status": "failed", "timestamp_start": 1300, "timestamp_end": 1400, "message": "err", "additional_info": {"k": "v"}}
            ]
        }
        writer_postgres.postgres_run_write(cur, "runs_table", "jobs_table", message)
        # 1 insert for run + 2 inserts for jobs
        self.assertEqual(len(cur.executions), 3)
        run_sql, run_params = cur.executions[0]
        self.assertIn("source_app_version", run_sql)  # ensure fixed column name present implicitly
        self.assertEqual(run_params[3], "runapp")  # source_app param
        job2_sql, job2_params = cur.executions[2]
        self.assertEqual(job2_params[5], "err")
        self.assertEqual(json.loads(job2_params[6]), {"k": "v"})

    def test_postgres_test_write(self):
        cur = MockCursor()
        message = {
            "event_id": "t1",
            "tenant_id": "tenant-x",
            "source_app": "test",
            "environment": "dev",
            "timestamp": 999,
            "additional_info": {"a": 1}
        }
        writer_postgres.postgres_test_write(cur, "table_test", message)
        self.assertEqual(len(cur.executions), 1)
        sql, params = cur.executions[0]
        self.assertEqual(params[0], "t1")
        self.assertEqual(params[1], "tenant-x")
        self.assertEqual(json.loads(params[5]), {"a": 1})

if __name__ == '__main__':
    unittest.main()
