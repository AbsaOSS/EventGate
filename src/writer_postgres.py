import json
import os

import boto3
from botocore.exceptions import ClientError
try:
    import psycopg2  # noqa: F401
except ImportError:  # pragma: no cover - environment without psycopg2
    psycopg2 = None

def init(logger):
    global _logger
    global POSTGRES
    
    _logger = logger
    
    secret_name = os.environ.get("POSTGRES_SECRET_NAME", "")
    secret_region = os.environ.get("POSTGRES_SECRET_REGION", "")
    
    if secret_name and secret_region:
        aws_secrets = boto3.Session().client(service_name='secretsmanager', region_name=secret_region)
        postgres_secret = aws_secrets.get_secret_value(SecretId=secret_name)["SecretString"]
        POSTGRES = json.loads(postgres_secret)
    else:
        POSTGRES = {"database": ""}

    _logger.debug("Initialized POSTGRES writer")

def postgres_edla_write(cursor, table, message):
    _logger.debug(f"Sending to Postgres - {table}")
    cursor.execute(f"""
        INSERT INTO {table} 
        (
            event_id, 
            tenant_id, 
            source_app, 
            source_app_version, 
            environment, 
            timestamp_event, 
            catalog_id, 
            operation, 
            "location", 
            "format", 
            format_options, 
            additional_info
        ) 
        VALUES
        (
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s,
            %s, 
            %s, 
            %s
        )""", (
            message["event_id"],
            message["tenant_id"],
            message["source_app"],
            message["source_app_version"],
            message["environment"],
            message["timestamp_event"],
            message["catalog_id"],
            message["operation"],
            message.get("location"),
            message["format"],
            json.dumps(message.get("format_options")) if "format_options" in message else None,
            json.dumps(message.get("additional_info")) if "additional_info" in message else None
        )
    )

def postgres_run_write(cursor, table_runs, table_jobs, message):
    _logger.debug(f"Sending to Postgres - {table_runs} and {table_jobs}")
    cursor.execute(f"""
        INSERT INTO {table_runs} 
        (
                event_id,
                job_ref,
                tenant_id,
                source_app,
                source_app_version,
                environment,
                timestamp_start,
                timestamp_end
        ) 
        VALUES
        (
            %s, 
            %s, 
            %s, 
            %s, 
            %s,
            %s, 
            %s, 
            %s
        )""", (
            message["event_id"],
            message["job_ref"],
            message["tenant_id"],
            message["source_app"],
            message["source_app_version"],
            message["environment"],
            message["timestamp_start"],
            message["timestamp_end"]
        )
    )
        
    for job in message["jobs"]:
        cursor.execute(f"""
        INSERT INTO {table_jobs} 
        (
                event_id,
                catalog_id,
                status,
                timestamp_start,
                timestamp_end,
                message,
                additional_info
        ) 
        VALUES
        (
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s, 
            %s
        )""", (
            message["event_id"],
            job["catalog_id"],
            job["status"],
            job["timestamp_start"],
            job["timestamp_end"],
            job.get("message"),
            json.dumps(job.get("additional_info")) if "additional_info" in job else None
        )
    )

def postgres_test_write(cursor, table, message):
    _logger.debug(f"Sending to Postgres - {table}")
    cursor.execute(f"""
        INSERT INTO {table} 
        (
            event_id,
            tenant_id,
            source_app, 
            environment, 
            timestamp_event, 
            additional_info
        ) 
        VALUES
        (
            %s,
            %s,
            %s, 
            %s, 
            %s, 
            %s
        )""", (
            message["event_id"],
            message["tenant_id"],
            message["source_app"],
            message["environment"],
            message["timestamp"],
            json.dumps(message.get("additional_info")) if "additional_info" in message else None
        )
    )

def write(topicName, message):
    try:
        if not POSTGRES["database"]:
            _logger.debug("No Postgres - skipping")
            return True, None
        if psycopg2 is None:
            _logger.debug("psycopg2 not available - skipping actual Postgres write")
            return True, None
            
        with psycopg2.connect(
            database=POSTGRES["database"],
            host=POSTGRES["host"],
            user=POSTGRES["user"],
            password=POSTGRES["password"],
            port=POSTGRES["port"]
        ) as connection:
            with connection.cursor() as cursor:
                if topicName == "public.cps.za.dlchange":
                    postgres_edla_write(cursor, "public_cps_za_dlchange", message)
                elif topicName == "public.cps.za.runs":
                    postgres_run_write(cursor, "public_cps_za_runs", "public_cps_za_runs_jobs", message)
                elif topicName == "public.cps.za.test":
                    postgres_test_write(cursor, "public_cps_za_test", message)
                else:
                    msg = f"unknown topic for postgres {topicName}"
                    _logger.error(msg)
                    return False, msg
                    
            connection.commit()
    except Exception as e:
        err_msg = f'The Postgres writer with failed unknown error: {str(e)}'
        _logger.error(err_msg)
        return False, err_msg
        
    return True, None
