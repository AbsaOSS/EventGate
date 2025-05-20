import json
import os

import psycopg2

def init(logger):
    global _logger
    global POSTGRES
    
    _logger = logger
    
    POSTGRES = {
    "host": os.environ.get("POSTGRES_HOST", ""),
    "port": os.environ.get("POSTGRES_PORT", ""),
    "user": os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "database": os.environ.get("POSTGRES_DATABASE", "")
    }
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
            message["location"] if "location" in message else None,
            message["format"],
            json.dumps(message["format_options"]) if "format_options" in message else None,
            json.dumps(message["additional_info"]) if "additional_info" in message else None
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
                soure_app,
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
            job["message"] if "message" in job else None,
            json.dumps(job["additional_info"]) if "additional_info" in job else None
        )
    )
    
def write(topicName, message):
    try:
        if not POSTGRES["database"]:
            _logger.debug("No Postgress - skipping")
            return
            
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
                else:
                    _logger.error(f"unknown topic for postgres {topicName}")
                    return False
                    
            connection.commit()
    except Exception as e:
        _logger.error(str(e))
        return False
        
    return True
