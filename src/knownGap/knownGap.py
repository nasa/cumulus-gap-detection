import boto3
import json
import os
import logging
from typing import Dict, Any
from psycopg import errors as psycopgErrors
from aws_lambda_typing import context as Context, events
import traceback
from utils import get_db_connection, validate_environment_variables, sanitize_versionid, check_gap_config
from dateutil.parser import parse as parse_datetime
import jsonschema
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder to handle datetime objects"""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def build_response(status_code: int, body: any) -> dict[str, any]:
    return {
        "statusCode": status_code,
        "body": json.dumps(body, cls=DateTimeEncoder),
    }

def parse_event(event):
    cid = start = end = reason = ""
    params = event.get("queryStringParameters", {}) or {}
    collection_name = params.get("short_name")
    collection_version = params.get("version")
    start = params.get("startDate")
    end = params.get("endDate")

    if collection_name and collection_version:
        collection_version = collection_version.replace(".", "_")
        cid = f"{collection_name}___{collection_version}"
    else:
        raise Exception("Error: `short_name` and `version` are required")
    return cid, start, end, reason

def get_known_gaps(cid, start, end, conn):
    """
    Gets all gaps that fall within the specified time range for a collection.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH reason_ranges AS (
                SELECT collection_id, start_ts, end_ts, reason
                FROM reasons 
                WHERE collection_id = %s
                AND tsrange(start_ts, end_ts) && tsrange(%s, %s)
            )
            SELECT g.gap_id, g.collection_id, g.start_ts, g.end_ts, r.reason
            FROM gaps g
            JOIN reason_ranges r ON (
                g.collection_id = r.collection_id 
                AND tsrange(g.start_ts, g.end_ts) && tsrange(r.start_ts, r.end_ts)
            )
            ORDER BY g.start_ts
        """,
            (cid, start, end),
        )


        columns = ["gap_id", "collection_id", "start_ts", "end_ts", "reason"]
        gaps = [dict(zip(columns, row)) for row in cur.fetchall()]

        return gaps

def add_reasons(reasons_data, conn):
    with conn.cursor() as cur:
        for idx, reason_obj in enumerate(reasons_data):
           shortname = reason_obj['shortname']
           version = reason_obj['version']
           start_ts = reason_obj['start_ts']
           end_ts = reason_obj['end_ts']
           reason = reason_obj['reason']
           
           collection_id = f"{shortname}___{sanitize_versionid(version)}"
           start_dt = parse_datetime(start_ts)
           end_dt = parse_datetime(end_ts)
           
           cur.execute("""
               INSERT INTO reasons (collection_id, start_ts, end_ts, reason)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (collection_id, start_ts, end_ts) 
               DO UPDATE SET reason = EXCLUDED.reason
           """, (collection_id, start_dt, end_dt, reason))
                
        conn.commit()
    
def lambda_handler(event: events.SQSEvent, context: Context) -> Dict[str, Any]:
    """Main event handler

    Args:
        event (dict):
        context (Context): The runtime information of the function.

    Returns:
        dict: HTTP response
    """
    validate_environment_variables(["RDS_SECRET", "RDS_PROXY_HOST"])
    http_method = event.get("httpMethod", "")
    resource_path = event.get("path", "")
    logger.info(f"Got HTTP {http_method} for {resource_path}")
    try:
        with get_db_connection() as conn:

            # Create new reason
            if http_method == "POST":
                try:
                    payload = json.loads(event["body"])["reasons"]
                except Exception as e:
                    logger.error(f"Invalid request: {str(e)}")
                    logger.error(traceback.format_exc())
                    return build_response(400, {"message": f"Invalid request: {str(e)}"})
                try:
                    add_reasons(payload, conn)
                    return build_response(201, {"message": f"Sucessfully added reasons for: {payload}"})
                except Exception as e:
                    logger.error(f"Server error: {str(e)}")
                    logger.error(traceback.format_exc())
                    return build_response(500, {"message": f"Server error: {str(e)}"})

            # Retreive gaps intersecting reasons
            elif http_method == "GET":
                try:
                    cid, start, end, reason = parse_event(event)
                except Exception as e:
                    logger.error(f"Invalid request: {str(e)}")
                    return build_response(400, {"message": f"Bad Request: {str(e)}"})

                gaps = get_known_gaps(cid, start, end, conn)
                return build_response(200, {"gaps": gaps})

            else:
                return build_response(
                    501, {"message": "Requested method not implemented"}
                )
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        logger.error(traceback.format_exc())
        return build_response(500, {"message": "Unexpected error ocurred"})
