import json
import logging
from typing import List, Tuple, Dict, Any
from aws_lambda_typing import context as Context
import traceback
from dateutil.parser import parse as parse_datetime
from datetime import datetime
from utils import (
    get_db_connection,
    validate_environment_variables,
    sanitize_versionid,
    check_gap_config,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder to handle datetime objects"""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def build_response(status_code: int, body: Any) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "body": json.dumps(body, cls=DateTimeEncoder),
    }


def parse_event(event: Dict[str, Any]) -> Tuple[str, str, str, str]:
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


def get_reasons(cid: str, start: str, end: str, conn: Any) -> List[Dict[str, Any]]:
    """
    Gets all gaps that fall within the specified time range for a collection.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT start_ts, end_ts, reason
            FROM  reasons r
            WHERE collection_id = %s
            AND tsrange(start_ts, end_ts) && tsrange(%s, %s)
            ORDER BY start_ts        
            """,
            (cid, start, end),
        )

        columns = ["start_time", "end_time", "reason"]
        gaps = [dict(zip(columns, row)) for row in cur.fetchall()]

        return gaps


# TODO Clarify overlap conflict behavior
def add_reasons(reasons_data: List[Dict[str, Any]], conn: Any) -> None:
    with conn.cursor() as cur:
        for idx, reason_obj in enumerate(reasons_data):
            shortname = reason_obj["shortname"]
            version = reason_obj["version"]
            start_ts = reason_obj["start_ts"]
            end_ts = reason_obj["end_ts"]
            reason = reason_obj["reason"]

            collection_id = f"{shortname}___{sanitize_versionid(version)}"
            start_dt = parse_datetime(start_ts)
            end_dt = parse_datetime(end_ts)

            cur.execute(
                """
               INSERT INTO reasons (collection_id, start_ts, end_ts, reason)
               VALUES (%s, %s, %s, %s)
           """,
                (collection_id, start_dt, end_dt, reason),
            )

        conn.commit()


def lambda_handler(event: Dict[str, Any], context: Context) -> Dict[str, Any]:
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
    
    logger.debug(f"Got HTTP {http_method} for {resource_path}")
    
    try:
        with get_db_connection() as conn:

            # Create new reason
            if http_method == "POST":
                try:
                    payload = json.loads(event["body"])["reasons"]
                except Exception as e:
                    logger.warning(f"Invalid request: {str(e)}")
                    return build_response(
                        400, {"message": f"Invalid request: {str(e)}"}
                    )
                
                try:
                    add_reasons(payload, conn)
                    logger.info(f"Successfully added {len(payload)} reasons")
                    return build_response(
                        201, {"message": f"Successfully added {len(payload)} reasons"}
                    )
                except Exception as e:
                    logger.error(f"Failed to add reasons: {str(e)}")
                    logger.debug(traceback.format_exc())
                    return build_response(500, {"message": f"Server error: {str(e)}"})

            # Retrieve reasons
            elif http_method == "GET":
                try:
                    cid, start, end, reason = parse_event(event)
                except Exception as e:
                    logger.warning(f"Invalid GET request parameters: {str(e)}")
                    return build_response(400, {"message": f"Bad Request: {str(e)}"})

                try:
                    reasons = get_reasons(cid, start, end, conn)
                    logger.info(f"Reasons retrieved: {cid} - {len(reasons)} reasons found")
                    return build_response(200, {"reasons": reasons})
                except Exception as e:
                    logger.error(f"Failed to retrieve reasons for {cid}: {str(e)}")
                    logger.debug(traceback.format_exc())
                    return build_response(500, {"message": f"Server error: {str(e)}"})

            else:
                logger.warning(f"Unsupported method: {http_method}")
                return build_response(
                    501, {"message": "Requested method not implemented"}
                )
                
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        logger.debug(traceback.format_exc())
        return build_response(500, {"message": "Unexpected error ocurred"})