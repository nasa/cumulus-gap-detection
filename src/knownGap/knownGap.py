import boto3
import json
import os
import logging
from typing import Dict, Any
from psycopg import errors as psycopgErrors
from aws_lambda_typing import context as Context, events
import traceback
from utils import get_db_connection, validate_environment_variables
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


def validate_request(body, schema):
    """Validate request against JSON schema"""
    try:
        validator = jsonschema.Draft7Validator(schema)
        errors = list(validator.iter_errors(body))
        if not errors:
            return True, None
        for error in errors:
            if "enum" in error.schema and "operation" in error.path:
                return (
                    False,
                    "Invalid operation. Must be one of: create, update, delete",
                )
            if error.validator in ("oneOf", "allOf", "dependencies"):
                for suberror in error.context:
                    if "errorMessage" in suberror.schema:
                        msg = suberror.schema["errorMessage"]
                        if "reason is required" in msg.lower() and "update" in str(
                            body.get("operation", "")
                        ):
                            return False, "Operation 'update' requires a reason"
                        if (
                            "delete" in str(body.get("operation", ""))
                            and "reason" in body
                        ):
                            return (
                                False,
                                "Operation 'delete' should not include a reason",
                            )
                        return False, msg
            if "errorMessage" in error.schema:
                return False, error.schema["errorMessage"]
        return False, str(errors[0])
    except Exception as e:
        return False, str(e)


def build_response(status_code: int, body: any) -> dict[str, any]:
    return {
        "statusCode": status_code,
        "headers": {
            "content-type": "application/json",
            "access-control-allow-methods": "GET, PUT, OPTIONS",
        },
        "body": json.dumps(body, cls=DateTimeEncoder),
    }


def parse_event(event):
    """Parses an event object for gap data. Checks both the request body and
    query string params to handle PUT and GET requests, respectively"""
    cid = start = end = reason = operation = ""
    if event.get("body"):
        body = json.loads(event["body"])
        collection_name = body.get("collection").get("short_name")
        collection_version = body.get("collection").get("version")
        start = body.get("gap_begin")
        end = body.get("gap_end")
        reason = body.get("reason")
        operation = body.get("operation")
    else:
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
    return cid, start, end, reason, operation


def get_gaps(cid, start, end, conn):
    """
    Gets all gaps that fall within the specified time range for a collection.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT gap_id, collection_id, start_ts, end_ts, reason 
            FROM gaps
            WHERE collection_id = %s
            AND tsrange(%s, %s) @> tsrange(start_ts, end_ts)
        """,
            (cid, start, end),
        )

        columns = ["gap_id", "collection_id", "start_ts", "end_ts", "reason"]
        gaps = [dict(zip(columns, row)) for row in cur.fetchall()]

        return gaps

# TODO Enforce all reasons equal
def validate_operation(gaps, operation):
    """
    Validates the requested operation against the current state of gaps.

    Args:
        gaps (list): List of gap dictionaries to validate
        operation (str): Operation type - "update", "delete", or "create"

    Returns:
        str or None: Error message if validation fails, None if validation passes
    """
    if not gaps:
        return "No gaps found in range"

    null_gaps = [gap["gap_id"] for gap in gaps if gap["reason"] is None]
    non_null_gaps = [gap["gap_id"] for gap in gaps if gap["reason"] is not None]

    if operation == "delete" and null_gaps:
        return f"Cannot delete NULL reasons from gaps: {null_gaps}"

    if operation == "update" and null_gaps:
        return f"Cannot update NULL reasons for gaps: {null_gaps}"

    if operation == "create" and non_null_gaps:
        return f"Cannot create reason for gaps that already have one: {non_null_gaps}"

    return None


def update_reason(cid, start, end, reason, operation, conn):
    """
    Updates the reason field for all gaps within a specified time range.

    Args:
        cid (str): Collection ID
        start (str): Start timestamp for the range
        end (str): End timestamp for the range
        reason (str): New reason to set (None for deletion)
        operation (str): "update", "create", or "delete"

    Returns:
        str: Message describing the result of the operation
    """
    gaps = get_gaps(cid, start, end, conn)
    error = validate_operation(gaps, operation)
    if error:
        raise ValueError(f"Invalid request: {error}")
    gap_ids = [gap["gap_id"] for gap in gaps]
    if not gap_ids:
        return f"No gaps found in range for collection {cid}"

    with conn.cursor() as cur:
        if operation == "delete":
            cur.execute(
                "UPDATE gaps SET reason = NULL WHERE gap_id = ANY(%s)",
                (gap_ids,),
            )
            return f"Deleted reason from {len(gap_ids)} gaps in range"
        else:
            cur.execute(
                "UPDATE gaps SET reason = %s WHERE gap_id = ANY(%s)",
                (reason, gap_ids),
            )
            operation_str = "Updated" if operation == "update" else "Added"
            return f"{operation_str} reason to '{reason}' for {len(gap_ids)} gaps in range"


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
            if http_method == "GET":
                try:
                    cid, start, end, _, _ = parse_event(event)
                except Exception as e:
                    logger.error(f"Invalid request: {str(e)}")
                    return build_response(400, {"message": f"Bad Request: {str(e)}"})

                gaps = get_gaps(cid, start, end, conn)
                return build_response(200, {"gaps": gaps})
            elif http_method == "PUT":
                if not event.get("body"):
                    logger.error("Missing request body")
                    return build_response(400, {"message": "Missing request body"})
                try:
                    body = json.loads(event["body"])
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON: {str(e)}")
                    return build_response(400, {"message": f"Invalid JSON: {str(e)}"})
                with open("schema.json") as f:
                    schema = json.loads(f.read())
                valid, error = validate_request(body, schema)
                if not valid:
                    logger.error(f"Invalid request: {error}")
                    return build_response(400, {"message": error})
                try:
                    cid, start, end, reason, operation = parse_event(event)
                    message = update_reason(cid, start, end, reason, operation, conn)
                except Exception as e:
                    logger.error(f"Invalid request: {str(e)}")
                    return build_response(400, {"message": f"Invalid request: {str(e)}"})
                return build_response(200, {"message": message})
            else:
                return build_response(501, {"message": "Requested method not implemented"})
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        logger.error(traceback.format_exc())
        return build_response(500, {"message": "Unexpected error ocurred"})
