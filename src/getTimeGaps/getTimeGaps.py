import boto3
import os
import psycopg
from datetime import datetime
import logging
import json
from botocore.exceptions import ClientError
from typing import Dict, Any, Set, Tuple, Optional
from dateutil.parser import parse
from utils import get_db_connection, validate_environment_variables, sanitize_versionid, get_granule_gap, fetch_time_gaps, check_gap_config

logger = logging.getLogger()
logger.setLevel(logging.INFO)



def build_response(status_code: int, body: any) -> dict[str, any]:
    """
    Builds https response object and returns it.

    Args:
        status_code (str): The HTTPS status code for response
        body (str): The message body for response

    Returns:
        dict: A dict for the response object
    """
    return {
        "statusCode": status_code,
        "headers": {
            "content-type": "application/json",
            "access-control-allow-origin": "*",
        },
        "body": json.dumps(body, default=str),
    }



def check_date_format(date_string) -> bool:
    """
    Checks if date string is a valid date in the format: %Y-%m-%d

    Args:
        date_string (str): The string of the date being queried

    Returns:
        bool: Returns whether date_string is a valid date in the required format.
    """
    format_string = "%Y-%m-%d"
    try:
        datetime.strptime(date_string, format_string)
        return True
    except ValueError:
        return False

def compare_dates(startDate, endDate) -> bool:
    """
    Compares startDate and endDate strings and returns whether startDate is less than or equal to endDate.

    Args:
        startDate (dict): The startDate provided by the request.
        context (object): The endDate provided by the request.

    Returns:
        bool: Result of comparison between startDate and endDate as datetime objects.
    """
    format_string = "%Y-%m-%d"

    startDatetime = datetime.strptime(startDate, format_string).date()
    endDatetime = datetime.strptime(endDate, format_string).date()

    if startDatetime <= endDatetime:
        return True
    else:
        return False


def get_presigned_url(data: dict, collection_id: str) -> str:
    s3_client = boto3.client("s3")
    bucket_name = os.environ.get("GAP_RESPONSE_BUCKET")

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    s3_key = f"gaps/{collection_id}/{timestamp}.json"

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(data, default=str),
            ContentType="application/json",
        )

        presigned_url = s3_client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket_name, "Key": s3_key}, ExpiresIn=3600
        )

        logger.info(f"Data saved to S3 and pre-signed URL generated: {s3_key}")
        return presigned_url

    except Exception as e:
        logger.error(f"Error saving data to S3: {e}")
        raise e


def lambda_handler(event, context):
    """
    AWS Lambda handler function. Retrieves granule gaps, fetches time gaps from PostgreSQL if necessary and filters them,
    and returns HTTPs response

    Args:
        event (dict): The input event containing shortname and version ID.
        context (object): AWS Lambda context object.

    Returns:
        dict: Response indicating success or failure.
    """

    logger.info(f"Received event: {json.dumps(event)}")

    # get query params
    params = event.get('queryStringParameters') or {}

    validate_environment_variables(["GAP_RESPONSE_BUCKET"])
    shortname = params.get('short_name')
    versionid = params.get('version')
    toleranceCheck = False
    knownCheck = False
    startDate = None
    endDate = None

    if not shortname or not versionid:
        return build_response(
            400, {"message": "Missing query parameters: shortname or versionid"}
        )

    collection_id = f"{shortname}___{sanitize_versionid(versionid)}"

    # Checks if tolerance filter was applied in parameters
    if 'tolerance' in params:
        tolerance_val = params['tolerance'].lower()
        if tolerance_val == "true":
            toleranceCheck = True
        elif tolerance_val == "false":
            toleranceCheck = False
        else:
            return build_response(
                400,
                {
                    "message": "Bad request: Tolerance flag should be either 'true' or 'false'"
                },
            )

    # Checks if known filter was applied in parameters
    if 'knownGap' in params:
        known_value = params['knownGap'].lower()
        if known_value == "true":
            knownCheck = True
        elif known_value == "false":
            knownCheck = False
        else:
            return build_response(
                400,
                {
                    "message": "Bad request: Known gap flag should be either 'true' or 'false'"
                },
            )

    # Checks if startDate filter was applied in parameters
    if 'startDate' in params:
        startDate = params['startDate']
        if not check_date_format(startDate):
            return build_response(400, {'message': "Bad request: Start date needs to be in format YEAR-MONTH-DAY"})

    # Checks if endDate filter was applied in parameters
    if 'endDate' in params:
        endDate = params['endDate']
        if not check_date_format(endDate):
            return build_response(
                400,
                {
                    "message": "Bad request: End date needs to be in format YEAR-MONTH-DAY"
                },
            )

    with get_db_connection() as conn:

        with conn.cursor() as cursor:

            # Checks if collection was configured in gap_config. Returns 200 if not.
            if check_gap_config(collection_id, cursor):
                logging.info(f"Collection {collection_id} found in gap config table.")
            else:
                return build_response(
                    400,
                    {
                        "message": f"Collection {collection_id} has not been initialized for gap detection."
                    },
                )

            # Gets gap tolerance if tolerance filter was applied
            if toleranceCheck:
                logger.info(f"Fetching time gaps for {shortname} version {versionid}")
                granule_gap = get_granule_gap(shortname, versionid)
                logger.info(
                    f"Granule gap for {shortname} v{versionid}: {granule_gap} seconds"
                )
            else:
                granule_gap = 0

            # Fetch time gaps
            try:
                time_gaps = fetch_time_gaps(
                     shortname, 
                     versionid, 
                     granule_gap, 
                     cursor, 
                     knownCheck, 
                     startDate, 
                     endDate
                )
                logger.info(f"Fetched {len(time_gaps)} time gaps exceeding the granulegap threshold.")
            except Exception as e:
                logger.error(f"Failed to fetch time gaps: {e}")
                return build_response(
                    500, {"message": f"Failed to fetch time gaps: {e}"}
                )

    if startDate and endDate:
        if not compare_dates(startDate, endDate):
            return build_response(
                400, {"message": "Bad request: Start date is greater than end date"}
            )

    # If no gaps, return early
    if not time_gaps:
        logger.info("No time gaps exceed the granulegap threshold.")
        return build_response(200, {"message": "No qualifying time gaps found."})

    body = {"timeGaps": time_gaps, "gapTolerance": granule_gap}

    response_size = len(json.dumps(body, default=str).encode("utf-8"))

    # Threshold dictated by Lambda's 6MB response payload limit
    size_threshold = 6 * 1024 * 1024

    if response_size > size_threshold:
        logger.info(
            f"Response size ({response_size} bytes) exceeds lambda payload limit, generating pre-signed URL"
        )
        try:
            presigned_url = get_presigned_url(body, collection_id)
            return build_response(
                200,
                {
                    "message": "Too many results for response, use the presigned URL",
                    "presigned_url": presigned_url,
                },
            )

        except Exception as e:
            logger.error(f"Failed to generate pre-signed URL: {e}")
            return build_response(
                500, {"message": f"Failed to generate results URL: {e}"}
            )

    return build_response(200, body)
