import requests
import boto3
import json
from datetime import datetime
import os
import logging
from typing import Dict, Any, Tuple
from psycopg import errors as psycopgErrors
import psycopg
from aws_lambda_typing import context as Context, events
import traceback
from utils import get_db_connection, validate_environment_variables
import re
from psycopg.sql import SQL, Identifier, Literal
import botocore
import asyncio
import aiobotocore.session

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def build_response(status_code: int, body: any) -> dict[str, any]:
    return {
        "statusCode": status_code,
        "headers": {
            "content-type": "application/json",
            "access-control-allow-origin": "*",
        },
        "body": json.dumps(body),
    }


def check_collections(conn) -> bool:
    """Gets all current collections"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT distinct collection_id from collections
            """
        )
        collections = cur.fetchall()
    return [coll[0] for coll in collections]


def parse_event(event):
    """Parses an event object for gap data"""
    collections = []
    body = json.loads(event["body"])
    for collection in body["collections"]:
        collection_name = collection.get("short_name")
        collection_version = collection.get("version")
        collection_version_safe = (
            collection_version.replace(".", "_") if collection_version else None
        )
        tolerance = collection.get("tolerance")

        if collection_name is None or collection_version is None:
            raise Exception("Error: `short_name` and `version` are required")

        collections.append(
            {
                "name": collection_name,
                "version": collection_version_safe,
                "raw_version": collection_version,  # save raw version for DynamoDB
                "tolerance": tolerance,
            }
        )
    return collections, body.get("backfill", "")


def get_cmr_time(collection_id: str) -> Tuple[str, str]:
    """Retrieve temporal extent information for a collection from CMR.

    Args:
        collection_id (str): The collection ID in format 'short_name___version'.

    Returns:
        tuple: A tuple containing (start_time, end_time) where start_time is a
               string and end_time is a string (either the CMR value or max datetime).
    """
    short_name, version = collection_id.rsplit("___", 1)
    version = version.replace("_", ".")
    cmr_env = os.getenv("CMR_ENV").lower()
    if cmr_env == "prod":
        url = f"https://cmr.earthdata.nasa.gov/search/collections.umm_json_v1_4?short_name={short_name}&version={version}"
    else:
        url = f"https://cmr.{cmr_env}.earthdata.nasa.gov/search/collections.umm_json_v1_4?short_name={short_name}&version={version}"
    logger.info(f"Requesting granule time from: {url}")
    res = requests.get(url)
    data = res.json()
    if not data["items"]:
        logger.error(f"{collection_id} not found in CMR")
        raise Exception(f"{collection_id} not found in CMR")
    # Assuming start time is always found
    temporal_extents = data["items"][0]["umm"]["TemporalExtents"][0]["RangeDateTimes"][
        0
    ]
    start = temporal_extents["BeginningDateTime"]
    end = temporal_extents.get("EndingDateTime", datetime.max.isoformat())
    return start, end


def init_collection(collection_name, collection_version, conn) -> str:
    """
    Initializes a collection in the database by creating a partition for the collection,
    adding collection to `collections` table, and adding an initial gap spanning the collection's
    temproral extent (implemented as a sql function triggered on insert into `collections` table)

    Args:
        collection_name (str): Collection short name
        collection_version (str): Collection version

    Returns:
        dict: Response from the Lambda invocation
    """

    collection_id = f"{collection_name}___{collection_version}"
    try:
        start, end = get_cmr_time(collection_id)
        logger.info(f"Initializing {collection_id} with {start, end}")

        # For new collection, partition `gaps` and `reasons` tables  and insert into `collections` table
        with conn.cursor() as cur:
            # Check if partition already exists
            safe_collection_id = re.sub(r"\W+", "_", collection_id)
            partition_name = f"gaps_{safe_collection_id}"
            cur.execute(
                """
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s AND n.nspname = 'public'
            """,
                (partition_name,),
            )
            if cur.fetchone() is None:
                # Create the partition
                cur.execute(
                    SQL("CREATE TABLE {} PARTITION OF gaps FOR VALUES IN ({})").format(
                        Identifier(partition_name), Literal(collection_id)
                    )
                )
                # Add the exclusion constraint to the partition
                constraint_name = f"{partition_name}_no_overlap"
                cur.execute(
                    SQL(
                        "ALTER TABLE {} ADD CONSTRAINT {} EXCLUDE USING gist (tsrange(start_ts, end_ts) WITH &&)"
                    ).format(Identifier(partition_name), Identifier(constraint_name))
                )
                logger.info(
                    f"Created gaps partition {partition_name} for collection {collection_id}"
                )
            # Create partition on `reasons` table
            reasons_partition_name = f"reasons_{safe_collection_id}"
            cur.execute(
                """
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s AND n.nspname = 'public'
            """,
                (reasons_partition_name,),
            )
            if cur.fetchone() is None:
                # Create the reasons partition
                cur.execute(
                    SQL(
                        "CREATE TABLE {} PARTITION OF reasons FOR VALUES IN ({})"
                    ).format(Identifier(reasons_partition_name), Literal(collection_id))
                )
                # Add the exclusion constraint to the reasons partition
                reasons_constraint_name = f"{reasons_partition_name}_no_overlap"
                cur.execute(
                    SQL(
                        "ALTER TABLE {} ADD CONSTRAINT {} EXCLUDE USING gist (tsrange(start_ts, end_ts) WITH &&)"
                    ).format(
                        Identifier(reasons_partition_name),
                        Identifier(reasons_constraint_name),
                    )
                )
                logger.info(
                    f"Created reasons partition {reasons_partition_name} for collection {collection_id}"
                )

            # Insert new collection in collections table, triggers initial gap insert into gap table
            cur.execute(
                """
                INSERT INTO collections (collection_id, temporal_extent_start, temporal_extent_end) 
                VALUES (%s, %s, %s)
                """,
                (collection_id, start, end),
            )

            conn.commit()
        return f"Initialized collection {collection_id} in table"

    except Exception as e:
        conn.rollback()
        logger.warning(traceback.format_exc())
        return f"Collection {collection_id} initialization failed: {str(e)}"


async def init_migration_stream(collections):
    """
    Invoke the gapMigrationStreamMessageCompiler Lambda function for multiple collections concurrently
    Args:
        collections (list): List of dicts with 'name' and 'version' keys
    Returns:
        dict: Aggregated response from all Lambda invocations
    """
    # Configure client to wait for backfill execution, defaults are too short
    config = aiobotocore.config.AioConfig(
        connect_timeout=900,
        read_timeout=900,
        retries={"max_attempts": 0},
        tcp_keepalive=True,  # NAT gateway has internal timeout of 350s so we need keepalive here
    )

    session = aiobotocore.session.get_session()
    async with session.create_client(
        "lambda", region_name=os.getenv("AWS_REGION"), config=config
    ) as lambda_client:

        # Create tasks for concurrent execution
        tasks = []
        for collection in collections:
            payload = {
                "Records": [
                    {
                        "Sns": {
                            "Message": json.dumps(
                                {
                                    "short_name": collection["name"],
                                    "version": collection["version"],
                                }
                            )
                        }
                    }
                ]
            }

            task = lambda_client.invoke(
                FunctionName=os.environ.get("MIGRATION_STREAM_COMPILER_LAMBDA"),
                Payload=json.dumps(payload),
            )
            tasks.append((task, collection))

        # Wait for all tasks to return
        responses = await asyncio.gather(*[task for task, _ in tasks])
        results = []
        for i, response in enumerate(responses):
            collection = tasks[i][1]
            payload_response = await response["Payload"].read()
            payload_data = json.loads(payload_response.decode())
            if response["StatusCode"] != 200 or payload_data.get("statusCode") != 200:
                #raise Exception(
                logger.warn(
                    f"Collection backfill failed for {collection['name']}: {payload_data.get('body')}"
                )

            results.append(
                {
                    "collection": collection["name"],
                    "status": "success",
                    "statusCode": response["StatusCode"],
                    "message": f"Collection backfill complete: {payload_data.get('body')}",
                }
            )

        return {
            "status": "success",
            "results": results,
            "message": f"All {len(collections)} collection backfills completed successfully",
        }


def save_tolerance_to_dynamodb(shortname: str, versionid: str, tolerance: int):
    """Save tolerance value to DynamoDB"""
    dynamodb = boto3.resource("dynamodb")
    table_name = os.environ.get("TOLERANCE_TABLE_NAME")
    if not table_name:
        raise ValueError("Missing TOLERANCE_TABLE_NAME environment variable")

    table = dynamodb.Table(table_name)
    try:
        response = table.put_item(
            Item={
                "shortname": shortname,
                "versionid": versionid,
                "granulegap": tolerance,
            }
        )
        logger.info(
            f"Saved tolerance for {shortname}___{versionid}: {tolerance} seconds. PutItem Response: {response['ResponseMetadata']['HTTPStatusCode']}"
        )
    except Exception as e:
        logger.error(f"Failed to save tolerance to DynamoDB: {str(e)}")
        raise


def lambda_handler(event: events.SQSEvent, context: Context) -> Dict[str, Any]:
    """Main event handler

    Args:
        event (dict):
        context (Context): The runtime information of the function.

    Returns:
        dict: HTTP response
    """
    validate_environment_variables(
        [
            "RDS_SECRET",
            "RDS_PROXY_HOST",
            "CMR_ENV",
            "MIGRATION_STREAM_COMPILER_LAMBDA",
            "TOLERANCE_TABLE_NAME",
        ]
    )

    try:
        http_method = event.get("httpMethod", "")
        resource_path = event.get("path", "")
        logger.info(f"Got HTTP {http_method} for {resource_path}")

        try:
            collections, backfill_behavior = parse_event(event)
        except Exception as e:
            message = f"Error processing request: {str(e)}"
            logger.error(traceback.format_exc())
            return build_response(400, {"message": message})

        if http_method != "POST":
            return build_response(405, {"message": "Unsupported request method"})

        backfill_collections = []
        with get_db_connection() as conn:
            current_collections = check_collections(conn)
            for collection in collections:
                collection_id = f"{collection['name']}___{collection['version']}"
                tolerance = collection.get("tolerance")
                # Update tolerance table even if the collection already exists
                if tolerance is not None:
                    try:
                        save_tolerance_to_dynamodb(
                            collection["name"],
                            collection["raw_version"],
                            int(tolerance),
                        )
                    except Exception as e:
                        logger.error(
                            f"Error saving tolerance for {collection['name']}___{collection['raw_version']}: {str(e)}"
                        )
                # Add collection to collections table, create partition for gaps table, set initial full gap
                if collection_id not in current_collections:
                    message = init_collection(
                        collection["name"], collection["version"], conn
                    )
                    logger.info(message)
                    backfill_collections.append(collection)

                # Skip DB init but still backfill granules from CMR
                elif backfill_behavior.lower() == "force":
                    logger.info(
                        f"Force flag detected, proceeding with backfill for existing collection: {collection_id}"
                    )
                    backfill_collections.append(collection)
                else:
                    logger.info(
                        f"Skipping initialization of {collection_id}: already exists in collection table"
                    )

            # Kick off the migration stream
            try:
                logger.info(f"Starting collection backfill")
                print(backfill_collections)
                migration_result = asyncio.run(
                    init_migration_stream(
                        [
                            {
                                "name": collection["name"],
                                "version": collection["version"].replace("_", "."),
                            }
                            for collection in backfill_collections
                        ]
                    )
                )
                #logger.info(f"Backfill result: {migration_result}")
            except Exception as e:
                message = f"Collection backfill failed for: {str(e)}"
                logger.error(message)
                logger.error(traceback.format_exc())
                logger.warn(
                    f"Collection left in incomplete state, use force=True to rectify"
                )
                return build_response(500, {"message": message})

        return build_response(
            200, {"message": f"Collection initialization complete for {collections}"}
        )

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        logger.error(traceback.format_exc())
        return build_response(500, {"message": "Unexpected error occurred"})
