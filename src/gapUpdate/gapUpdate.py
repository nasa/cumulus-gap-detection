import boto3
import json
import os
import psycopg
import logging
from collections import defaultdict, deque
from datetime import datetime
from botocore.exceptions import ClientError
import requests
from io import StringIO
from typing import Dict, Any, Set, Tuple, Optional
from aws_lambda_typing import context as Context, events
from psycopg.sql import SQL, Identifier, Literal
from utils import get_db_connection, validate_environment_variables
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_sql(filename: str) -> str:
    with open(os.path.join(_MODULE_DIR, filename)) as f:
        return f.read()


SHRINK_GAPS_QUERY = _load_sql("shrink_gaps.sql")
GROW_GAPS_QUERY = _load_sql("grow_gaps.sql")


def get_all_collections(conn) -> Set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT collection_id FROM collections")
        return {row[0] for row in cur.fetchall()}


def _run_gap_transaction(
    collection_id, records_buffer: StringIO, conn: psycopg.Connection,
    grow: bool = False, blocking: bool = True
) -> bool:
    """Runs the specified gap transaction for one collection. """
    query = GROW_GAPS_QUERY if grow else SHRINK_GAPS_QUERY

    cursor = conn.cursor()
    try:
        # Aqcuire lock to prevent races across concurrent executions.
        if blocking:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (collection_id,))
            acquired = True
        else:
            cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (collection_id,))
            acquired = cursor.fetchone()[0]

        if not acquired:
            conn.rollback()
            logger.debug(f"Collection {collection_id} is locked elsewhere, deferring")
            return False

        cursor.execute(
            """
            CREATE TEMP TABLE input_records(
                collection_id text,
                start_ts timestamp,
                end_ts timestamp) ON COMMIT DROP
        """
        )
        with cursor.copy("COPY input_records FROM STDIN WITH DELIMITER '\t'") as copy:
            copy.write(records_buffer.read())
        cursor.execute(query, {"collection_id": collection_id})
        conn.commit()
        logger.debug(f"Transaction committed for collection {collection_id}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing collection {collection_id}: {str(e)}")
        logger.debug(traceback.format_exc())
        raise e
    finally:
        cursor.close()

def lambda_handler(event: events.SQSEvent, context: Context) -> Dict[str, Any]:
    """Main event handler that orchestrates batch processing.

    Args:
        event (dict): SQS event containing collection records.
        context (Context): The runtime information of the function.

    Returns:
        dict: HTTP response with status code 200 on success or error status.
    """
    validate_environment_variables(
        ["RDS_SECRET", "RDS_PROXY_HOST", "CMR_ENV", "AWS_REGION", "DELETION_QUEUE_ARN"]
    )

    failures = []
    with get_db_connection() as conn:
        # Fetch all monitored collections
        monitored_collections = get_all_collections(conn)

        delete = False
        records_by_collection = defaultdict(lambda: {"records": [], "message_ids": []})
        unmonitored_seen = set()

        for record in event["Records"]:
            # Check which queue this event is from
            if record["eventSourceARN"] == os.getenv("DELETION_QUEUE_ARN"):
                logger.debug("Adding gaps for deleted granules")
                delete = True
            r = json.loads(json.loads(record["body"])["Message"])["record"]
            collection_id = r["collectionId"].replace(".", "_")

            if collection_id not in monitored_collections:
                if collection_id not in unmonitored_seen:
                    logger.info(f"Skipping unmonitored collection, not opted into gap tracking: {collection_id}")
                    unmonitored_seen.add(collection_id)
                continue

            records_by_collection[collection_id]["records"].append(
                {
                    "collection_id": collection_id,
                    "start_ts": r["beginningDateTime"],
                    "end_ts": r["endingDateTime"],
                }
            )
            records_by_collection[collection_id]["message_ids"].append(record["messageId"])

        total_records = sum(len(data["records"]) for data in records_by_collection.values())
        logger.info(
            f"Processing gap {'growth' if delete else 'shrink'}: {total_records} records across "
            f"{len(records_by_collection)} monitored collections"
        )

        def build_buffer(records):
            buffer = StringIO()
            for r in records:
                buffer.write(f"{r['collection_id']}\t{r['start_ts']}\t{r['end_ts']}\n")
            buffer.seek(0)
            return buffer

        # Process each collection, skip waiting for lock while we have multiple pending
        pending = deque(
            (collection_id, data, build_buffer(data["records"]), False)
            for collection_id, data in records_by_collection.items()
        )
        while pending:
            collection_id, data, buffer, blocking = pending.popleft()
            try:
                logger.debug(f"Processing collection {collection_id} with {len(data['records'])} records")
                acquired = _run_gap_transaction(collection_id, buffer, conn, grow=delete, blocking=blocking)
                if not acquired:
                    pending.append((collection_id, data, buffer, True))
            except Exception as e:
                logger.error(f"Failed to process collection {collection_id}: {str(e)}")
                failures.extend(data["message_ids"])

    # Summary logging
    if failures:
        logger.warning(f"gap {'growth' if delete else 'shrink'} completed with failures: {len(failures)} failed messages from {len(records_by_collection)} collections")
    else:
        logger.info(f"gap {'growth' if delete else 'shrink'} completed successfully: {len(records_by_collection)} collections processed")

    # Return failed messages to the queue
    return {
        "batchItemFailures": [{"itemIdentifier": message_id} for message_id in failures]
    }
