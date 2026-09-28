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
import time 
import threading
from psycopg import errors as pg_errors

CLEANUP_MARGIN_SECONDS = float(os.getenv("CLEANUP_MARGIN_SECONDS", "1"))

class _DeadlineExceeded(Exception):
    """Raised when the invocation's cleanup margin is reached"""


logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_sql(filename: str) -> str:
    with open(os.path.join(_MODULE_DIR, filename)) as f:
        return f.read()


CLOSE_GAPS_QUERY = _load_sql("close_gaps.sql")
OPEN_GAPS_QUERY = _load_sql("open_gaps.sql")


def get_all_collections(conn) -> Set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT collection_id FROM collections")
        return {row[0] for row in cur.fetchall()}


def _run_gap_transaction(
    collection_id, records_buffer: StringIO, conn: psycopg.Connection,
    deadline: float, _open: bool = False
) -> bool:
    query = OPEN_GAPS_QUERY if _open else CLOSE_GAPS_QUERY
    cursor = conn.cursor()

    if deadline - time.monotonic() <= 0:
        raise _DeadlineExceeded(collection_id)
    try:
        # Aqcuire lock to prevent races across concurrent executions.
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
    except pg_errors.QueryCanceled:
        conn.rollback()
        logger.warning(f"Cancelled processing collection {collection_id}: cleanup margin reached")
        raise _DeadlineExceeded(collection_id)
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

    # deadline for the invocation at which point we abort remaining work
    deadline = time.monotonic() + (context.get_remaining_time_in_millis() / 1000.0) - CLEANUP_MARGIN_SECONDS

    all_message_ids = [record["messageId"] for record in event["Records"]]
    failures = []

    with get_db_connection() as conn:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            logger.warning("Deadline already reached after acquiring connection; failing entire batch")
            return {"batchItemFailures": [{"itemIdentifier": mid} for mid in all_message_ids]}

        timer = threading.Timer(remaining, conn.cancel_safe)
        timer.daemon = True
        timer.start()

        pending = deque()
        records_by_collection = defaultdict(lambda: {"records": [], "message_ids": []})
        delete = False

        try:
             # Fetch all monitored collections
            monitored_collections = get_all_collections(conn)

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
                f"Processing gap {'open' if delete else 'close'}: {total_records} records across "
                f"{len(records_by_collection)} monitored collections"
            )

            def build_buffer(records):
                buffer = StringIO()
                for r in records:
                    buffer.write(f"{r['collection_id']}\t{r['start_ts']}\t{r['end_ts']}\n")
                buffer.seek(0)
                return buffer

            pending.extend(
                (collection_id, data, build_buffer(data["records"]))
                for collection_id, data in records_by_collection.items()
            )
            while pending:
                collection_id, data, buffer = pending.popleft()
                try:
                    logger.debug(f"Processing collection {collection_id} with {len(data['records'])} records")
                    acquired = _run_gap_transaction(collection_id, buffer, conn, deadline, _open=delete)
                    if not acquired:
                        pending.append((collection_id, data, buffer))
                except _DeadlineExceeded:
                    logger.warning(
                        f"Deadline reached on collection {collection_id}; deferring it and "
                        f"{len(pending)} remaining collection(s) to batchItemFailures"
                    )
                    failures.extend(data["message_ids"])
                    for _cid, d, _buf in pending:
                        failures.extend(d["message_ids"])
                    pending.clear()
                    raise
                except Exception as e:
                    logger.error(f"Failed to process collection {collection_id}: {str(e)}")
                    failures.extend(data["message_ids"])

        except pg_errors.QueryCanceled:
            conn.rollback()
            logger.warning("Deadline reached during collection lookup; failing entire batch")
            return {"batchItemFailures": [{"itemIdentifier": mid} for mid in all_message_ids]}
        except _DeadlineExceeded:
            pass
        finally:
            timer.cancel()

    if failures:
        logger.warning(f"gap {'open' if delete else 'close'} completed with failures: {len(failures)} failed messages from {len(records_by_collection)} collections")
    else:
        logger.info(f"gap {'open' if delete else 'close'} completed successfully: {len(records_by_collection)} collections processed")

    return {
        "batchItemFailures": [{"itemIdentifier": message_id} for message_id in failures]
    }
