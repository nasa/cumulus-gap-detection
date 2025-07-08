import boto3
import json
import os
import psycopg
import logging
from datetime import datetime
from botocore.exceptions import ClientError
import requests
from io import StringIO
from typing import Dict, Any, Set, Tuple, Optional
from aws_lambda_typing import context as Context, events
from psycopg.sql import SQL, Identifier, Literal
from utils import get_db_connection, validate_environment_variables
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def validate_collections(collections: Set[str], conn) -> bool:
    """Verifies that all given collections are in in collections table.
    Args:
        collections (set): A set of collection IDs to check and potentially initialize.
        conn (psycopg.Connection): An active database connection.

    Returns true if all input collections are in collections table and false otherwise.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT collection_id FROM collections WHERE collection_id IN ({','.join(['%s'] * len(collections))})",
            tuple(collections),
        )
        found = {row[0] for row in cur.fetchall()}
        missing = collections - found
        if missing:
            logging.error(f"Collections not in table: {missing}")
        return not missing


def update_gaps(
    collection_id, records_buffer: StringIO, update_query, conn: psycopg.Connection
) -> None:
    try:
        cursor = conn.cursor()
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
        # Lock collection and perform update procedure
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (collection_id,))
        cursor.execute(update_query)
        conn.commit()
        logger.info(f"Transaction committed for collection {collection_id}")
    except Exception as e:
        # Rollback on error
        conn.rollback()
        logger.error(f"Error processing collection {collection_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise e
    finally:
        cursor.close()

def add_gaps(
    collection_id, records_buffer: StringIO, conn: psycopg.Connection
) -> None:
    try:
        cursor = conn.cursor()
        # Load event records
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

        # Prevent races across concurrent executions
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (collection_id,))

       # Check for deletions on existing gaps
        cursor.execute("""
            SELECT 
                ir.start_ts as granule_start,
                ir.end_ts as granule_end,
                gaps.start_ts as gap_start,
                gaps.end_ts as gap_end
            FROM gaps, input_records ir 
            WHERE gaps.collection_id = ir.collection_id 
              AND tsrange(gaps.start_ts, gaps.end_ts) && 
                  tsrange(ir.start_ts, date_trunc('second', ir.end_ts) + interval '1 second')
        """)
        
        overlaps = cursor.fetchall()
        if overlaps:
            overlap_details = [f"granule[{o[0]}, {o[1]}] overlaps gap[{o[2]}, {o[3]}]" for o in overlaps]
            logger.warning(
                    f"Deleting nonexistent data: {collection_id}: {len(overlaps)} deleted granules overlap existing gaps. "
                f"Details: {', '.join(overlap_details)}"
            )
        
        # Add gaps for deleted granules and merge with existing gaps
        cursor.execute("""
            -- Round granule end time up to nearest second to eliminate boundary gaps
            WITH input_ranges AS (
                SELECT collection_id, tsrange(start_ts, date_trunc('second', end_ts) + interval '1 second') as gap_range
                FROM input_records
            ),

            -- Remove adjacent existing gaps
            removed_gaps AS (
                DELETE FROM gaps WHERE gap_id IN (
                    SELECT gap_id FROM gaps, input_ranges 
                    WHERE gaps.collection_id = input_ranges.collection_id 
                    AND (tsrange(gaps.start_ts, gaps.end_ts) && input_ranges.gap_range 
                         OR tsrange(gaps.start_ts, gaps.end_ts) -|- input_ranges.gap_range)
                ) RETURNING collection_id, tsrange(start_ts, end_ts) as gap_range
            ),
            -- Merge new gaps with existing gaps
            all_ranges AS (
                SELECT collection_id, gap_range FROM input_ranges
                UNION ALL SELECT collection_id, gap_range FROM removed_gaps
            )
            INSERT INTO gaps (collection_id, start_ts, end_ts)
            SELECT collection_id, lower(merged_range), upper(merged_range) 
            FROM (SELECT collection_id, unnest(range_agg(gap_range)) merged_range 
                  FROM all_ranges GROUP BY collection_id) final_ranges
        """)
        
        conn.commit()
        logger.info(f"Added gaps to collection {collection_id}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing collection {collection_id}: {str(e)}")
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
    delete = False
    # Parse records and group by collection
    records_by_collection = {}
    all_collections = set()
    for record in event["Records"]:
        # Check which queue this event is from 
        if record["eventSourceARN"] == os.getenv("DELETION_QUEUE_ARN"):
            logger.info("Adding gaps for deleted granules")
            delete = True
        r = json.loads(json.loads(record["body"])["Message"])["record"]
        collection_id = r["collectionId"].replace(".", "_")

        # Initialize empty key if first record from collection this batch
        if collection_id not in records_by_collection:
            records_by_collection[collection_id] = {"records": [], "message_ids": []}

        records_by_collection[collection_id]["records"].append(
            {
                "collection_id": collection_id,
                "start_ts": r["beginningDateTime"],
                "end_ts": r["endingDateTime"],
            }
        )
        records_by_collection[collection_id]["message_ids"].append(record["messageId"])
        all_collections.add(collection_id)
    logger.info(f"Grouped {sum(len(recs["records"]) for recs in records_by_collection.values())} records into {len(all_collections)} collections")

    failures = []
    with get_db_connection() as conn:
        # Load update query
        current_dir = os.path.dirname(os.path.abspath(__file__))
        query_path = os.path.join(current_dir, "update_gaps.sql")
        with open(query_path) as f:
            update_query = f.read()

        # Process each collection in a dedicated transaction to facilitate parallelism
        for collection_id, data in records_by_collection.items():
            try:
                # Verify this collection has been initialized
                if not validate_collections({collection_id}, conn):
                    failures.extend(data["message_ids"])
                    raise Exception(
                        "Error: Records for uninitialized collections detected, aborting."
                    )

                logger.info(f"Processing collection {collection_id} with {len(data["records"])} records")
                buffer = StringIO()
                for r in data["records"]:
                    line = f"{r['collection_id']}\t{r['start_ts']}\t{r['end_ts']}\n"
                    buffer.write(line)
                buffer.seek(0)
                if delete:
                    add_gaps(collection_id, buffer, conn)
                else:
                    update_gaps(collection_id, buffer, update_query, conn)

            except Exception as e:
                logger.error(f"Failed to process collection {collection_id}: {str(e)}")
                failures.extend(data["message_ids"])

    # Return failed messages to the queue
    return {
        "batchItemFailures": [{"itemIdentifier": message_id} for message_id in failures]
    }
