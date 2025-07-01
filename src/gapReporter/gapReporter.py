import boto3
import csv
import os
import psycopg
from datetime import datetime
import logging
import json
from botocore.exceptions import ClientError
from utils import get_db_connection, validate_environment_variables, sanitize_versionid, get_granule_gap, fetch_time_gaps

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def check_collections(conn):
    """Gets all distinct collection IDs from collections table"""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT collection_id FROM collections;")
        collections = cur.fetchall()
    return [coll[0] for coll in collections]

def parse_collection_id(collection_id):
    """
    Parses collection_id formatted as 'shortname___versionid' into components,
    splitting on the rightmost '___'.
    """
    if '___' not in collection_id:
        raise ValueError(f"Invalid collection_id format: {collection_id}")
    shortname, versionid = collection_id.rsplit('___', 1)
    return shortname, versionid.replace('_', '.')  # Reverse sanitize_versionid


def lambda_handler(event, context):
    """
    AWS Lambda handler that processes all collections from the DB.
    For each collection:
      - gets granule gap from DynamoDB
      - fetches time gaps exceeding granule gap
      - creates and uploads a CSV to S3 if gaps exist

    Returns summary of uploads.
    """
    validate_environment_variables(['GAP_REPORT_BUCKET'])

    results = []
    with get_db_connection() as conn:
        try:
            collections = check_collections(conn)
            logger.info(f"Found {len(collections)} collections in collections table.")
        except Exception as e:
            logger.error(f"Failed to fetch collections: {e}")
            return {'statusCode': 500, 'body': 'Failed to fetch collections'}

        for collection_id in collections:
            try:
                shortname, versionid = parse_collection_id(collection_id)
                logger.info(f"Processing collection: {shortname} version {versionid}")

                granule_gap = get_granule_gap(shortname, versionid)
                logger.info(f"Granule gap: {granule_gap} seconds")

                with conn.cursor() as cursor:
                    time_gaps = fetch_time_gaps(shortname, versionid, granule_gap, cursor)
                    logger.info(f"Found {len(time_gaps)} time gaps exceeding threshold.")

                if not time_gaps:
                    logger.info(f"No qualifying time gaps for {collection_id}. Skipping upload.")
                    results.append({'collection_id': collection_id, 'status': 'no gaps'})
                    continue

                # Create CSV
                output_csv = f'/tmp/{shortname}_{sanitize_versionid(versionid)}_filtered_time_gaps.csv'
                with open(output_csv, 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(['gap_begin', 'gap_end'])
                    csvwriter.writerows(time_gaps)
                logger.info(f"Created CSV file {output_csv}")

                # Upload to S3
                s3 = boto3.client('s3')
                bucket_name = os.environ['GAP_REPORT_BUCKET']
                s3_output_key = os.path.basename(output_csv)

                s3.upload_file(output_csv, bucket_name, s3_output_key)
                logger.info(f"Uploaded CSV to s3://{bucket_name}/{s3_output_key}")

                os.remove(output_csv)
                logger.info(f"Deleted temporary CSV file: {output_csv}")

                results.append({'collection_id': collection_id, 'status': 'uploaded', 's3_key': s3_output_key})

            except Exception as e:
                logger.error(f"Error processing collection {collection_id}: {e}")
                results.append({'collection_id': collection_id, 'status': 'error', 'error': str(e)})

    return {
        'statusCode': 200,
        'body': json.dumps(results, indent=2, default=str)
    }