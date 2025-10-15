#!/usr/bin/env python3

import boto3
import csv
import json
import sys
import os
import time
import fcntl
from datetime import datetime, timedelta
from typing import Optional, List

def acquire_lock():
    """
    Acquire an exclusive file lock to prevent multiple instances from running.
    
    Returns:
        file object: The lock file handle (must be kept open to maintain lock)
    
    Exits:
        If another instance is already running
    """
    lock_file_path = '/tmp/lambda_processor.lock'
    
    try:
        lock_file = open(lock_file_path, 'w')
        # Try to get exclusive, non-blocking lock
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(f"{os.getpid()}\n")
        lock_file.flush()
        print(f"Acquired lock (PID: {os.getpid()})")
        return lock_file
    except (IOError, OSError) as e:
        print("Another instance is already running. Exiting.")
        print(f"Lock file: {lock_file_path}")
        sys.exit(1)

def release_lock(lock_file):
    """
    Release the file lock.
    
    Args:
        lock_file: The lock file handle returned by acquire_lock()
    """
    if lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)  # Unlock
            lock_file.close()
            print("Released lock")
        except Exception as e:
            print(f"Warning: Error releasing lock: {e}")

def check_sqs_message_count(sqs_client, queue_name: str) -> int:
    """
    Check the total number of messages in an SQS queue
    
    Returns:
        int: Total number of messages (visible + in-flight)
    """
    try:
        # Get queue URL
        response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        
        # Get queue attributes
        attributes = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible'
            ]
        )
        
        # Extract message counts
        visible_messages = int(attributes['Attributes'].get('ApproximateNumberOfMessages', 0))
        in_flight_messages = int(attributes['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))
        total_messages = visible_messages + in_flight_messages
        
        return total_messages
        
    except Exception as e:
        print(f"Error checking queue '{queue_name}': {e}")
        raise e

def is_queue_empty(sqs_client, queue_name: str) -> bool:
    """
    Check if the SQS queue is empty.
    Wait indefinitely until queue is empty.

    Args:
        sqs_client: boto3 SQS client
        queue_name: SQS queue name

    Returns:
        bool: Always returns True when queue is finally empty
    """
    start_time = time.time()

    print(f"Starting queue monitoring")

    while True:
        try:
            total_messages = check_sqs_message_count(sqs_client, queue_name)

            print(f"Queue status - Total messages: {total_messages}")

            if total_messages == 0:
                print("Queue is empty, proceeding with next invocation")
                return True

            elapsed_minutes = int((time.time() - start_time) / 60)
            elapsed_seconds = int(time.time() - start_time) % 60
            print(f"Queue not empty ({total_messages} messages), waiting... ({elapsed_minutes}m {elapsed_seconds}s elapsed)")

            time.sleep(10)

        except Exception as e:
            print(f"Error checking queue: {str(e)}")
            time.sleep(10)

def invoke_lambda_for_collection(lambda_client, sqs_client, function_name: str, short_name: str, version: str, queue_name: str, tolerance: Optional[int] = None, response_dir: str = "responses"):
    """
    Synchronously invoke the Lambda function for a single collection.
    """
    collection = {
        "short_name": short_name,
        "version": version
    }

    if tolerance is not None:
        collection["tolerance"] = tolerance

    event_payload = {
        "httpMethod": "POST",
        "path": "/init",
        "body": json.dumps({
            "collections": [collection],
            "backfill": "force"
        })
    }

    tolerance_str = f" (tolerance: {tolerance})" if tolerance is not None else ""
    print(f"Processing: {short_name} v{version}{tolerance_str}")

    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Asynchronous invocation
            Payload=json.dumps(event_payload)
        )

        # For async invocation, expect 202
        if response['StatusCode'] == 202:
            print(f"Lambda invoked asynchronously: {short_name} v{version}")

            # Wait for Lambda to spawn processes and populate the queue
            initial_wait = 30  # seconds
            print(f"Waiting {initial_wait} seconds for Lambda to populate queue...")
            time.sleep(initial_wait)

            # Now wait for queue to be empty before proceeding
            print("Monitoring queue until empty before next invocation...")
            is_queue_empty(sqs_client, queue_name)

            # Save response
            response_filename = os.path.join(response_dir, f"response_{short_name}_{version}.json")
            with open(response_filename, 'w') as f:
                json.dump({
                    "async_invocation": True,
                    "status_code": response['StatusCode'],
                    "invoked_at": datetime.now().isoformat()
                }, f, indent=2)

            return True, short_name, version, None
        else:
            error_msg = f"Lambda async invocation failed with status: {response['StatusCode']}"
            print(f"Failed: {short_name} v{version} - {error_msg}")
            return False, short_name, version, error_msg

    except Exception as e:
        error_msg = f"Error invoking Lambda: {str(e)}"
        print(f"Exception: {short_name} v{version} - {error_msg}")
        return False, short_name, version, error_msg

def process_csv_sequential(csv_file: str, function_name: str, queue_name: str):
    """
    Process CSV file and invoke Lambda functions
    """
    collections = []

    # Read CSV file
    try:
        with open(csv_file, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)

            first_row = next(reader, None)
            if first_row and first_row[0].lower() in ['short_name', 'shortname', 'name']:
                print("Skipping header row...")
            else:
                csvfile.seek(0)
                reader = csv.reader(csvfile)

            for row_num, row in enumerate(reader, start=1):
                if not row or len(row) < 2:
                    print(f"Skipping row {row_num}: insufficient data")
                    continue

                short_name = row[0].strip()
                version = row[1].strip()

                tolerance = None
                if len(row) >= 3 and row[2].strip():
                    try:
                        tolerance = int(row[2].strip())
                    except ValueError:
                        print(f"Warning: Invalid tolerance value '{row[2]}' in row {row_num}")

                collections.append((short_name, version, tolerance))

    except Exception as e:
        print(f"Error processing CSV file: {str(e)}")
        return False

    if not collections:
        print("No valid collections found in CSV file")
        return False

    # Create response directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    response_dir = f"responses_{timestamp}"
    os.makedirs(response_dir, exist_ok=True)
    print(f"Created response directory: {response_dir}")

    print(f"Found {len(collections)} collections to process. Processing...")

    # Create AWS clients with increased timeout
    config = boto3.session.Config(
        read_timeout=900,  # 15 minutes
        connect_timeout=60,
        retries={'max_attempts': 3}
    )

    lambda_client = boto3.client('lambda', region_name='us-west-2', config=config)
    sqs_client = boto3.client('sqs', region_name='us-west-2', config=config)

    successful_invocations = 0
    failed_invocations = 0

    # Process each collection sequentially
    for i, (short_name, version, tolerance) in enumerate(collections, 1):
        print(f"\n[{i}/{len(collections)}] ", end="")

        success, _, _, error_msg = invoke_lambda_for_collection(
            lambda_client, sqs_client, function_name, short_name, version, queue_name, tolerance, response_dir
        )

        if success:
            successful_invocations += 1
        else:
            failed_invocations += 1

    return successful_invocations, failed_invocations, response_dir

def main():
    # Acquire lock before doing anything else
    lock_file = acquire_lock()
    
    try:
        if len(sys.argv) != 4:
            print("Usage: python3 lambda_bulk_invoker.py <lambda_function_name> <csv_file> <queue_name>")
            print("Example: python3 lambda_bulk_invoker.py gesdisc-cumulus-prod-gapConfig collections.csv gesdisc-cumulus-prod-gapDetectionIngestQueue")
            sys.exit(1)

        FUNCTION_NAME = sys.argv[1]
        CSV_FILE = sys.argv[2]
        QUEUE_NAME = sys.argv[3]

        print(f"Using SQS Queue: {QUEUE_NAME}")
        print(f"Queue monitoring via direct SQS API calls")

        if not os.path.isfile(CSV_FILE):
            print(f"Error: {CSV_FILE} not found.")
            print("Please create a CSV file with columns: short_name, version, tolerance (optional)")
            sys.exit(1)

        result = process_csv_sequential(CSV_FILE, FUNCTION_NAME, QUEUE_NAME)

        if result is False:
            sys.exit(1)

        successful_invocations, failed_invocations, response_dir = result
        total_invocations = successful_invocations + failed_invocations

        print(f"\n=== Summary ===")
        print(f"Response files saved to: {response_dir}/")
        print(f"Total invocations: {total_invocations}")
        print(f"Successful: {successful_invocations}")
        print(f"Failed: {failed_invocations}")

        if failed_invocations > 0:
            print(f"\nNote: {failed_invocations} collections failed to process")
            sys.exit(1)
        else:
            print(f"\nAll {successful_invocations} collections processed successfully!")
            
    finally:
        # Always release the lock, even if script fails
        release_lock(lock_file)

if __name__ == "__main__":
    main()