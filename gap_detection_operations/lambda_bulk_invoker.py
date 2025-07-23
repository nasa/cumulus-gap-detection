#!/usr/bin/env python3

import boto3
import csv
import json
import sys
import os
import time
from datetime import datetime
from typing import Optional, List, Tuple

def invoke_lambda_for_collection(lambda_client, function_name: str, short_name: str, version: str, tolerance: Optional[int] = None, response_dir: str = "responses"):
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
            "collections": [collection]
        })
    }

    tolerance_str = f" (tolerance: {tolerance})" if tolerance is not None else ""
    print(f"Processing: {short_name} v{version}{tolerance_str}")

    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(event_payload)
        )

        response_payload = response['Payload'].read()
        response_data = json.loads(response_payload.decode('utf-8'))

        # Save individual response file
        response_filename = os.path.join(response_dir, f"response_{short_name}_{version}.json")
        with open(response_filename, 'w') as f:
            json.dump(response_data, f, indent=2)

        # Check if the Lambda execution was successful
        if response_data.get('statusCode') == 200:
            print(f"Success: {short_name} v{version}")
            return True, short_name, version, None
        else:
            error_msg = f"Lambda returned error: {response_data.get('body', 'Unknown error')}"
            print(f"Failed: {short_name} v{version} - {error_msg}")
            return False, short_name, version, error_msg

    except Exception as e:
        error_msg = f"Error invoking Lambda: {str(e)}"
        print(f"Exception: {short_name} v{version} - {error_msg}")
        return False, short_name, version, error_msg

def process_csv_sequential(csv_file: str, function_name: str):
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

    # Create Lambda client once and reuse
    lambda_client = boto3.client('lambda', region_name='us-west-2')

    successful_invocations = 0
    failed_invocations = 0

    # Process each collection sequentially
    for i, (short_name, version, tolerance) in enumerate(collections, 1):
        print(f"[{i}/{len(collections)}] ", end="")

        success, _, _, error_msg = invoke_lambda_for_collection(
            lambda_client, function_name, short_name, version, tolerance, response_dir
        )

        if success:
            successful_invocations += 1
        else:
            failed_invocations += 1

        time.sleep(0.5)

    return successful_invocations, failed_invocations, response_dir

def main():
    if len(sys.argv) != 3:
        print("Usage: python lambda_bulk_invoker.py <lambda_function_name> <csv_file>")
        print("Example: python lambda_bulk_invoker.py gesdisc-cumulus-prod-gapConfig collections.csv")
        sys.exit(1)
    
    FUNCTION_NAME = sys.argv[1]
    CSV_FILE = sys.argv[2]

    if not os.path.isfile(CSV_FILE):
        print(f"Error: {CSV_FILE} not found.")
        print("Please create a CSV file with columns: short_name, version, tolerance (optional)")
        sys.exit(1)

    result = process_csv_sequential(CSV_FILE, FUNCTION_NAME)

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

if __name__ == "__main__":
    main()
