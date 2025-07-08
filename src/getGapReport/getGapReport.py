import boto3
import botocore
import json
import os
from utils import validate_environment_variables


# Define size limits
LAMBDA_RESPONSE_LIMIT = 6 * 1024 * 1024  # 6 MB
API_GATEWAY_RESPONSE_LIMIT = 10 * 1024 * 1024  # 10 MB

# Use the smaller of the two limits
MAX_RESPONSE_SIZE = min(LAMBDA_RESPONSE_LIMIT, API_GATEWAY_RESPONSE_LIMIT)


def lambda_handler(event, context):

    validate_environment_variables(['GAP_REPORT_BUCKET'])
    try:
        # Initialize S3 client
        s3_client = boto3.client("s3")
        # Get query parameters
        query_params = event.get("queryStringParameters", {})
        collection_name = query_params.get("short_name")
        collection_version = query_params.get("version", "").replace(
            ".", "_"
        )
        output_format = query_params.get(
            "output", ""
        ).lower()  # Check for output parameter

        if not collection_name or not collection_version:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {
                        "message": "Missing query parameters: short_name or version"
                    }
                ),
            }

        # Construct the S3 object key
        object_key = f"{collection_name}_{collection_version}_filtered_time_gaps.csv"

        # Get the S3 bucket name from environment variables
        bucket_name = os.getenv("GAP_REPORT_BUCKET")
        if not bucket_name:
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "S3 bucket not configured"}),
            }

        # Get metadata to determine file size
        metadata = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        file_size = metadata.get("ContentLength", 0)

        # If file size exceeds limits, generate and return a presigned URL
        if file_size > MAX_RESPONSE_SIZE:
            presigned_url = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": object_key},
                ExpiresIn=3600,  # URL expires in 1 hour
            )
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "File too large for direct download, use the presigned URL",
                        "presigned_url": presigned_url,
                    }
                ),
                "headers": {"Content-Type": "application/json"},
            }

        # Retrieve the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

        # Read the content of the S3 object
        content = response["Body"].read().decode("utf-8")

        # Return the content based on output parameter
        if output_format == "csv":
            return {
                "statusCode": 200,
                "body": content,
                "headers": {
                    "Content-Type": "text/csv",
                    "Content-Disposition": f'attachment; filename="{object_key}"',
                },
            }
        else:
            return {
                "statusCode": 200,
                "body": content,
                "headers": {"Content-Type": "text/plain"},
            }

    except botocore.exceptions.ClientError:
        return {
            "statusCode": 404,
            "body": json.dumps(
                {"message": f"Object {object_key} not found in bucket {bucket_name}"}
            ),
        }
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"message": str(e)})}
