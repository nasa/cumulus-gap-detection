import os
import sys
import json
import boto3
from botocore.exceptions import ClientError
import pytest
from moto import mock_aws
from unittest.mock import patch, MagicMock

from src.getGapReport.getGapReport import lambda_handler, MAX_RESPONSE_SIZE

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["GapReportBucket"] = "test-bucket"
    os.environ["BUCKET_NAME"] = "test-bucket"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")



def test_missing_query_parameters():
    event = {"queryStringParameters": {}}
    response = lambda_handler(event, None)
    assert response["statusCode"] == 400
    assert "Missing query parameters" in json.loads(response["body"])["message"]

def test_missing_bucket_name():
    event = {
        "queryStringParameters": {
            "short_name": "test",
            "version": "1.0",
        }
    }
    with patch.dict(os.environ, {"GAP_REPORT_BUCKET": ""}):
        response = lambda_handler(event, None)
    assert response["statusCode"] == 500
    assert "S3 bucket not configured" in json.loads(response["body"])["message"]

def test_large_file_presigned_url(s3):
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="test_1_0_filtered_time_gaps.csv",
        Body="x" * (MAX_RESPONSE_SIZE + 1),
    )

    event = {
        "queryStringParameters": {
            "short_name": "test",
            "version": "1.0",
        }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert "presigned_url" in body
    assert "File too large for direct download" in body["message"]

def test_small_file_csv_output(s3):
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="test_1_0_filtered_time_gaps.csv", Body="test,data")

    event = {
        "queryStringParameters": {
            "short_name": "test",
            "version": "1.0",
            "output": "csv",
        }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == 200
    assert response["body"] == "test,data"
    assert response["headers"]["Content-Type"] == "text/csv"
    assert "Content-Disposition" in response["headers"]

def test_small_file_default_output(s3):
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="test_1_0_filtered_time_gaps.csv", Body="test,data")

    event = {
        "queryStringParameters": {
            "short_name": "test",
            "version": "1.0",
        }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == 200
    assert response["body"] == "test,data"
    assert response["headers"]["Content-Type"] == "text/plain"

def test_file_doesnot_exist(s3):
    s3.create_bucket(Bucket="test-bucket")

    event = {
        "queryStringParameters": {
            "short_name": "test",
            "version": "1.0",
        }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == 404
    assert json.loads(response["body"]) == {
        "message": "Object test_1_0_filtered_time_gaps.csv not found in bucket test-bucket"
    }

def test_version_dot_replacement(s3):
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket", Key="test_1_0_1_time_gaps.csv", Body="test,data"
    )
    event = {
        "queryStringParameters": {
            "short_name": "test",
            "version": "1.0.1",
        }
    }

    with patch("boto3.client") as mock_client:
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        mock_s3.head_object.return_value = {"ContentLength": 100}
        mock_s3.get_object.return_value = {"Body": MagicMock(read=lambda: b"test,data")}

        response = lambda_handler(event, None)

    assert response["statusCode"] == 200
    assert response["body"] == "test,data"

def test_other_exception(s3):
    event = "WRONG EVENT"

    response = lambda_handler(event, None)
    assert response["statusCode"] == 500
    assert json.loads(response["body"]) == {
        "message": "'str' object has no attribute 'get'"
    }
