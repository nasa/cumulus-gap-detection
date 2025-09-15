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
    os.environ["GAP_REPORT_BUCKET"] = "test-bucket"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


def test_missing_query_parameters():
    with patch.dict(os.environ, {"GAP_REPORT_BUCKET": "test-bucket"}):
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
        Bucket="test-bucket", Key="test_1_0_1_filtered_time_gaps.csv", Body="test,data"
    )
    event = {
        "queryStringParameters": {
            "short_name": "test",
            "version": "1.0.1",
        }
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == 200
    assert response["body"] == "test,data"

def test_other_exception():
    with patch.dict(os.environ, {"GAP_REPORT_BUCKET": "test-bucket"}):
        event = "WRONG EVENT"

        with pytest.raises(UnboundLocalError) as exc_info:
            lambda_handler(event, None)
        
        assert "cannot access local variable 'collection_name'" in str(exc_info.value)

def test_nosuchkey_error_logging():
    """Test that NoSuchKey errors are properly logged and handled"""
    event = {
        "queryStringParameters": {
            "short_name": "test",
            "version": "1.0",
        }
    }
    
    with patch.dict(os.environ, {"GAP_REPORT_BUCKET": "test-bucket"}):
        with patch("boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3
            
            # Mock head_object to raise NoSuchKey error
            mock_s3.head_object.side_effect = ClientError(
                error_response={'Error': {'Code': 'NoSuchKey', 'Message': 'The specified key does not exist.'}},
                operation_name='HeadObject'
            )
            
            with patch('src.getGapReport.getGapReport.logger') as mock_logger:
                response = lambda_handler(event, None)
                
                # Verify the warning log was called with the correct message
                mock_logger.warning.assert_called_once_with("Gap report not found: test v1_0")
                
                # Verify the response
                assert response["statusCode"] == 404
                assert json.loads(response["body"]) == {
                    "message": "Object test_1_0_filtered_time_gaps.csv not found in bucket test-bucket"
                }

def test_general_exception_with_defined_variables():
    """Test the general exception handler when collection_name and collection_version are defined"""
    event = {
        "queryStringParameters": {
            "short_name": "test",
            "version": "1.0",
        }
    }
    
    with patch.dict(os.environ, {"GAP_REPORT_BUCKET": "test-bucket"}):
        with patch("boto3.client") as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3
            
            # Mock head_object to succeed (so variables get defined)
            mock_s3.head_object.return_value = {"ContentLength": 100}
            
            # Mock get_object to raise a non-ClientError exception
            mock_s3.get_object.side_effect = ValueError("Unexpected encoding error")
            
            with patch('src.getGapReport.getGapReport.logger') as mock_logger:
                response = lambda_handler(event, None)
                
                # Verify the error log was called with the correct message
                mock_logger.error.assert_called_once_with(
                    "Unexpected error retrieving gap report for test v1_0: Unexpected encoding error"
                )
                
                # Verify the response
                assert response["statusCode"] == 500
                assert json.loads(response["body"]) == {
                    "message": "Unexpected encoding error"
                }