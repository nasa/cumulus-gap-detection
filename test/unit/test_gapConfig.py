import pytest
import psycopg
from datetime import datetime
from unittest.mock import patch, MagicMock
import io
import os
import json
from pathlib import Path

from src.gapConfig.gapConfig import init_collection, lambda_handler, get_cmr_time, init_migration_stream, save_tolerance_to_dynamodb

from conftest import TEST_COLLECTION_ID, setup_test_data, create_api_test_event

@pytest.fixture(autouse=True)
def set_cmr_env(monkeypatch):
    """Default to UAT env for tests"""
    monkeypatch.setenv("CMR_ENV", "UAT")

@patch("requests.get")
def test_get_cmr_time_success(mock_get):
    """collection has both start and end times"""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "items": [
            {
                "umm": {
                    "TemporalExtents": [
                        {
                            "RangeDateTimes": [
                                {
                                    "BeginningDateTime": "2000-01-01T00:00:00Z",
                                    "EndingDateTime": "2020-01-01T00:00:00Z",
                                }
                            ]
                        }
                    ]
                }
            }
        ]
    }
    mock_get.return_value = mock_response

    start, end = get_cmr_time("TEST___2_0")
    assert start == "2000-01-01T00:00:00Z"
    assert end == "2020-01-01T00:00:00Z"

@pytest.fixture
def mock_cmr_time():
    with patch('src.gapConfig.gapConfig.get_cmr_time', 
               return_value=("2023-01-01T00:00:00Z", "2023-12-31T23:59:59Z")):
        yield

def test_collection_already_exists(setup_test_data, mock_cmr_time):
    """Test init_collection when the collection already exists."""
    from utils import get_db_connection
    collection_name = "TEST_COLLECTION"
    collection_version = "1_0"
    with get_db_connection() as conn:
        result = init_collection(collection_name, collection_version, conn)
    assert result.startswith("Successfully initialized collection") or "already exists" in result

def test_successful_initialization(mock_cmr_time):
    """Test successful initialization of a new collection."""
    from utils import get_db_connection
    collection_name = "NEW_TEST_COLLECTION"
    collection_version = "1_0"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM collections WHERE collection_id = %s", 
                       (f"{collection_name}___{collection_version}",))
    with get_db_connection() as conn:
        result = init_collection(collection_name, collection_version, conn)
    assert result == f"Initialized collection {collection_name}___{collection_version} in table"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM collections WHERE collection_id = %s", 
                       (f"{collection_name}___{collection_version}",))
            assert cur.fetchone() is not None

@pytest.fixture
def simulate_unique_violation():
    """Fixture to simulate a UniqueViolation error during collection insertion."""
    with patch('psycopg.Cursor.execute') as mock_execute:
        def side_effect(query, *args, **kwargs):
            if "INSERT INTO collections" in str(query) and args and "NEW_TEST_COLLECTION" in str(args):
                raise psycopg.errors.UniqueViolation("Duplicate key")
        mock_execute.side_effect = side_effect
        yield

def test_unique_violation_error(mock_cmr_time, simulate_unique_violation):
    """Test handling of UniqueViolation errors."""
    from utils import get_db_connection
    
    # Try to initialize a collection, the mock should cause it to fail
    with get_db_connection() as conn:
        result = init_collection("NEW_TEST_COLLECTION", "1_0", conn)
    assert "initialization failed" in result

def test_unsupported_method(mock_cmr_time):
    event = create_api_test_event(
        'GET',
        '/gap',
        {
            'collections': [
                {'short_name': 'TEST_COLLECTION', 'version': '2.0'},
            ]
        }
    )
    response = lambda_handler(event, None)
    assert response['statusCode'] == 405

@patch("src.gapConfig.gapConfig.save_tolerance_to_dynamodb")
@patch("boto3.client")
def test_handler(mock_boto3_client, mock_save_tolerance, mock_cmr_time=None):
    # Mock Lambda client for migration stream
    mock_lambda = MagicMock()
    payload_bytes = json.dumps({"statusCode": 200, "body": "OK"}).encode()
    mock_lambda.invoke.return_value = {
        "StatusCode": 200,
        "Payload": io.BytesIO(payload_bytes),
    }
    mock_boto3_client.return_value = mock_lambda

    # Test event with tolerance
    event = create_api_test_event(
        "POST",
        "/gap",
        {
            "collections": [
                {
                    "short_name": "TEST_COLLECTION",
                    "version": "2.0",
                    "raw_version": "2.0",
                    "tolerance": 3600,
                }
            ]
        },
    )

    # Patch logging
    mock_logger = MagicMock()
    with patch("src.gapConfig.gapConfig.logger", mock_logger):
        response = lambda_handler(event, None)

    # Assert Lambda response
    assert response["statusCode"] == 200
    assert "Collection initialization complete" in response["body"]

    # Assert save_tolerance_to_dynamodb was called correctly
    mock_save_tolerance.assert_called_once_with(
        "TEST_COLLECTION", "2.0", 3600
    )

    # Assert log message for tolerance update
    mock_logger.info.assert_any_call(
        "Updated tolerance for TEST_COLLECTION v2.0: 3600s"
    )

    # Assert migration stream invocation log
    mock_logger.info.assert_any_call(
        "Collection initialization completed for 1 collection(s)"
    )

@patch("src.gapConfig.gapConfig.save_tolerance_to_dynamodb", side_effect=Exception("Simulated failure"))
@patch("boto3.client")
def test_handler_tolerance_exception(mock_boto3_client, mock_save_tolerance):
    # Mock Lambda client for migration stream
    mock_lambda = MagicMock()
    payload_bytes = json.dumps({"statusCode": 200, "body": "OK"}).encode()
    mock_lambda.invoke.return_value = {
        "StatusCode": 200,
        "Payload": io.BytesIO(payload_bytes),
    }
    mock_boto3_client.return_value = mock_lambda

    # Test event with tolerance
    event = create_api_test_event(
        "POST",
        "/gap",
        {
            "collections": [
                {
                    "short_name": "TEST_COLLECTION",
                    "version": "2.0",
                    "raw_version": "2.0",
                    "tolerance": 3600,
                }
            ]
        },
    )

    # Patch logging
    mock_logger = MagicMock()
    with patch("src.gapConfig.gapConfig.logger", mock_logger):
        response = lambda_handler(event, None)

    # Assert Lambda response still succeeds
    assert response["statusCode"] == 200
    assert "Collection initialization complete" in response["body"]

    # Assert save_tolerance_to_dynamodb was called
    mock_save_tolerance.assert_called_once_with("TEST_COLLECTION", "2.0", 3600)

    # Assert logger.error was called with the expected message
    mock_logger.error.assert_any_call(
        "Error saving tolerance for TEST_COLLECTION___2.0: Simulated failure"
    )

@patch('boto3.client')
def test_missing_params(mock_boto3_client, mock_cmr_time):
    # Create mock lambda client that returns success
    mock_lambda = MagicMock()
    mock_lambda.invoke.return_value = {"StatusCode": 200}
    mock_boto3_client.return_value = mock_lambda
    
    # Uses `name` instead of `short_name`
    event = create_api_test_event(
        'POST',
        '/gap',
        {
            'collections': [
                {'name': 'TEST_COLLECTION', 'version': '2.0'},
            ]
        }
    )
    response = lambda_handler(event, None)
    assert response['statusCode'] == 400

@patch("requests.get")
def test_get_cmr_time_no_end_time(mock_get):
    """If EndingDateTime is missing, should return datetime.max"""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "items": [
            {
                "umm": {
                    "TemporalExtents": [
                        {
                            "RangeDateTimes": [
                                {
                                    "BeginningDateTime": "2010-05-05T00:00:00Z"
                                }
                            ]
                        }
                    ]
                }
            }
        ]
    }
    mock_get.return_value = mock_response

    start, end = get_cmr_time("TEST___1_0")
    assert start == "2010-05-05T00:00:00Z"
    assert end == datetime.max.isoformat()


@patch("requests.get")
def test_get_cmr_time_not_found(mock_get):
    """If CMR returns no items, should raise an exception"""
    mock_response = MagicMock()
    mock_response.json.return_value = {"items": []}
    mock_get.return_value = mock_response

    with pytest.raises(Exception, match="TEST___9_9 not found in CMR"):
        get_cmr_time("TEST___9_9")

def test_cmr_prod_url():
    """Test the production CMR URL line."""
    
    # Mock the environment variable to be "prod"
    with patch.dict(os.environ, {"CMR_ENV": "prod"}):
        # Mock requests.get to avoid actual HTTP call
        with patch('src.gapConfig.gapConfig.requests') as mock_requests:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "items": [{
                    "umm": {
                        "TemporalExtents": [{
                            "RangeDateTimes": [{
                                "BeginningDateTime": "2022-01-01T00:00:00Z"
                            }]
                        }]
                    }
                }]
            }
            mock_requests.get.return_value = mock_response
            
            # Call the function - this will hit the prod URL line
            start, end = get_cmr_time("TEST___001")
            
            # Verify the production URL was used
            mock_requests.get.assert_called_once()
            called_url = mock_requests.get.call_args[0][0]
            assert "https://cmr.earthdata.nasa.gov/search/collections" in called_url
            assert "short_name=TEST&version=001" in called_url

@patch("boto3.client")
def test_init_migration_stream_failure(mock_boto3_client):
    """Simulate migration lambda returning a non-200 payload to trigger failure path"""
    mock_lambda = MagicMock()
    # Payload has statusCode 500 and a body message
    payload_bytes = json.dumps({"statusCode": 500, "body": "Something went wrong"}).encode()
    mock_lambda.invoke.return_value = {
        "StatusCode": 200,
        "Payload": io.BytesIO(payload_bytes),
    }
    mock_boto3_client.return_value = mock_lambda

    with pytest.raises(Exception, match="Collection backfill failed: Something went wrong"):
        init_migration_stream(
            collection_name="TEST_COLLECTION",
            collection_version="2.0",
        )

@patch("boto3.resource")
def test_save_tolerance_to_dynamodb_success(mock_boto3_resource):
    """Test that save_tolerance_to_dynamodb writes to DynamoDB successfully"""
    # Mock table and put_item response
    mock_table = MagicMock()
    mock_table.put_item.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    
    mock_dynamodb = MagicMock()
    mock_dynamodb.Table.return_value = mock_table
    mock_boto3_resource.return_value = mock_dynamodb

    os.environ["TOLERANCE_TABLE_NAME"] = "test_table"

    # Call function
    save_tolerance_to_dynamodb("TEST_COLLECTION", "1.0", 3600)

    mock_dynamodb.Table.assert_called_once_with("test_table")
    mock_table.put_item.assert_called_once_with(
        Item={
            "shortname": "TEST_COLLECTION",
            "versionid": "1.0",
            "granulegap": 3600,
        }
    )

@patch("boto3.resource")
def test_save_tolerance_to_dynamodb_missing_table_env(mock_boto3_resource):
    """Test that function raises ValueError if table name env is missing"""
    if "TOLERANCE_TABLE_NAME" in os.environ:
        del os.environ["TOLERANCE_TABLE_NAME"]

    with pytest.raises(ValueError, match="Missing TOLERANCE_TABLE_NAME environment variable"):
        save_tolerance_to_dynamodb("TEST_COLLECTION", "1.0", 3600)

@patch("boto3.resource")
def test_save_tolerance_to_dynamodb_put_item_exception(mock_boto3_resource):
    """Test that exceptions from DynamoDB put_item are raised"""
    mock_table = MagicMock()
    mock_table.put_item.side_effect = Exception("DynamoDB error")

    mock_dynamodb = MagicMock()
    mock_dynamodb.Table.return_value = mock_table
    mock_boto3_resource.return_value = mock_dynamodb

    os.environ["TOLERANCE_TABLE_NAME"] = "test_table"

    with pytest.raises(Exception, match="DynamoDB error"):
        save_tolerance_to_dynamodb("TEST_COLLECTION", "1.0", 3600)

@patch("src.gapConfig.gapConfig.init_migration_stream", side_effect=Exception("Backfill failed"))
def test_backfill_force_exception(mock_init_migration_stream):
    # Event simulating a POST with a collection, force backfill
    event = create_api_test_event(
        "POST",
        "/gap",
        {
            "collections": [
                {
                    "short_name": "EXISTING_COLLECTION",
                    "version": "1.0",
                    "backfill": "force",
                }
            ]
        },
    )

    # Patch logger
    mock_logger = MagicMock()
    with patch("src.gapConfig.gapConfig.logger", mock_logger):
        response = lambda_handler(event, None)

    collection_id = "EXISTING_COLLECTION___1_0"

    # Lambda should return 500
    assert response["statusCode"] == 500

    # Parse the JSON string body to a dict
    body_dict = json.loads(response["body"])
    assert f"Collection backfill failed for {collection_id}" in body_dict["message"]

    # Logger.error should capture the exception message
    mock_logger.error.assert_any_call(
        "Collection backfill failed for EXISTING_COLLECTION___1_0: Backfill failed"
    )

    # Ensure init_migration_stream was called
    mock_init_migration_stream.assert_called_once_with("EXISTING_COLLECTION", "1.0")

@pytest.mark.asyncio
async def test_force_backfill_success():
    """Test force backfill behavior - success path."""
    event = {
        "httpMethod": "POST",
        "path": "/collections",
        "body": json.dumps({
            "collections": [{"short_name": "TEST", "version": "001"}],
            "backfill": "force"
        })
    }
    
    with patch('src.gapConfig.gapConfig.get_db_connection') as mock_conn, \
         patch('src.gapConfig.gapConfig.check_collections') as mock_check, \
         patch('src.gapConfig.gapConfig.init_migration_stream') as mock_migration:
        
        # Collection already exists
        mock_check.return_value = ["TEST___001"]
        mock_migration.return_value = {"status": "success"}
        
        result = lambda_handler(event, {})
        
        # Verify force backfill was called
        mock_migration.assert_called_once_with("TEST", "001")
        assert result["statusCode"] == 200


@pytest.mark.asyncio
async def test_force_backfill_failure():
    """Test force backfill behavior - failure path."""
    event = {
        "httpMethod": "POST", 
        "path": "/collections",
        "body": json.dumps({
            "collections": [{"short_name": "TEST", "version": "001"}],
            "backfill": "force"
        })
    }
    
    with patch('src.gapConfig.gapConfig.get_db_connection') as mock_conn, \
         patch('src.gapConfig.gapConfig.check_collections') as mock_check, \
         patch('src.gapConfig.gapConfig.init_migration_stream') as mock_migration:
        
        # Collection already exists
        mock_check.return_value = ["TEST___001"]
        # Migration fails
        mock_migration.side_effect = Exception("Migration failed")
        
        result = lambda_handler(event, {})
        
        # Verify error response
        assert result["statusCode"] == 500
        assert "Collection backfill failed" in json.loads(result["body"])["message"]


@pytest.mark.asyncio
async def test_skip_existing_collection():
    """Test skipping existing collection (else branch)."""
    event = {
        "httpMethod": "POST",
        "path": "/collections", 
        "body": json.dumps({
            "collections": [{"short_name": "TEST", "version": "001"}],
            "backfill": ""  # Not "force"
        })
    }
    
    with patch('src.gapConfig.gapConfig.get_db_connection') as mock_conn, \
         patch('src.gapConfig.gapConfig.check_collections') as mock_check, \
         patch('src.gapConfig.gapConfig.init_migration_stream') as mock_migration:
        
        # Collection already exists
        mock_check.return_value = ["TEST___001"]
        
        result = lambda_handler(event, {})
        
        # Verify migration was NOT called (skipped)
        mock_migration.assert_not_called()
        assert result["statusCode"] == 200


def test_unexpected_exception_handling():
    """Test the main exception handler block."""
    event = {
        "httpMethod": "POST",
        "path": "/collections",
        "body": json.dumps({
            "collections": [{"short_name": "TEST", "version": "001"}]
        })
    }
    
    # Force an exception after parse_event succeeds but before the main logic
    with patch('src.gapConfig.gapConfig.get_db_connection') as mock_db:
        mock_db.side_effect = Exception("Database connection failed unexpectedly")
        
        result = lambda_handler(event, {})
        
        # Verify the exception handler response
        assert result["statusCode"] == 500
        assert json.loads(result["body"])["message"] == "Unexpected error occurred"