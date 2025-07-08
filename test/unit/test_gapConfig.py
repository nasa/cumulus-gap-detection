import pytest
import psycopg
from datetime import datetime
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

from src.gapConfig.gapConfig import init_collection, lambda_handler

from conftest import TEST_COLLECTION_ID, setup_test_data, create_api_test_event

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

@patch('boto3.client')
def test_handler(mock_boto3_client, mock_cmr_time):
    # Create mock lambda client that returns success
    mock_lambda = MagicMock()
    mock_lambda.invoke.return_value = {"StatusCode": 200}
    mock_boto3_client.return_value = mock_lambda
    
    event = create_api_test_event(
        'POST',
        '/gap',
        {
            'collections': [
                {'short_name': 'TEST_COLLECTION', 'version': '2.0'},
            ]
        }
    )
    response = lambda_handler(event, None)
    assert response['statusCode'] == 200

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
