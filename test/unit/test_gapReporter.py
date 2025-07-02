import os
import pytest
from unittest.mock import patch, MagicMock
import boto3
import json
from botocore.exceptions import ClientError
from decimal import Decimal
from moto import mock_aws
from src.gapReporter.gapReporter import lambda_handler, get_granule_gap, fetch_time_gaps, sanitize_versionid
from utils import get_db_connection, validate_environment_variables

@pytest.fixture
def mock_env_vars():
    """Set up environment variables for the Lambda function."""
    os.environ['SECRET_NAME'] = 'test_secret_name'
    os.environ['TOLERANCE_TABLE'] = 'test_tolerance_table'
    os.environ['S3_BUCKET'] = 'test-bucket'
    os.environ['RDS_SECRET'] = 'my-secret-id'

# Test sanitize_versionid function
def test_sanitize_versionid():
    result = sanitize_versionid('1.2.3')
    assert result == '1_2_3'

# Test get_granule_gap with DynamoDB mock
@mock_aws
def test_get_granule_gap(mock_env_vars, mocker):
    # Mock DynamoDB response
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.create_table(
        TableName='test_tolerance_table',
        KeySchema=[
            {'AttributeName': 'shortname', 'KeyType': 'HASH'},
            {'AttributeName': 'versionid', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'shortname', 'AttributeType': 'S'},
            {'AttributeName': 'versionid', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
    )

    # Insert a test item
    table.put_item(
        Item={
            'shortname': 'collection1',
            'versionid': 'v1',
            'granulegap': '30'
        }
    )

    result = get_granule_gap('collection1', 'v1')
    assert result == 30

    # Test when no granulegap is found
    result = get_granule_gap('collection1', 'v2')
    assert result == 0

@mock_aws
def test_get_granule_gap_client_error(mock_env_vars, mocker):
    # Mock DynamoDB response to simulate a failure
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.create_table(
        TableName='test_tolerance_table',
        KeySchema=[
            {'AttributeName': 'shortname', 'KeyType': 'HASH'},
            {'AttributeName': 'versionid', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'shortname', 'AttributeType': 'S'},
            {'AttributeName': 'versionid', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
    )

    # Simulate a DynamoDB exception during retrieval
    with patch('src.gapReporter.gapReporter.boto3.resource') as mock_dynamodb:
        mock_dynamodb.side_effect = ClientError(
            {'Error': {'Code': 'InternalError', 'Message': 'Internal server error'}},
            'GetItem'
        )

        with pytest.raises(ClientError):
            get_granule_gap('collection1', 'v1')

# Test fetch_time_gaps with mocked cursor
def test_fetch_time_gaps(mock_env_vars, mocker):
    # Create a mock cursor for testing
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [('2025-01-01 00:00:00', '2025-01-01 01:00:00')]

    # Call fetch_time_gaps function
    result = fetch_time_gaps('collection1', 'v1', 30, mock_cursor)
    
    assert result == [('2025-01-01 00:00:00', '2025-01-01 01:00:00')]

# Test fetch_time_gaps raising Exception
def test_fetch_time_gaps_exception(mock_env_vars, mocker):
    # Create a mock cursor that raises an exception
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = Exception("Database error")

    with pytest.raises(Exception):
        fetch_time_gaps('collection1', 'v1', 30, mock_cursor)


# Test lambda_handler with mock S3 and DynamoDB
@mock_aws
def test_lambda_handler(mock_env_vars, mocker):
    # Set up mock event
    event = {
        'shortname': 'collection1',
        'version': 'v1'
    }

    # Mock DynamoDB and S3 services
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.create_table(
        TableName='test_tolerance_table',
        KeySchema=[
            {'AttributeName': 'shortname', 'KeyType': 'HASH'},
            {'AttributeName': 'versionid', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'shortname', 'AttributeType': 'S'},
            {'AttributeName': 'versionid', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
    )

    # Insert test data into DynamoDB
    table.put_item(
        Item={
            'shortname': 'collection1',
            'versionid': 'v1',
            'granulegap': '30'
        }
    )

    # Create a mock context manager for database connection
    class MockDBContext:
        def __init__(self):
            self.conn = MagicMock()
            self.cursor = MagicMock()
            self.cursor.fetchall.return_value = [('2025-01-01 00:00:00', '2025-01-01 01:00:00')]
            self.conn.cursor.return_value = self.cursor
            
        def __enter__(self):
            return self.conn
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    
    # Patch get_db_connection to return our mock context
    mocker.patch('src.gapReporter.gapReporter.get_db_connection', return_value=MockDBContext())

    # Mock S3 upload_file method
    mock_s3_client = MagicMock()
    mock_s3_client.upload_file.return_value = None
    mocker.patch('boto3.client', return_value=mock_s3_client)

    # Call the lambda handler
    response = lambda_handler(event, None)

    assert response['statusCode'] == 200
    assert 'Filtered time gaps CSV file uploaded successfully' in response['body']


# Test S3 upload file exception
@mock_aws
def test_lambda_handler_s3_upload_exception(mock_env_vars, mocker):
    # Set up mock event
    event = {
        'shortname': 'collection1',
        'version': 'v1'
    }

    # Mock DynamoDB and S3 services
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.create_table(
        TableName='test_tolerance_table',
        KeySchema=[
            {'AttributeName': 'shortname', 'KeyType': 'HASH'},
            {'AttributeName': 'versionid', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'shortname', 'AttributeType': 'S'},
            {'AttributeName': 'versionid', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
    )

    # Insert test data into DynamoDB
    table.put_item(
        Item={
            'shortname': 'collection1',
            'versionid': 'v1',
            'granulegap': '30'
        }
    )

    # Create mock context manager
    class MockDBContext:
        def __init__(self):
            self.conn = MagicMock()
            self.cursor = MagicMock()
            self.cursor.fetchall.return_value = [('2025-01-01 00:00:00', '2025-01-01 01:00:00')]
            self.conn.cursor.return_value = self.cursor
            
        def __enter__(self):
            return self.conn
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    
    # Patch get_db_connection to return our mock context
    mocker.patch('src.gapReporter.gapReporter.get_db_connection', return_value=MockDBContext())

    # Mock S3 upload_file method to raise an exception
    mock_s3_client = MagicMock()
    mock_s3_client.upload_file.side_effect = Exception("S3 upload failed")
    mocker.patch('boto3.client', return_value=mock_s3_client)

    # Call the lambda handler
    response = lambda_handler(event, None)

    assert response is not None
    assert 'statusCode' in response
    assert response['statusCode'] == 500  # Should handle the exception and return a 500 status
    assert "Failed to upload CSV file to S3" in response['body']

@mock_aws
def test_lambda_handler_fetch_time_gaps_exception(mock_env_vars, mocker):
    # Set up mock event
    event = {
        'shortname': 'collection1',
        'version': 'v1'
    }

    # Mock DynamoDB and S3 services
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.create_table(
        TableName='test_tolerance_table',
        KeySchema=[
            {'AttributeName': 'shortname', 'KeyType': 'HASH'},
            {'AttributeName': 'versionid', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'shortname', 'AttributeType': 'S'},
            {'AttributeName': 'versionid', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
    )

    # Insert test data into DynamoDB
    table.put_item(
        Item={
            'shortname': 'collection1',
            'versionid': 'v1',
            'granulegap': '30'
        }
    )

    # Create mock context manager with a cursor that raises an exception
    class MockDBContext:
        def __init__(self):
            self.conn = MagicMock()
            self.cursor = MagicMock()
            # Simulate a database error by making the `fetchall` method raise an exception
            self.cursor.fetchall.side_effect = Exception("Database query failed")
            self.conn.cursor.return_value = self.cursor
            self.cursor.__enter__ = MagicMock(return_value=self.cursor)
            self.cursor.__exit__ = MagicMock(return_value=None)
            
        def __enter__(self):
            return self.conn
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    
    # Patch get_db_connection to return our mock context
    mocker.patch('src.gapReporter.gapReporter.get_db_connection', return_value=MockDBContext())

    # Mock logger to capture the log messages
    mock_logger = MagicMock()
    mocker.patch('src.gapReporter.gapReporter.logger', mock_logger)

    # Call the lambda handler
    response = lambda_handler(event, None)

    # Assert that the response has a status code of 500
    assert response['statusCode'] == 500
    assert 'Failed to fetch time gaps' in response['body']
    
    # Assert that the exception is logged properly
    mock_logger.error.assert_called_with('Failed to fetch time gaps: Database query failed')

@mock_aws
def test_lambda_handler_no_time_gaps(mock_env_vars, mocker):
    # Set up mock event
    event = {
        'shortname': 'collection1',
        'version': 'v1'
    }

    # Mock DynamoDB and S3 services
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.create_table(
        TableName='test_tolerance_table',
        KeySchema=[
            {'AttributeName': 'shortname', 'KeyType': 'HASH'},
            {'AttributeName': 'versionid', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'shortname', 'AttributeType': 'S'},
            {'AttributeName': 'versionid', 'AttributeType': 'S'}
        ],
        ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
    )

    # Insert test data into DynamoDB
    table.put_item(
        Item={
            'shortname': 'collection1',
            'versionid': 'v1',
            'granulegap': '30'
        }
    )

    # Create mock context manager with a cursor that returns an empty list
    class MockDBContext:
        def __init__(self):
            self.conn = MagicMock()
            self.cursor = MagicMock()
            # Simulate a query returning no time gaps (empty list)
            self.cursor.fetchall.return_value = []  # No time gaps exceeding the threshold
            self.conn.cursor.return_value = self.cursor
            self.cursor.__enter__ = MagicMock(return_value=self.cursor)
            self.cursor.__exit__ = MagicMock(return_value=None)
 
            
        def __enter__(self):
            return self.conn
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    
    # Patch get_db_connection to return our mock context
    mocker.patch('src.gapReporter.gapReporter.get_db_connection', return_value=MockDBContext())

    # Mock logger to capture the log messages
    mock_logger = MagicMock()
    mocker.patch('src.gapReporter.gapReporter.logger', mock_logger)

    # Call the lambda handler
    response = lambda_handler(event, None)

    # Assert that the response has a status code of 200
    assert response['statusCode'] == 200
    assert 'No qualifying time gaps found.' in response['body']
    
    # Assert that the "no time gaps exceed the threshold" message is logged
    mock_logger.info.assert_called_with('No time gaps exceed the granulegap threshold. No file will be uploaded.')


def test_get_granule_gap_client_error():
    with patch("boto3.resource") as mock_resource:
        mock_table = MagicMock()
        mock_resource.return_value.Table.return_value = mock_table
        
        # Simulate ClientError
        mock_table.get_item.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Table not found."}},
            "GetItem"
        )
        
        with pytest.raises(ClientError) as exc_info:
            get_granule_gap("test_shortname", "test_versionid")
        
        assert "Table not found." in str(exc_info.value)
