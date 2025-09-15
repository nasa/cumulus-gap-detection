import os
import pytest
from unittest.mock import patch, MagicMock
import boto3
import json
from botocore.exceptions import ClientError
from decimal import Decimal
from moto import mock_aws
from src.gapReporter.gapReporter import lambda_handler, parse_collection_id, check_collections
from utils import get_granule_gap, fetch_time_gaps, sanitize_versionid

@pytest.fixture
def mock_env_vars():
    """Set up environment variables for the Lambda function."""
    os.environ['AWS_REGION'] = 'us-west-2'
    os.environ['TOLERANCE_TABLE'] = 'test_tolerance_table'
    os.environ['GAP_REPORT_BUCKET'] = 'test-bucket'
    os.environ['RDS_SECRET'] = 'my-secret-id'
    os.environ['RDS_PROXY_HOST'] = 'test-host'

# Test sanitize_versionid function
def test_sanitize_versionid():
    result = sanitize_versionid('1.2.3')
    assert result == '1_2_3'

def test_parse_collection_id():
    """Test the parse_collection_id function"""
    shortname, versionid = parse_collection_id('MODIS_AQUA___1_0')
    assert shortname == 'MODIS_AQUA'
    assert versionid == '1.0'
    
    # Test with multiple underscores in shortname
    shortname, versionid = parse_collection_id('MODIS_AQUA_L2___2_1_3')
    assert shortname == 'MODIS_AQUA_L2'
    assert versionid == '2.1.3'
    
    # Test invalid format
    with pytest.raises(ValueError, match="Invalid collection_id format"):
        parse_collection_id('INVALID_FORMAT')

# Test get_granule_gap with DynamoDB mock
@mock_aws
def test_get_granule_gap(mock_env_vars):
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

def test_get_granule_gap_client_error(mock_env_vars):
    """Test get_granule_gap with ClientError"""
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

def test_fetch_time_gaps(mock_env_vars):
    '''Test fetch_time_gaps normal'''
    mock_cursor = MagicMock()
    from datetime import datetime
    mock_cursor.fetchall.return_value = [
        (datetime(2025, 1, 1, 0, 0, 0), datetime(2025, 1, 1, 1, 0, 0))
    ]

    # Call fetch_time_gaps function
    result = fetch_time_gaps('collection1', 'v1', 30, mock_cursor)
    
    # Should return datetime tuples
    assert len(result) == 1
    assert result[0][0] == datetime(2025, 1, 1, 0, 0, 0)
    assert result[0][1] == datetime(2025, 1, 1, 1, 0, 0)

def test_fetch_time_gaps_exception(mock_env_vars):
    '''Test fetch_time_gaps raising Exception'''
    # Create a mock cursor that raises an exception
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = Exception("Database error")

    with pytest.raises(Exception):
        fetch_time_gaps('collection1', 'v1', 30, mock_cursor)

def test_check_collections(mock_env_vars):
    """Test the check_collections function"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [('MODIS_AQUA___1_0',), ('VIIRS_NPP___2_1',)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    result = check_collections(mock_conn)
    assert result == ['MODIS_AQUA___1_0', 'VIIRS_NPP___2_1']


@mock_aws
def test_lambda_handler_success(mock_env_vars):
    '''Test lambda_handler with successful processing'''
    # mock event
    event = {}

    # Mock DynamoDB
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
            'shortname': 'MODIS_AQUA',
            'versionid': '1.0',
            'granulegap': '30'
        }
    )

    # Mock S3
    s3 = boto3.client('s3', region_name='us-west-2')
    s3.create_bucket(
        Bucket='test-bucket',
        CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
    )

    # Create a mock context manager for database connection
    class MockDBContext:
        def __init__(self):
            self.conn = MagicMock()
            self.cursor = MagicMock()
            # Mock collections query
            self.cursor.fetchall.side_effect = [
                [('MODIS_AQUA___1_0',)],  # check_collections result
                [(datetime(2025, 1, 1, 0, 0, 0), datetime(2025, 1, 1, 1, 0, 0))]  # fetch_time_gaps result
            ]
            self.conn.cursor.return_value.__enter__.return_value = self.cursor
            
        def __enter__(self):
            return self.conn
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    
    from datetime import datetime
    
    # Patch get_db_connection to return our mock context
    with patch('src.gapReporter.gapReporter.get_db_connection', return_value=MockDBContext()):
        # Call the lambda handler
        response = lambda_handler(event, None)

    assert response['statusCode'] == 200
    # Parse the JSON response body
    results = json.loads(response['body'])
    assert len(results) == 1
    assert results[0]['collection_id'] == 'MODIS_AQUA___1_0'
    assert results[0]['status'] == 'uploaded'

# Test lambda_handler with no collections
def test_lambda_handler_no_collections(mock_env_vars):
    event = {}

    class MockDBContext:
        def __init__(self):
            self.conn = MagicMock()
            self.cursor = MagicMock()
            # Return empty list for collections
            self.cursor.fetchall.return_value = []
            self.conn.cursor.return_value.__enter__.return_value = self.cursor
            
        def __enter__(self):
            return self.conn
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    
    with patch('src.gapReporter.gapReporter.get_db_connection', return_value=MockDBContext()):
        response = lambda_handler(event, None)

    assert response['statusCode'] == 200
    results = json.loads(response['body'])
    assert results == []  # No collections processed

# Test lambda_handler with database connection failure
def test_lambda_handler_db_connection_failure(mock_env_vars):
    event = {}
    
    # Mock database connection to raise an exception
    with patch('src.gapReporter.gapReporter.get_db_connection') as mock_get_db_connection:
        mock_get_db_connection.side_effect = Exception("Database connection failed")
        
        # The lambda doesn't catch exceptions from get_db_connection() itself
        # It only catches exceptions from operations inside the connection context
        with pytest.raises(Exception, match="Database connection failed"):
            lambda_handler(event, None)

# Test lambda_handler with check_collections failure
def test_lambda_handler_check_collections_failure(mock_env_vars):
    """Test the exception handling for check_collections failure"""
    event = {}
    
    class MockDBContext:
        def __init__(self):
            self.conn = MagicMock()
            self.cursor = MagicMock()
            # Make check_collections fail by having the cursor raise an exception
            self.cursor.fetchall.side_effect = Exception("Database query failed")
            self.conn.cursor.return_value.__enter__.return_value = self.cursor
            
        def __enter__(self):
            return self.conn
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    
    with patch('src.gapReporter.gapReporter.get_db_connection', return_value=MockDBContext()), \
         patch('src.gapReporter.gapReporter.logger') as mock_logger:
        
        response = lambda_handler(event, None)
        
        # Should catch the exception and return 500 error
        assert response['statusCode'] == 500
        assert response['body'] == 'Failed to fetch collections'
        
        # Should log the error
        mock_logger.error.assert_called_with('Failed to fetch collections: Database query failed')

# Test lambda_handler with S3 upload failure
@mock_aws
def test_lambda_handler_s3_upload_failure(mock_env_vars):
    event = {}

    # Mock DynamoDB
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

    table.put_item(
        Item={
            'shortname': 'MODIS_AQUA',
            'versionid': '1.0',
            'granulegap': '30'
        }
    )

    class MockDBContext:
        def __init__(self):
            self.conn = MagicMock()
            self.cursor = MagicMock()
            self.cursor.fetchall.side_effect = [
                [('MODIS_AQUA___1_0',)],  # check_collections result
                [(datetime(2025, 1, 1, 0, 0, 0), datetime(2025, 1, 1, 1, 0, 0))]  # fetch_time_gaps result
            ]
            self.conn.cursor.return_value.__enter__.return_value = self.cursor
            
        def __enter__(self):
            return self.conn
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    
    from datetime import datetime
    
    # Mock S3 upload to fail
    with patch('src.gapReporter.gapReporter.get_db_connection', return_value=MockDBContext()), \
         patch('boto3.client') as mock_boto_client:
        
        mock_s3 = MagicMock()
        mock_s3.upload_file.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchBucket', 'Message': 'Bucket does not exist'}},
            'PutObject'
        )
        mock_boto_client.return_value = mock_s3
        
        response = lambda_handler(event, None)

    assert response['statusCode'] == 200  # Lambda still succeeds but reports errors
    results = json.loads(response['body'])
    assert len(results) == 1
    assert results[0]['status'] == 'upload_failed'
    assert 'NoSuchBucket' in results[0]['error']

@mock_aws
def test_lambda_handler_no_time_gaps(mock_env_vars):
    '''Test lambda_handler with no time gaps'''
    event = {}

    # Mock DynamoDB
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

    table.put_item(
        Item={
            'shortname': 'MODIS_AQUA',
            'versionid': '1.0',
            'granulegap': '30'
        }
    )

    class MockDBContext:
        def __init__(self):
            self.conn = MagicMock()
            self.cursor = MagicMock()
            self.cursor.fetchall.side_effect = [
                [('MODIS_AQUA___1_0',)],  # check_collections result
                []  # fetch_time_gaps result - no gaps
            ]
            self.conn.cursor.return_value.__enter__.return_value = self.cursor
            
        def __enter__(self):
            return self.conn
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    
    with patch('src.gapReporter.gapReporter.get_db_connection', return_value=MockDBContext()):
        response = lambda_handler(event, None)

    assert response['statusCode'] == 200
    results = json.loads(response['body'])
    assert len(results) == 1
    assert results[0]['status'] == 'no gaps'

@mock_aws
def test_lambda_handler_invalid_collection_format(mock_env_vars):
    """Test ValueError handling for invalid collection ID format"""
    event = {}

    class MockDBContext:
        def __init__(self):
            self.conn = MagicMock()
            self.cursor = MagicMock()
            # Return a collection with invalid format (no ___)
            self.cursor.fetchall.return_value = [('INVALID_COLLECTION_FORMAT',)]
            self.conn.cursor.return_value.__enter__.return_value = self.cursor
            
        def __enter__(self):
            return self.conn
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    
    with patch('src.gapReporter.gapReporter.get_db_connection', return_value=MockDBContext()), \
         patch('src.gapReporter.gapReporter.logger') as mock_logger:
        
        response = lambda_handler(event, None)

    assert response['statusCode'] == 200
    results = json.loads(response['body'])
    assert len(results) == 1
    assert results[0]['collection_id'] == 'INVALID_COLLECTION_FORMAT'
    assert results[0]['status'] == 'invalid_format'
    assert 'Invalid collection_id format' in results[0]['error']
    
    # log the warning
    mock_logger.warning.assert_called_with('Invalid collection ID format: INVALID_COLLECTION_FORMAT')

@mock_aws
def test_lambda_handler_collection_processing_exception(mock_env_vars):
    """Test general Exception handling during collection processing"""
    event = {}

    # Mock DynamoDB to cause an exception during get_granule_gap
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

    class MockDBContext:
        def __init__(self):
            self.conn = MagicMock()
            self.cursor = MagicMock()
            # Return a valid collection
            self.cursor.fetchall.return_value = [('MODIS_AQUA___1_0',)]
            self.conn.cursor.return_value.__enter__.return_value = self.cursor
            
        def __enter__(self):
            return self.conn
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    
    # Mock get_granule_gap to raise an exception
    with patch('src.gapReporter.gapReporter.get_db_connection', return_value=MockDBContext()), \
         patch('src.gapReporter.gapReporter.get_granule_gap', side_effect=Exception("DynamoDB connection failed")), \
         patch('src.gapReporter.gapReporter.logger') as mock_logger:
        
        response = lambda_handler(event, None)

    assert response['statusCode'] == 200
    results = json.loads(response['body'])
    assert len(results) == 1
    assert results[0]['collection_id'] == 'MODIS_AQUA___1_0'
    assert results[0]['status'] == 'error'
    assert 'DynamoDB connection failed' in results[0]['error']
    
    # Should log the error
    mock_logger.error.assert_called_with('Failed to process collection MODIS_AQUA___1_0: DynamoDB connection failed')