import json
import pytest
import logging
from unittest.mock import patch
from utils import get_db_connection

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from conftest import (
    TEST_COLLECTION_ID, 
    DEFAULT_DATE, 
    DEFAULT_END_DATE, 
    create_api_test_event, 
    seed_test_data,
    get_reason,
    insert_reason
)
from src.knownGap.knownGap import lambda_handler

def get_record_with_reason(collection_id, start_ts, end_ts):
    """Get a specific record from the gaps table with its reason."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Get gap record
            cur.execute("""
                SELECT gap_id, collection_id, start_ts, end_ts 
                FROM gaps 
                WHERE collection_id = %s
                AND start_ts = %s
                AND end_ts = %s
            """, (collection_id, start_ts, end_ts))
            gap_record = cur.fetchone()
            
            if not gap_record:
                return None
            
            # Get reason if it exists
            cur.execute("""
                SELECT reason 
                FROM reasons 
                WHERE collection_id = %s
                AND start_ts = %s
                AND end_ts = %s
            """, (collection_id, start_ts, end_ts))
            reason_record = cur.fetchone()
            
            # Return gap record with reason appended
            return gap_record + (reason_record[0] if reason_record else None,)

def test_add_reason():
    """Test adding reasons via POST method"""
    # Clear any existing test data
    seed_test_data([])
    
    # Prepare POST request body according to the API format
    event = create_api_test_event(
        'POST',
        '/gap',
        {
            'reasons': [
                {
                    'shortname': 'TEST_COLLECTION',
                    'version': '1.0',
                    'start_ts': '2023-01-01T00:00:00Z',
                    'end_ts': '2023-01-10T00:00:00Z',
                    'reason': 'New reason added'
                }
            ]
        }
    )
    
    response = lambda_handler(event, None)
    print(f"Response: {response}")  # Debug output
    
    assert response['statusCode'] == 201
    response_body = json.loads(response['body'])
    assert 'Successfully added 1 reasons' in response_body['message']
    
    # Verify the reason was added to the database
    reason = get_reason(TEST_COLLECTION_ID, '2023-01-01 00:00:00', '2023-01-10 00:00:00')
    assert reason == 'New reason added'

def test_add_multiple_reasons():
    """Test adding multiple reasons in one POST request"""
    # Clear any existing test data
    seed_test_data([])
    
    event = create_api_test_event(
        'POST',
        '/gap',
        {
            'reasons': [
                {
                    'shortname': 'TEST_COLLECTION',
                    'version': '1.0',
                    'start_ts': '2023-01-01T00:00:00Z',
                    'end_ts': '2023-01-10T00:00:00Z',
                    'reason': 'First reason'
                },
                {
                    'shortname': 'TEST_COLLECTION',
                    'version': '1.0',
                    'start_ts': '2023-02-01T00:00:00Z',
                    'end_ts': '2023-02-10T00:00:00Z',
                    'reason': 'Second reason'
                }
            ]
        }
    )
    
    response = lambda_handler(event, None)
    print(f"Response: {response}")  # Debug output
    
    assert response['statusCode'] == 201
    response_body = json.loads(response['body'])
    assert 'Successfully added 2 reasons' in response_body['message']
    
    # Verify both reasons were added
    reason1 = get_reason(TEST_COLLECTION_ID, '2023-01-01 00:00:00', '2023-01-10 00:00:00')
    reason2 = get_reason(TEST_COLLECTION_ID, '2023-02-01 00:00:00', '2023-02-10 00:00:00')
    assert reason1 == 'First reason'
    assert reason2 == 'Second reason'

def test_post_invalid_body():
    """Test POST with invalid request body"""
    event = create_api_test_event(
        'POST',
        '/gap',
        {
            'invalid_key': 'invalid_value'  # Missing 'reasons' key
        }
    )
    
    response = lambda_handler(event, None)
    print(f"Response: {response}")  # Debug output
    
    assert response['statusCode'] == 400
    response_body = json.loads(response['body'])
    assert 'Invalid request' in response_body['message']

def test_get_gap():
    """Test retrieving gap information for a time range"""
    test_data = [
        {
            'start_ts': '2023-01-01 00:00:00',
            'end_ts': '2023-01-10 00:00:00',
            'reason': 'Gap 1'
        },
        {
            'start_ts': '2023-02-01 00:00:00',
            'end_ts': '2023-02-10 00:00:00',
            'reason': 'Gap 2'
        }
    ]
    seed_test_data(test_data)
    
    event = create_api_test_event(
        'GET',
        '/gap',
        None,  
        query_string_parameters={
            'short_name': 'TEST_COLLECTION',
            'version': '1.0',
            'startDate': '2023-01-01 00:00:00',
            'endDate': '2023-02-15 00:00:00'
        }
    )
    
    response = lambda_handler(event, None)
    print(f"Response: {response}")  # Debug output
    
    assert response['statusCode'] == 200
    response_body = json.loads(response['body'])
    
    # The API returns reasons
    reasons = response_body['reasons']
    assert len(reasons) >= 2
    
    # Find the first gap by checking start_time
    first_gap = None
    for gap in reasons:
        start_time = gap.get('start_time', '')
        if '2023-01-01' in str(start_time):
            first_gap = gap
            break
    
    assert first_gap is not None, f"Could not find gap starting 2023-01-01 in {reasons}"
    
    # Check the fields that are returned by the API
    end_time = first_gap.get('end_time', '')
    assert '2023-01-10' in str(end_time)
    
    # Verify the reason is included
    assert first_gap.get('reason') == 'Gap 1'

def test_missing_params():
    """Test incorrect query params for GET request"""
    
    event = create_api_test_event(
        'GET',
        '/gap',
        None,  
        # `name` instead of `short_name`
        query_string_parameters={
            'name': 'TEST_COLLECTION',
            'version': '1.0',
            'startDate': '2023-01-01 00:00:00',
            'endDate': '2023-02-15 00:00:00'
        }
    )
    
    response = lambda_handler(event, None)
    print(f"Response: {response}")  # Debug output
    
    assert response['statusCode'] == 400
    response_body = json.loads(response['body'])
    assert 'short_name` and `version` are required' in response_body['message']

def test_unsupported_method():
    """Test unsupported HTTP method (PUT)"""
    event = create_api_test_event(
        'PUT',
        '/gap',
        {
            'collection': {
                'short_name': 'TEST_COLLECTION',
                'version': '1.0'
            },
            'gap_begin': '2023-02-01 00:00:00',
            'gap_end': '2023-02-10 00:00:00',
            'reason': 'Updated reason',
            'operation': 'update'
        }
    )
    
    response = lambda_handler(event, None)
    print(f"Response: {response}")  # Debug output
    
    assert response['statusCode'] == 501
    response_body = json.loads(response['body'])
    assert 'Requested method not implemented' in response_body['message']

def test_post_database_error():
    """Test POST with data that would cause a database error"""
    event = create_api_test_event(
        'POST',
        '/gap',
        {
            'reasons': [
                {
                    'shortname': 'NONEXISTENT_COLLECTION',
                    'version': '1.0',
                    'start_ts': '2023-01-01T00:00:00Z',
                    'end_ts': '2023-01-10T00:00:00Z',
                    'reason': 'This should fail'
                }
            ]
        }
    )
    
    response = lambda_handler(event, None)
    print(f"Response: {response}")  # Debug output
    
    # Should return 500 due to foreign key constraint violation
    assert response['statusCode'] == 500
    response_body = json.loads(response['body'])
    assert 'Server error' in response_body['message']

@patch('src.knownGap.knownGap.get_db_connection')
def test_database_connection_error(mock_get_db_connection):
    """Test that triggers the outermost exception handler"""
    # Make get_db_connection raise an exception
    mock_get_db_connection.side_effect = Exception("Database connection failed")
    
    event = create_api_test_event(
        'GET',
        '/gap',
        None,
        query_string_parameters={
            'short_name': 'TEST_COLLECTION',
            'version': '1.0',
            'startDate': '2023-01-01 00:00:00',
            'endDate': '2023-02-15 00:00:00'
        }
    )
    
    response = lambda_handler(event, None)
    print(f"Response: {response}")  # Debug output
    
    # Should return 500 with "Unexpected error occurred" message
    assert response['statusCode'] == 500
    response_body = json.loads(response['body'])
    assert 'Unexpected error ocurred' in response_body['message']

def test_datetime_encoder():
    """Test the DateTimeEncoder class to cover all branches"""
    from src.knownGap.knownGap import DateTimeEncoder
    from datetime import datetime
    import json
    
    encoder = DateTimeEncoder()
    
    # Test with datetime object (covers first return statement)
    test_datetime = datetime(2023, 1, 1, 12, 0, 0)
    result = encoder.default(test_datetime)
    assert result == '2023-01-01T12:00:00'
    
    # Test with non-datetime object to trigger super().default() call
    # This should raise TypeError since the object is not JSON serializable
    class NonSerializableObject:
        pass
    
    non_serializable = NonSerializableObject()
    
    # This should raise TypeError, covering the super().default(obj) line
    with pytest.raises(TypeError):
        encoder.default(non_serializable)

def test_get_database_error():
    """Test GET request that causes a database error during retrieval"""
    # Create a request with malformed date parameters that will cause SQL errors
    event = create_api_test_event(
        'GET',
        '/gap',
        None,
        query_string_parameters={
            'short_name': 'TEST_COLLECTION',
            'version': '1.0',
            'startDate': 'invalid-date-format',  # This should cause a SQL error
            'endDate': '2023-02-15 00:00:00'
        }
    )
    
    response = lambda_handler(event, None)
    print(f"Response: {response}")  # Debug output
    
    # Should return 500 due to date parsing/SQL error
    assert response['statusCode'] == 500
    response_body = json.loads(response['body'])
    assert 'Server error' in response_body['message']