import json
import pytest
import logging
from utils import get_db_connection

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from conftest import TEST_COLLECTION_ID, DEFAULT_DATE, DEFAULT_END_DATE, create_api_test_event, seed_test_data
from src.knownGap.knownGap import lambda_handler

def get_record(collection_id, start_ts, end_ts):
    """Get a specific record from the gaps table."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT gap_id, collection_id, start_ts, end_ts, reason 
                FROM gaps 
                WHERE collection_id = %s
                AND start_ts = %s
                AND end_ts = %s
            """, (collection_id, start_ts, end_ts))
            return cur.fetchone()

def test_add_reason():
    """Test adding a reason to a gap that has no reason"""
    test_data = [
        {
            'start_ts': '2023-01-01 00:00:00',
            'end_ts': '2023-01-10 00:00:00',
            'reason': None
        }
    ]
    seed_test_data(test_data)
    
    event = create_api_test_event(
        'PUT',
        '/gap',
        {
            'collection': {
                'short_name': 'TEST_COLLECTION',
                'version': '1.0'
            },
            'gap_begin': '2023-01-01 00:00:00',
            'gap_end': '2023-01-10 00:00:00',
            'reason': 'New reason added',
            'operation': 'create'
        }
    )
    response = lambda_handler(event, None)
    assert response['statusCode'] == 200
    response_body = json.loads(response['body'])
    assert 'Added' in response_body['message']
    
    after = get_record(TEST_COLLECTION_ID, '2023-01-01 00:00:00', '2023-01-10 00:00:00')
    assert after[4] == 'New reason added'

def test_update_reason():
    """Test updating a reason for a gap that already has one"""
    test_data = [
        {
            'start_ts': '2023-02-01 00:00:00',
            'end_ts': '2023-02-10 00:00:00',
            'reason': 'Initial reason'
        }
    ]
    seed_test_data(test_data)
    
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
    assert response['statusCode'] == 200
    response_body = json.loads(response['body'])
    assert 'Updated' in response_body['message']

    after = get_record(TEST_COLLECTION_ID, '2023-02-01 00:00:00', '2023-02-10 00:00:00')
    assert after[4] == 'Updated reason'

def test_remove_reason():
    """Test removing a reason by using delete operation"""
    test_data = [
        {
            'start_ts': '2023-03-01 00:00:00',
            'end_ts': '2023-03-10 00:00:00',
            'reason': 'Initial reason'
        }
    ]
    seed_test_data(test_data)
    
    event = create_api_test_event(
        'PUT',
        '/gap',
        {
            'collection': {
                'short_name': 'TEST_COLLECTION',
                'version': '1.0'
            },
            'gap_begin': '2023-03-01 00:00:00',
            'gap_end': '2023-03-10 00:00:00',
            'operation': 'delete'
        }
    )
    response = lambda_handler(event, None)
    assert response['statusCode'] == 200
    response_body = json.loads(response['body'])
    assert 'Deleted' in response_body['message']

    after = get_record(TEST_COLLECTION_ID, '2023-03-01 00:00:00', '2023-03-10 00:00:00')
    assert after[4] is None

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
    assert response['statusCode'] == 200
    response_body = json.loads(response['body'])
    gaps = response_body['gaps']
    assert len(gaps) >= 2
    first_gap = next(gap for gap in gaps if '2023-01-01' in gap['start_ts'])
    assert first_gap['collection_id'] == TEST_COLLECTION_ID
    assert '2023-01-10' in first_gap['end_ts']

def test_missing_params():
    """Test incorrect query params"""
    
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
    assert response['statusCode'] == 400

def test_bad_request():
    """Test PUT with invalid keys"""

    event = create_api_test_event(
        'PUT',
        '/gap',
        # `name` instead of `short_name`
        {
            'collection': {
                'name': 'TEST_COLLECTION',
                'version': '1.0'
            },
            'gap_begin': '2023-02-01 00:00:00',
            'gap_end': '2023-02-10 00:00:00',
            'reason': 'Updated reason',
            'operation': 'update'
        }
    )
    response = lambda_handler(event, None)
    assert response['statusCode'] == 400



