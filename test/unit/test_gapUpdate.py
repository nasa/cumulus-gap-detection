from datetime import datetime
import pytest
from unittest.mock import patch, mock_open, MagicMock
import os
import sys

from conftest import (
    TEST_COLLECTION_ID, SECOND_COLLECTION_ID,
    create_granule, create_buffer, create_sqs_event,
    insert_gap, get_gaps, get_gap_count, get_sql_query
)

from src.gapUpdate.gapUpdate import update_gaps, validate_collections, lambda_handler

def test_collection_validation(setup_test_data):
    from utils import get_db_connection
    
    with get_db_connection() as conn:
        assert validate_collections({TEST_COLLECTION_ID}, conn) is True
        assert validate_collections({"nonexistent_collection"}, conn) is False
        assert validate_collections({TEST_COLLECTION_ID, "nonexistent_collection"}, conn) is False

class TestGapUpdates:
    @pytest.mark.parametrize("scenario,initial_gaps,granules,expected_gaps", [
        # Basic gap splitting
        (
            "basic_split", 
            [('2000-01-01 00:00:00', '2000-12-31 23:59:59')],
            [("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z")],
            [('2000-01-01 00:00:00', '2000-06-01 00:00:00'), 
             ('2000-07-01 00:00:00', '2000-12-31 23:59:59')]
        ),
        # Complete gap coverage
        (
            "complete_coverage",
            [('2000-03-01 00:00:00', '2000-03-31 23:59:59')],
            [("2000-02-15T00:00:00.000Z", "2000-04-15T23:59:59.000Z")],
            []
        ),
        # Multiple non-overlapping granules
        (
            "multiple_granules",
            [('2000-01-01 00:00:00', '2000-12-31 23:59:59')],
            [
                ("2000-03-01T00:00:00.000Z", "2000-03-31T23:59:59.000Z"),
                ("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z"),
                ("2000-09-01T00:00:00.000Z", "2000-09-30T23:59:59.000Z")
            ],
            [
                ('2000-01-01 00:00:00', '2000-03-01 00:00:00'),
                ('2000-04-01 00:00:00', '2000-06-01 00:00:00'),
                ('2000-07-01 00:00:00', '2000-09-01 00:00:00'),
                ('2000-10-01 00:00:00', '2000-12-31 23:59:59')
            ]
        ),
        # Overlapping granules
        (
            "overlapping_granules",
            [('2000-01-01 00:00:00', '2000-12-31 23:59:59')],
            [
                ("2000-03-01T00:00:00.000Z", "2000-04-15T23:59:59.000Z"),
                ("2000-04-01T00:00:00.000Z", "2000-05-15T23:59:59.000Z")
            ],
            [
                ('2000-01-01 00:00:00', '2000-03-01 00:00:00'),
                ('2000-05-16 00:00:00', '2000-12-31 23:59:59')
            ]
        ),
        # Granule spanning multiple gaps
        (
            "spanning_multiple_gaps",
            [
                ('2000-01-01 00:00:00', '2000-03-31 23:59:59'),
                ('2000-06-01 00:00:00', '2000-09-30 23:59:59')
            ],
            [("2000-02-01T00:00:00.000Z", "2000-07-15T23:59:59.000Z")],
            [
                ('2000-01-01 00:00:00', '2000-02-01 00:00:00'),
                ('2000-07-16 00:00:00', '2000-09-30 23:59:59')
            ]
        ),
    ])
    def test_scenarios(self, setup_test_data, mock_sql, scenario, initial_gaps, granules, expected_gaps):
        # Set up the gaps
        for gap in initial_gaps:
            insert_gap(TEST_COLLECTION_ID, gap[0], gap[1])
        
        test_data = [create_granule(g[0], g[1]) for g in granules]
        
        from utils import get_db_connection
        
        with get_db_connection() as conn:
            update_gaps(TEST_COLLECTION_ID, create_buffer(test_data), get_sql_query(), conn)
        
        # Verify results
        gaps = get_gaps(TEST_COLLECTION_ID)
        assert len(gaps) == len(expected_gaps)
        for i, expected in enumerate(expected_gaps):
            assert gaps[i][0] == datetime.fromisoformat(expected[0])
            assert gaps[i][1] == datetime.fromisoformat(expected[1])

    def test_multiple_collections(self, setup_test_data, mock_sql):
        # Create identical gaps in both collections
        insert_gap(TEST_COLLECTION_ID, '2000-01-01 00:00:00', '2000-12-31 23:59:59')
        insert_gap(SECOND_COLLECTION_ID, '2000-01-01 00:00:00', '2000-12-31 23:59:59')
        
        # Process granule only for first collection
        test_data = [create_granule("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z")]
        
        from utils import get_db_connection
        
        with get_db_connection() as conn:
            update_gaps(TEST_COLLECTION_ID, create_buffer(test_data), get_sql_query(), conn)
        
        # Verify: first collection split, second unchanged
        assert get_gap_count(TEST_COLLECTION_ID) == 2
        
        gap = get_gaps(SECOND_COLLECTION_ID)[0]
        assert gap[0] == datetime.fromisoformat('2000-01-01 00:00:00')
        assert gap[1] == datetime.fromisoformat('2000-12-31 23:59:59')

    def test_transaction_behavior(self, setup_test_data, mock_sql):
        insert_gap(TEST_COLLECTION_ID, '2000-01-01 00:00:00', '2000-12-31 23:59:59')
        
        # Simulate a transaction failure
        test_data = [create_granule("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z")]
        
        from utils import get_db_connection
        
        with pytest.raises(Exception):
            with get_db_connection() as conn:
                update_gaps(TEST_COLLECTION_ID, create_buffer(test_data), "INVALID SQL", conn)
        
        # Verify the original gap is unchanged
        gaps = get_gaps(TEST_COLLECTION_ID)
        assert len(gaps) == 1
        assert gaps[0][0] == datetime.fromisoformat('2000-01-01 00:00:00')
        assert gaps[0][1] == datetime.fromisoformat('2000-12-31 23:59:59')

class TestLambdaHandler:
    def test_basic(self):
        with patch('src.gapUpdate.gapUpdate.validate_collections', return_value=True), \
             patch('src.gapUpdate.gapUpdate.update_gaps') as mock_update_gaps, \
             patch('os.path.dirname', return_value='/mock/path'), \
             patch('os.path.join', return_value='/mock/path/update_gaps.sql'), \
             patch('builtins.open', mock_open(read_data="SELECT 1;")):
            
            test_data = [{
                "collectionId": TEST_COLLECTION_ID, 
                "beginningDateTime": "2000-01-01T00:00:00.000Z", 
                "endingDateTime": "2000-01-02T00:00:00.000Z"
            }]
            result = lambda_handler(create_sqs_event(test_data), None)
        
        assert mock_update_gaps.called
        assert result["statusCode"] == 200

    def test_multiple_collections(self):
        with patch('src.gapUpdate.gapUpdate.validate_collections', return_value=True), \
             patch('src.gapUpdate.gapUpdate.update_gaps') as mock_update_gaps, \
             patch('os.path.dirname', return_value='/mock/path'), \
             patch('os.path.join', return_value='/mock/path/update_gaps.sql'), \
             patch('builtins.open', mock_open(read_data="SELECT 1;")):
            
            test_data = [
                {
                    "collectionId": TEST_COLLECTION_ID, 
                    "beginningDateTime": "2000-01-01T00:00:00.000Z", 
                    "endingDateTime": "2000-01-02T00:00:00.000Z"
                },
                {
                    "collectionId": "SECOND_COLLECTION___1_0", 
                    "beginningDateTime": "2000-02-01T00:00:00.000Z", 
                    "endingDateTime": "2000-02-02T00:00:00.000Z"
                }
            ]
            result = lambda_handler(create_sqs_event(test_data), None)
        
        assert mock_update_gaps.call_count == 2
        assert result["statusCode"] == 200

    def test_validation_failure(self):
        with patch('src.gapUpdate.gapUpdate.validate_collections', return_value=False), \
             patch('os.path.dirname', return_value='/mock/path'), \
             patch('os.path.join', return_value='/mock/path/update_gaps.sql'), \
             patch('builtins.open', mock_open(read_data="SELECT 1;")):
            
            test_data = [{
                "collectionId": TEST_COLLECTION_ID, 
                "beginningDateTime": "2000-01-01T00:00:00.000Z", 
                "endingDateTime": "2000-01-02T00:00:00.000Z"
            }]
            
            with pytest.raises(Exception) as excinfo:
                lambda_handler(create_sqs_event(test_data), None)
            
            assert "uninitialized collections" in str(excinfo.value)
