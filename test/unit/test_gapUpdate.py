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
        # Set up gaps
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
    @patch.dict(os.environ, {
        'RDS_SECRET': 'test-secret',
        'RDS_PROXY_HOST': 'test-host',
        'CMR_ENV': 'PROD',
        'AWS_REGION': 'us-west-2',
        'DELETION_QUEUE_ARN': 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
    })
    def test_basic(self):
        with patch('src.gapUpdate.gapUpdate.validate_collections', return_value=True), \
             patch('src.gapUpdate.gapUpdate.update_gaps') as mock_update_gaps, \
             patch('src.gapUpdate.gapUpdate.get_db_connection') as mock_db_conn, \
             patch('os.path.dirname', return_value='/mock/path'), \
             patch('os.path.join', return_value='/mock/path/update_gaps.sql'), \
             patch('builtins.open', mock_open(read_data="SELECT 1;")):
            
            # Mock database connection
            mock_conn = MagicMock()
            mock_db_conn.return_value.__enter__.return_value = mock_conn
            
            test_data = [{
                "collectionId": TEST_COLLECTION_ID, 
                "beginningDateTime": "2000-01-01T00:00:00.000Z", 
                "endingDateTime": "2000-01-02T00:00:00.000Z"
            }]
            
            # Create SQS event with required fields
            event = create_sqs_event(test_data)
            # Add missing SQS fields
            for record in event["Records"]:
                record["eventSourceARN"] = "arn:aws:sqs:us-west-2:123456789012:update-queue"
                record["messageId"] = "test-message-id-1"
            
            result = lambda_handler(event, None)
        
        assert mock_update_gaps.called
        assert result["batchItemFailures"] == []

    @patch.dict(os.environ, {
        'RDS_SECRET': 'test-secret',
        'RDS_PROXY_HOST': 'test-host',
        'CMR_ENV': 'PROD',
        'AWS_REGION': 'us-west-2',
        'DELETION_QUEUE_ARN': 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
    })
    def test_multiple_collections(self):
        with patch('src.gapUpdate.gapUpdate.validate_collections', return_value=True), \
             patch('src.gapUpdate.gapUpdate.update_gaps') as mock_update_gaps, \
             patch('src.gapUpdate.gapUpdate.get_db_connection') as mock_db_conn, \
             patch('os.path.dirname', return_value='/mock/path'), \
             patch('os.path.join', return_value='/mock/path/update_gaps.sql'), \
             patch('builtins.open', mock_open(read_data="SELECT 1;")):
            
            # Mock database connection
            mock_conn = MagicMock()
            mock_db_conn.return_value.__enter__.return_value = mock_conn
            
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
            
            # Create SQS event with required fields
            event = create_sqs_event(test_data)
            # Add missing SQS fields
            for i, record in enumerate(event["Records"]):
                record["eventSourceARN"] = "arn:aws:sqs:us-west-2:123456789012:update-queue"
                record["messageId"] = f"test-message-id-{i+1}"
            
            result = lambda_handler(event, None)
        
        assert mock_update_gaps.call_count == 2
        assert result["batchItemFailures"] == []

    @patch.dict(os.environ, {
        'RDS_SECRET': 'test-secret',
        'RDS_PROXY_HOST': 'test-host',
        'CMR_ENV': 'PROD',
        'AWS_REGION': 'us-west-2',
        'DELETION_QUEUE_ARN': 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
    })
    def test_validation_failure(self):
        with patch('src.gapUpdate.gapUpdate.validate_collections', return_value=False), \
             patch('src.gapUpdate.gapUpdate.get_db_connection') as mock_db_conn, \
             patch('os.path.dirname', return_value='/mock/path'), \
             patch('os.path.join', return_value='/mock/path/update_gaps.sql'), \
             patch('builtins.open', mock_open(read_data="SELECT 1;")):
            
            # Mock database connection
            mock_conn = MagicMock()
            mock_db_conn.return_value.__enter__.return_value = mock_conn
            
            test_data = [{
                "collectionId": TEST_COLLECTION_ID, 
                "beginningDateTime": "2000-01-01T00:00:00.000Z", 
                "endingDateTime": "2000-01-02T00:00:00.000Z"
            }]
            
            # Create SQS event with required fields
            event = create_sqs_event(test_data)
            # Add missing SQS fields
            for record in event["Records"]:
                record["eventSourceARN"] = "arn:aws:sqs:us-west-2:123456789012:update-queue"
                record["messageId"] = "test-message-id-1"
            
            result = lambda_handler(event, None)
            
            # Should have batch item failures due to validation failure
            assert len(result["batchItemFailures"]) > 0

    @patch.dict(os.environ, {
        'RDS_SECRET': 'test-secret',
        'RDS_PROXY_HOST': 'test-host',
        'CMR_ENV': 'PROD',
        'AWS_REGION': 'us-west-2',
        'DELETION_QUEUE_ARN': 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
    })
    def test_deletion_queue_detection(self):
        """Test that records from deletion queue trigger gap addition instead of update"""
        with patch('src.gapUpdate.gapUpdate.validate_collections', return_value=True), \
             patch('src.gapUpdate.gapUpdate.add_gaps') as mock_add_gaps, \
             patch('src.gapUpdate.gapUpdate.update_gaps') as mock_update_gaps, \
             patch('src.gapUpdate.gapUpdate.get_db_connection') as mock_db_conn:
            
            # Mock database connection
            mock_conn = MagicMock()
            mock_db_conn.return_value.__enter__.return_value = mock_conn
            
            test_data = [{
                "collectionId": TEST_COLLECTION_ID, 
                "beginningDateTime": "2000-01-01T00:00:00.000Z", 
                "endingDateTime": "2000-01-02T00:00:00.000Z"
            }]
            
            # Create event from deletion queue
            event = create_sqs_event(test_data)
            # Add required SQS fields and set to deletion queue
            for record in event["Records"]:
                record["eventSourceARN"] = 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
                record["messageId"] = "test-message-id-1"
            
            result = lambda_handler(event, None)
            
            # Should call add_gaps instead of update_gaps
            assert mock_add_gaps.called
            assert not mock_update_gaps.called
            assert result["batchItemFailures"] == []

    @patch.dict(os.environ, {
        'RDS_SECRET': 'test-secret',
        'RDS_PROXY_HOST': 'test-host',
        'CMR_ENV': 'PROD',
        'AWS_REGION': 'us-west-2',
        'DELETION_QUEUE_ARN': 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
    })
    def test_processing_exception_handling(self):
        """Test that exceptions during processing are handled properly"""
        with patch('src.gapUpdate.gapUpdate.validate_collections', return_value=True), \
             patch('src.gapUpdate.gapUpdate.update_gaps', side_effect=Exception("Database error")), \
             patch('src.gapUpdate.gapUpdate.get_db_connection') as mock_db_conn, \
             patch('os.path.dirname', return_value='/mock/path'), \
             patch('os.path.join', return_value='/mock/path/update_gaps.sql'), \
             patch('builtins.open', mock_open(read_data="SELECT 1;")):
            
            # Mock database connection
            mock_conn = MagicMock()
            mock_db_conn.return_value.__enter__.return_value = mock_conn
            
            test_data = [{
                "collectionId": TEST_COLLECTION_ID, 
                "beginningDateTime": "2000-01-01T00:00:00.000Z", 
                "endingDateTime": "2000-01-02T00:00:00.000Z"
            }]
            
            # Create SQS event with required fields
            event = create_sqs_event(test_data)
            # Add missing SQS fields
            for record in event["Records"]:
                record["eventSourceARN"] = "arn:aws:sqs:us-west-2:123456789012:update-queue"
                record["messageId"] = "test-message-id-1"
            
            result = lambda_handler(event, None)
            
            # Should have batch item failures due to processing exception
            assert len(result["batchItemFailures"]) > 0

class TestAddGaps:
    def test_add_gaps_basic(self, setup_test_data):
        """Test basic gap addition for deleted granules"""
        from src.gapUpdate.gapUpdate import add_gaps
        from utils import get_db_connection
        
        # Start with no gaps
        assert get_gap_count(TEST_COLLECTION_ID) == 0
        
        # Create deletion records for granules that were deleted
        deleted_granules = [
            create_granule("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z"),
            create_granule("2000-09-01T00:00:00.000Z", "2000-09-30T23:59:59.000Z")
        ]
        
        with get_db_connection() as conn:
            add_gaps(TEST_COLLECTION_ID, create_buffer(deleted_granules), conn)
        
        # Should have created 2 gaps for the deleted granules
        gaps = get_gaps(TEST_COLLECTION_ID)
        assert len(gaps) == 2
        
        # Verify the gaps match the deleted granule periods (rounded up to next second)
        assert gaps[0][0] == datetime.fromisoformat('2000-06-01 00:00:00')
        assert gaps[0][1] == datetime.fromisoformat('2000-07-01 00:00:00')  # End time rounded up
        
        assert gaps[1][0] == datetime.fromisoformat('2000-09-01 00:00:00')
        assert gaps[1][1] == datetime.fromisoformat('2000-10-01 00:00:00')  # End time rounded up

    def test_add_gaps_merge_with_existing(self, setup_test_data):
        """Test that new gaps merge with existing adjacent gaps"""
        from src.gapUpdate.gapUpdate import add_gaps
        from utils import get_db_connection
        
        # Create an existing gap that ends exactly where the new gap begins
        insert_gap(TEST_COLLECTION_ID, '2000-05-01 00:00:00', '2000-06-01 00:00:00')
        
        # Add a gap that should merge with the existing one (adjacent)
        deleted_granule = [create_granule("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z")]
        
        with get_db_connection() as conn:
            add_gaps(TEST_COLLECTION_ID, create_buffer(deleted_granule), conn)
        
        # Should have merged into a single gap
        gaps = get_gaps(TEST_COLLECTION_ID)
        assert len(gaps) == 1
        
        # Verify the merged gap spans both periods
        assert gaps[0][0] == datetime.fromisoformat('2000-05-01 00:00:00')
        assert gaps[0][1] == datetime.fromisoformat('2000-07-01 00:00:00')

    def test_add_gaps_overlap_detection(self, setup_test_data, caplog):
        """Test that overlapping deleted granules with existing gaps are detected and logged"""
        from src.gapUpdate.gapUpdate import add_gaps
        from utils import get_db_connection
        import logging
        
        # Create an existing gap
        insert_gap(TEST_COLLECTION_ID, '2000-06-01 00:00:00', '2000-06-30 23:59:59')
        
        # Try to add a gap for a deleted granule that overlaps with existing gap
        deleted_granule = [create_granule("2000-06-15T00:00:00.000Z", "2000-07-15T23:59:59.000Z")]
        
        with caplog.at_level(logging.WARNING):
            with get_db_connection() as conn:
                add_gaps(TEST_COLLECTION_ID, create_buffer(deleted_granule), conn)
        
        # Should log a warning about the overlap
        assert "Deletion overlap detected" in caplog.text
        assert TEST_COLLECTION_ID in caplog.text
        
        # Should still merge the gaps properly
        gaps = get_gaps(TEST_COLLECTION_ID)
        assert len(gaps) == 1
        
        # Verify the merged gap covers both periods
        assert gaps[0][0] == datetime.fromisoformat('2000-06-01 00:00:00')
        assert gaps[0][1] == datetime.fromisoformat('2000-07-16 00:00:00')

    def test_add_gaps_transaction_rollback(self, setup_test_data):
        """Test that exceptions during add_gaps cause transaction rollback"""
        from src.gapUpdate.gapUpdate import add_gaps
        from utils import get_db_connection
        
        # Create initial state
        insert_gap(TEST_COLLECTION_ID, '2000-01-01 00:00:00', '2000-01-31 23:59:59')
        initial_count = get_gap_count(TEST_COLLECTION_ID)
        
        # Create deletion record
        deleted_granule = [create_granule("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z")]
        
        # Mock a database error during the operation
        with patch('src.gapUpdate.gapUpdate.logger') as mock_logger:
            with get_db_connection() as conn:
                # Force an error by closing the cursor mid-operation
                with patch.object(conn, 'cursor') as mock_cursor_method:
                    mock_cursor = MagicMock()
                    mock_cursor_method.return_value = mock_cursor
                    mock_cursor.execute.side_effect = [None, None, Exception("Database error")]
                    
                    with pytest.raises(Exception, match="Database error"):
                        add_gaps(TEST_COLLECTION_ID, create_buffer(deleted_granule), conn)
        
        # Verify the gap count hasn't changed (rollback worked)
        assert get_gap_count(TEST_COLLECTION_ID) == initial_count

    def test_add_gaps_multiple_overlapping_deletions(self, setup_test_data):
        """Test adding multiple overlapping deleted granules"""
        from src.gapUpdate.gapUpdate import add_gaps
        from utils import get_db_connection
        
        # Create multiple overlapping deletion records
        deleted_granules = [
            create_granule("2000-06-01T00:00:00.000Z", "2000-06-15T23:59:59.000Z"),
            create_granule("2000-06-10T00:00:00.000Z", "2000-06-25T23:59:59.000Z"),
            create_granule("2000-06-20T00:00:00.000Z", "2000-07-05T23:59:59.000Z")
        ]
        
        with get_db_connection() as conn:
            add_gaps(TEST_COLLECTION_ID, create_buffer(deleted_granules), conn)
        
        # Should merge into a single gap covering the entire range
        gaps = get_gaps(TEST_COLLECTION_ID)
        assert len(gaps) == 1
        
        # Verify the gap covers the full merged range
        assert gaps[0][0] == datetime.fromisoformat('2000-06-01 00:00:00')
        assert gaps[0][1] == datetime.fromisoformat('2000-07-06 00:00:00')  # End time rounded up