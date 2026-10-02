from datetime import datetime
import pytest
from unittest.mock import patch, MagicMock
import os

from conftest import (
    TEST_COLLECTION_ID, SECOND_COLLECTION_ID,
    create_granule, create_buffer, create_sqs_event,
    insert_gap, get_gaps, get_gap_count,
    FakeLambdaContext, future_deadline
)

from src.gapUpdate.gapUpdate import _run_gap_transaction, get_all_collections, lambda_handler


def test_get_all_collections(setup_test_data):
    from utils import get_db_connection

    with get_db_connection() as conn:
        collections = get_all_collections(conn)

    assert TEST_COLLECTION_ID in collections
    assert SECOND_COLLECTION_ID in collections
    assert "nonexistent_collection" not in collections


class TestCloseOperation:
    """Tests _run_gap_transaction with _open=False (the default), i.e.
    shrinking gap coverage based on newly-arrived granules."""

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
    def test_scenarios(self, setup_test_data, scenario, initial_gaps, granules, expected_gaps):
        for gap in initial_gaps:
            insert_gap(TEST_COLLECTION_ID, gap[0], gap[1])

        test_data = [create_granule(g[0], g[1]) for g in granules]

        from utils import get_db_connection

        with get_db_connection() as conn:
            _run_gap_transaction(TEST_COLLECTION_ID, create_buffer(test_data), conn, future_deadline())

        gaps = get_gaps(TEST_COLLECTION_ID)
        assert len(gaps) == len(expected_gaps)
        for i, expected in enumerate(expected_gaps):
            assert gaps[i][0] == datetime.fromisoformat(expected[0])
            assert gaps[i][1] == datetime.fromisoformat(expected[1])

    def test_multiple_collections(self, setup_test_data):
        insert_gap(TEST_COLLECTION_ID, '2000-01-01 00:00:00', '2000-12-31 23:59:59')
        insert_gap(SECOND_COLLECTION_ID, '2000-01-01 00:00:00', '2000-12-31 23:59:59')

        test_data = [create_granule("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z")]

        from utils import get_db_connection

        with get_db_connection() as conn:
            _run_gap_transaction(TEST_COLLECTION_ID, create_buffer(test_data), conn, future_deadline())

        assert get_gap_count(TEST_COLLECTION_ID) == 2

        gap = get_gaps(SECOND_COLLECTION_ID)[0]
        assert gap[0] == datetime.fromisoformat('2000-01-01 00:00:00')
        assert gap[1] == datetime.fromisoformat('2000-12-31 23:59:59')

    def test_scopes_to_correct_collection_only(self, setup_test_data):
        insert_gap(TEST_COLLECTION_ID, '2000-01-01 00:00:00', '2000-12-31 23:59:59')
        insert_gap(SECOND_COLLECTION_ID, '2000-01-01 00:00:00', '2000-12-31 23:59:59')

        test_data = [create_granule("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z")]

        from utils import get_db_connection

        with get_db_connection() as conn:
            _run_gap_transaction(TEST_COLLECTION_ID, create_buffer(test_data), conn, future_deadline())

        assert get_gap_count(TEST_COLLECTION_ID) == 2
        assert get_gap_count(SECOND_COLLECTION_ID) == 1

        untouched_gap = get_gaps(SECOND_COLLECTION_ID)[0]
        assert untouched_gap[0] == datetime.fromisoformat('2000-01-01 00:00:00')
        assert untouched_gap[1] == datetime.fromisoformat('2000-12-31 23:59:59')

    def test_transaction_behavior(self, setup_test_data):
        insert_gap(TEST_COLLECTION_ID, '2000-01-01 00:00:00', '2000-12-31 23:59:59')

        test_data = [create_granule("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z")]

        from utils import get_db_connection

        with get_db_connection() as conn:
            with patch.object(conn, 'cursor') as mock_cursor_method:
                mock_cursor = MagicMock()
                mock_cursor_method.return_value = mock_cursor
                mock_cursor.execute.side_effect = [None, None, Exception("Database error")]

                with pytest.raises(Exception, match="Database error"):
                    _run_gap_transaction(TEST_COLLECTION_ID, create_buffer(test_data), conn, future_deadline())

        gaps = get_gaps(TEST_COLLECTION_ID)
        assert len(gaps) == 1
        assert gaps[0][0] == datetime.fromisoformat('2000-01-01 00:00:00')
        assert gaps[0][1] == datetime.fromisoformat('2000-12-31 23:59:59')


class TestOpenOperation:

    def test_basic(self, setup_test_data):
        from utils import get_db_connection

        assert get_gap_count(TEST_COLLECTION_ID) == 0

        deleted_granules = [
            create_granule("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z"),
            create_granule("2000-09-01T00:00:00.000Z", "2000-09-30T23:59:59.000Z")
        ]

        with get_db_connection() as conn:
            _run_gap_transaction(TEST_COLLECTION_ID, create_buffer(deleted_granules), conn, future_deadline(), _open=True)

        gaps = get_gaps(TEST_COLLECTION_ID)
        assert len(gaps) == 2

        assert gaps[0][0] == datetime.fromisoformat('2000-06-01 00:00:00')
        assert gaps[0][1] == datetime.fromisoformat('2000-07-01 00:00:00')

        assert gaps[1][0] == datetime.fromisoformat('2000-09-01 00:00:00')
        assert gaps[1][1] == datetime.fromisoformat('2000-10-01 00:00:00')

    def test_merge_with_existing(self, setup_test_data):
        from utils import get_db_connection

        insert_gap(TEST_COLLECTION_ID, '2000-05-01 00:00:00', '2000-06-01 00:00:00')

        deleted_granule = [create_granule("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z")]

        with get_db_connection() as conn:
            _run_gap_transaction(TEST_COLLECTION_ID, create_buffer(deleted_granule), conn, future_deadline(), _open=True)

        gaps = get_gaps(TEST_COLLECTION_ID)
        assert len(gaps) == 1

        assert gaps[0][0] == datetime.fromisoformat('2000-05-01 00:00:00')
        assert gaps[0][1] == datetime.fromisoformat('2000-07-01 00:00:00')

    def test_transaction_rollback(self, setup_test_data):
        from utils import get_db_connection

        insert_gap(TEST_COLLECTION_ID, '2000-01-01 00:00:00', '2000-01-31 23:59:59')
        initial_count = get_gap_count(TEST_COLLECTION_ID)

        deleted_granule = [create_granule("2000-06-01T00:00:00.000Z", "2000-06-30T23:59:59.000Z")]

        with patch('src.gapUpdate.gapUpdate.logger') as mock_logger:
            with get_db_connection() as conn:
                with patch.object(conn, 'cursor') as mock_cursor_method:
                    mock_cursor = MagicMock()
                    mock_cursor_method.return_value = mock_cursor
                    mock_cursor.execute.side_effect = [None, None, Exception("Database error")]

                    with pytest.raises(Exception, match="Database error"):
                        _run_gap_transaction(TEST_COLLECTION_ID, create_buffer(deleted_granule), conn, future_deadline(), _open=True)

        assert get_gap_count(TEST_COLLECTION_ID) == initial_count

    def test_multiple_overlapping_deletions(self, setup_test_data):
        from utils import get_db_connection

        deleted_granules = [
            create_granule("2000-06-01T00:00:00.000Z", "2000-06-15T23:59:59.000Z"),
            create_granule("2000-06-10T00:00:00.000Z", "2000-06-25T23:59:59.000Z"),
            create_granule("2000-06-20T00:00:00.000Z", "2000-07-05T23:59:59.000Z")
        ]

        with get_db_connection() as conn:
            _run_gap_transaction(TEST_COLLECTION_ID, create_buffer(deleted_granules), conn, future_deadline(), _open=True)

        gaps = get_gaps(TEST_COLLECTION_ID)
        assert len(gaps) == 1

        assert gaps[0][0] == datetime.fromisoformat('2000-06-01 00:00:00')
        assert gaps[0][1] == datetime.fromisoformat('2000-07-06 00:00:00')


class TestLambdaHandler:
    @patch.dict(os.environ, {
        'RDS_SECRET': 'test-secret',
        'RDS_PROXY_HOST': 'test-host',
        'CMR_ENV': 'PROD',
        'AWS_REGION': 'us-west-2',
        'DELETION_QUEUE_ARN': 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
    })
    def test_basic(self):
        with patch('src.gapUpdate.gapUpdate.get_all_collections', return_value={TEST_COLLECTION_ID}), \
             patch('src.gapUpdate.gapUpdate._run_gap_transaction') as mock_run, \
             patch('src.gapUpdate.gapUpdate.get_db_connection') as mock_db_conn:

            mock_conn = MagicMock()
            mock_db_conn.return_value.__enter__.return_value = mock_conn

            test_data = [{
                "collectionId": TEST_COLLECTION_ID,
                "beginningDateTime": "2000-01-01T00:00:00.000Z",
                "endingDateTime": "2000-01-02T00:00:00.000Z"
            }]

            event = create_sqs_event(test_data)
            for record in event["Records"]:
                record["eventSourceARN"] = "arn:aws:sqs:us-west-2:123456789012:update-queue"
                record["messageId"] = "test-message-id-1"

            result = lambda_handler(event, FakeLambdaContext())

        assert mock_run.called
        assert mock_run.call_args.kwargs.get("_open", False) is False
        assert result["batchItemFailures"] == []

    @patch.dict(os.environ, {
        'RDS_SECRET': 'test-secret',
        'RDS_PROXY_HOST': 'test-host',
        'CMR_ENV': 'PROD',
        'AWS_REGION': 'us-west-2',
        'DELETION_QUEUE_ARN': 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
    })
    def test_multiple_collections(self):
        with patch('src.gapUpdate.gapUpdate.get_all_collections',
                   return_value={TEST_COLLECTION_ID, "SECOND_COLLECTION___1_0"}), \
             patch('src.gapUpdate.gapUpdate._run_gap_transaction') as mock_run, \
             patch('src.gapUpdate.gapUpdate.get_db_connection') as mock_db_conn:

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

            event = create_sqs_event(test_data)
            for i, record in enumerate(event["Records"]):
                record["eventSourceARN"] = "arn:aws:sqs:us-west-2:123456789012:update-queue"
                record["messageId"] = f"test-message-id-{i+1}"

            result = lambda_handler(event, FakeLambdaContext())

        assert mock_run.call_count == 2
        assert result["batchItemFailures"] == []

    @patch.dict(os.environ, {
        'RDS_SECRET': 'test-secret',
        'RDS_PROXY_HOST': 'test-host',
        'CMR_ENV': 'PROD',
        'AWS_REGION': 'us-west-2',
        'DELETION_QUEUE_ARN': 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
    })
    def test_unmonitored_collection_skipped(self):
        with patch('src.gapUpdate.gapUpdate.get_all_collections', return_value=set()), \
             patch('src.gapUpdate.gapUpdate._run_gap_transaction') as mock_run, \
             patch('src.gapUpdate.gapUpdate.get_db_connection') as mock_db_conn:

            mock_conn = MagicMock()
            mock_db_conn.return_value.__enter__.return_value = mock_conn

            test_data = [{
                "collectionId": TEST_COLLECTION_ID,
                "beginningDateTime": "2000-01-01T00:00:00.000Z",
                "endingDateTime": "2000-01-02T00:00:00.000Z"
            }]

            event = create_sqs_event(test_data)
            for record in event["Records"]:
                record["eventSourceARN"] = "arn:aws:sqs:us-west-2:123456789012:update-queue"
                record["messageId"] = "test-message-id-1"

            result = lambda_handler(event, FakeLambdaContext())

        assert result["batchItemFailures"] == []
        assert not mock_run.called

    @patch.dict(os.environ, {
        'RDS_SECRET': 'test-secret',
        'RDS_PROXY_HOST': 'test-host',
        'CMR_ENV': 'PROD',
        'AWS_REGION': 'us-west-2',
        'DELETION_QUEUE_ARN': 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
    })
    def test_mixed_monitored_and_unmonitored_collections(self):
        with patch('src.gapUpdate.gapUpdate.get_all_collections', return_value={TEST_COLLECTION_ID}), \
             patch('src.gapUpdate.gapUpdate._run_gap_transaction') as mock_run, \
             patch('src.gapUpdate.gapUpdate.get_db_connection') as mock_db_conn:

            mock_conn = MagicMock()
            mock_db_conn.return_value.__enter__.return_value = mock_conn

            test_data = [
                {
                    "collectionId": TEST_COLLECTION_ID,
                    "beginningDateTime": "2000-01-01T00:00:00.000Z",
                    "endingDateTime": "2000-01-02T00:00:00.000Z"
                },
                {
                    "collectionId": "UNMONITORED_COLLECTION___1_0",
                    "beginningDateTime": "2000-02-01T00:00:00.000Z",
                    "endingDateTime": "2000-02-02T00:00:00.000Z"
                }
            ]

            event = create_sqs_event(test_data)
            for i, record in enumerate(event["Records"]):
                record["eventSourceARN"] = "arn:aws:sqs:us-west-2:123456789012:update-queue"
                record["messageId"] = f"test-message-id-{i+1}"

            result = lambda_handler(event, FakeLambdaContext())

        assert mock_run.call_count == 1
        assert mock_run.call_args[0][0] == TEST_COLLECTION_ID
        assert result["batchItemFailures"] == []

    @patch.dict(os.environ, {
        'RDS_SECRET': 'test-secret',
        'RDS_PROXY_HOST': 'test-host',
        'CMR_ENV': 'PROD',
        'AWS_REGION': 'us-west-2',
        'DELETION_QUEUE_ARN': 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
    })
    def test_deletion_queue_detection(self):
        with patch('src.gapUpdate.gapUpdate.get_all_collections', return_value={TEST_COLLECTION_ID}), \
             patch('src.gapUpdate.gapUpdate._run_gap_transaction') as mock_run, \
             patch('src.gapUpdate.gapUpdate.get_db_connection') as mock_db_conn:

            mock_conn = MagicMock()
            mock_db_conn.return_value.__enter__.return_value = mock_conn

            test_data = [{
                "collectionId": TEST_COLLECTION_ID,
                "beginningDateTime": "2000-01-01T00:00:00.000Z",
                "endingDateTime": "2000-01-02T00:00:00.000Z"
            }]

            event = create_sqs_event(test_data)
            for record in event["Records"]:
                record["eventSourceARN"] = 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
                record["messageId"] = "test-message-id-1"

            result = lambda_handler(event, FakeLambdaContext())

            assert mock_run.called
            assert mock_run.call_args.kwargs["_open"] is True
            assert result["batchItemFailures"] == []

    @patch.dict(os.environ, {
        'RDS_SECRET': 'test-secret',
        'RDS_PROXY_HOST': 'test-host',
        'CMR_ENV': 'PROD',
        'AWS_REGION': 'us-west-2',
        'DELETION_QUEUE_ARN': 'arn:aws:sqs:us-west-2:123456789012:deletion-queue'
    })
    def test_processing_exception_handling(self):
        with patch('src.gapUpdate.gapUpdate.get_all_collections', return_value={TEST_COLLECTION_ID}), \
             patch('src.gapUpdate.gapUpdate._run_gap_transaction', side_effect=Exception("Database error")), \
             patch('src.gapUpdate.gapUpdate.get_db_connection') as mock_db_conn:

            mock_conn = MagicMock()
            mock_db_conn.return_value.__enter__.return_value = mock_conn

            test_data = [{
                "collectionId": TEST_COLLECTION_ID,
                "beginningDateTime": "2000-01-01T00:00:00.000Z",
                "endingDateTime": "2000-01-02T00:00:00.000Z"
            }]

            event = create_sqs_event(test_data)
            for record in event["Records"]:
                record["eventSourceARN"] = "arn:aws:sqs:us-west-2:123456789012:update-queue"
                record["messageId"] = "test-message-id-1"

            result = lambda_handler(event, FakeLambdaContext())

            assert len(result["batchItemFailures"]) > 0
