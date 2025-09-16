import pytest
import json
import os
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler import (
    lambda_handler, split_date_ranges, build_message, get_params, fetch_cmr_range
)

class TestGapMigrationStreamMessageCompiler:
    
    @pytest.fixture
    def event(self):
        return {
            "Records": [
                {
                    "Sns": {
                        "Message": json.dumps({
                            "short_name": "TEST_COLLECTION",
                            "version": "1_0"
                        })
                    }
                }
            ]
        }
    
    @pytest.fixture
    def context(self):
        context = MagicMock()
        context.function_name = "test-function"
        return context
    
    def test_split_date_ranges(self):
        start_date = "2000-01-01T00:00:00Z"
        end_date = "2000-01-05T00:00:00Z"
        num_ranges = 4
        ranges = split_date_ranges(start_date, end_date, num_ranges)
        assert len(ranges) == num_ranges
        assert ranges[0][0] == "2000-01-01T00:00:00Z"
        assert ranges[-1][1] == "2000-01-05T00:00:00Z"
    
    def test_build_message(self):
        granule = {
            "id": "G1",
            "time_start": "2000-01-01T00:00:00Z",
            "time_end": "2000-01-02T00:00:00Z"
        }
        short_name = "TEST_COLLECTION"
        version = "1_0"
        message = build_message(granule, short_name, version)
        assert message["Id"] == "G1"
        message_body = json.loads(message["MessageBody"])
        inner_message = json.loads(message_body["Message"])
        assert inner_message["record"]["collectionId"] == "TEST_COLLECTION___1_0"
        assert inner_message["record"]["beginningDateTime"] == "2000-01-01T00:00:00Z"
        assert inner_message["record"]["endingDateTime"] == "2000-01-02T00:00:00Z"
    
    @patch.dict(os.environ, {'QUEUE_URL': 'https://sqs.example.com/test-queue'})
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.get_params')
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.loop')
    def test_lambda_handler_success(self, mock_loop, mock_get_params, event, context):
        partitions = [
            ("2000-01-01T00:00:00Z", "2000-01-02T00:00:00Z"),
            ("2000-01-02T00:00:00Z", "2000-01-03T00:00:00Z")
        ]
        mock_get_params.return_value = (partitions, 2, 4000, 100)
        
        result = lambda_handler(event, context)
        
        assert result["statusCode"] == 200
        assert "Processing complete" in result["body"]
        mock_loop.run_until_complete.assert_called_once()
    
    @patch.dict(os.environ, {'QUEUE_URL': 'https://sqs.example.com/test-queue'})
    def test_lambda_handler_missing_params(self, context):
        invalid_event = {
            "Records": [
                {
                    "Sns": {
                        "Message": json.dumps({})
                    }
                }
            ]
        }
        result = lambda_handler(invalid_event, context)
        assert result["statusCode"] == 400
        assert "Missing short_name or version" in result["body"]
    
    @patch.dict(os.environ, {'QUEUE_URL': 'https://sqs.example.com/test-queue'})
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.get_params')
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.loop')
    def test_lambda_handler_get_params_failure(self, mock_loop, mock_get_params, event, context):
        """Test when get_params returns an error"""
        mock_get_params.return_value = (None, {
            "statusCode": 400,
            "body": json.dumps({"error": "Collection not found"})
        })
        
        # The lambda will raise ValueError due to unpacking issue
        with pytest.raises(ValueError, match="not enough values to unpack"):
            lambda_handler(event, context)
    
    @patch.dict(os.environ, {'QUEUE_URL': 'https://sqs.example.com/test-queue'})
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.get_params')
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.loop')
    def test_lambda_handler_processing_exception(self, mock_loop, mock_get_params, event, context):
        """Test when collection processing raises an exception"""
        partitions = [("2000-01-01T00:00:00Z", "2000-01-02T00:00:00Z")]
        mock_get_params.return_value = (partitions, 2, 4000, 100)
        
        # Make process_collection raise an exception
        mock_loop.run_until_complete.side_effect = Exception("Processing failed")
        
        result = lambda_handler(event, context)
        
        assert result["statusCode"] == 500
        assert "Processing failed" in result["body"]
    
    def test_lambda_handler_missing_environment_variable(self, event, context):
        """Test when QUEUE_URL environment variable is missing"""
        with patch.dict(os.environ, {}, clear=True):
            result = lambda_handler(event, context)
            
            # The lambda returns a tuple (None, error_dict)
            assert result[0] is None
            assert result[1]["statusCode"] == 400
            assert "error" in result[1]["body"]
    
    def test_lambda_handler_invalid_event_format(self, context):
        """Test with malformed event structure"""
        invalid_event = {
            "Records": [
                {
                    "InvalidKey": "invalid_value"
                }
            ]
        }
        
        with patch.dict(os.environ, {'QUEUE_URL': 'https://sqs.example.com/test-queue'}):
            result = lambda_handler(invalid_event, context)
            
            # returns tuple (None, error_dict) with KeyError
            assert result[0] is None
            assert result[1]["statusCode"] == 400
    
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.GranuleQuery')
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.CollectionQuery')
    def test_get_params_success(self, mock_collection_query, mock_granule_query):
        """Test successful get_params execution"""
        # Mock GranuleQuery
        mock_granule_api = MagicMock()
        mock_granule_api.hits.return_value = 1000
        mock_granule_query.return_value = mock_granule_api
        
        # Mock CollectionQuery
        mock_collection_api = MagicMock()
        mock_collection_api.get_all.return_value = [{
            "time_start": "2020-01-01T00:00:00Z",
            "time_end": "2020-12-31T23:59:59Z"
        }]
        mock_collection_query.return_value = mock_collection_api
        
        date_ranges, n_consumers, queue_size, num_granules = get_params("TEST", "1.0")
        
        assert date_ranges is not None
        assert isinstance(n_consumers, int)
        assert isinstance(queue_size, int)
        assert num_granules == 1000
    
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.GranuleQuery')
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.CollectionQuery')
    def test_get_params_no_collections_found(self, mock_collection_query, mock_granule_query):
        """Test get_params when no collections are found"""
        # Mock GranuleQuery
        mock_granule_api = MagicMock()
        mock_granule_api.hits.return_value = 1000
        mock_granule_query.return_value = mock_granule_api
        
        # Mock CollectionQuery to return empty list
        mock_collection_api = MagicMock()
        mock_collection_api.get_all.return_value = []
        mock_collection_query.return_value = mock_collection_api
        
        date_ranges, error_response = get_params("NONEXISTENT", "1.0")
        
        assert date_ranges is None
        assert error_response["statusCode"] == 400
        assert "No collections found" in error_response["body"]
    
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.GranuleQuery')
    def test_get_params_cmr_exception(self, mock_granule_query):
        """Test get_params when CMR API raises an exception"""
        # Mock GranuleQuery to raise an exception
        mock_granule_query.side_effect = Exception("CMR API unavailable")
        
        date_ranges, error_response = get_params("TEST", "1.0")
        
        assert date_ranges is None
        assert error_response["statusCode"] == 400
        assert "CMR API unavailable" in error_response["body"]


    def test_fetch_cmr_range_max_retries_exceeded(self):
        """Test CMR range fetching when max retries are exceeded"""
        from src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler import fetch_cmr_range
        
        async def run_test():
            # Mock response that always fails
            mock_response_fail = AsyncMock()
            mock_response_fail.status = 500
            mock_response_fail.text = AsyncMock(return_value="Internal Server Error")
            
            mock_session = AsyncMock()
            mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_response_fail)
            mock_session.get.return_value.__aexit__ = AsyncMock(return_value=None)
            
            result_queue = asyncio.Queue()
            fetch_stats = {"total": 0}
            params = {"short_name": "TEST", "version": "1.0"}
            
            with patch('asyncio.sleep', new_callable=AsyncMock):  # Mock sleep to speed up test
                with pytest.raises(Exception, match="Max retries reached"):
                    await fetch_cmr_range(mock_session, "https://cmr.test.com", params, result_queue, fetch_stats)
        
        asyncio.run(run_test())

    def test_send_to_sqs_success(self):
        """Test successful SQS message sending"""
        from src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler import send_to_sqs
        
        async def run_test():
            mock_sqs_client = AsyncMock()
            mock_sqs_client.send_message_batch = AsyncMock()
            
            result_queue = asyncio.Queue()
            send_stats = {"total": 0}
            
            # Add test messages to queue
            test_messages = [
                {"Id": "1", "MessageBody": "test1"},
                {"Id": "2", "MessageBody": "test2"}
            ]
            
            for msg in test_messages:
                await result_queue.put(msg)
            await result_queue.put(None)  # Signal to stop
            
            await send_to_sqs(
                mock_sqs_client, "TEST", "1.0", result_queue, 
                "https://sqs.test.com", 1, send_stats
            )
            
            assert send_stats["total"] == 2
            mock_sqs_client.send_message_batch.assert_called()
        
        asyncio.run(run_test())

    def test_send_to_sqs_batch_processing(self):
        """Test SQS batch processing with exactly 10 messages"""
        from src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler import send_to_sqs
        
        async def run_test():
            mock_sqs_client = AsyncMock()
            mock_sqs_client.send_message_batch = AsyncMock()
            
            result_queue = asyncio.Queue()
            send_stats = {"total": 0}
            
            # Add exactly 10 messages to trigger batch send
            for i in range(10):
                await result_queue.put({"Id": str(i), "MessageBody": f"test{i}"})
            await result_queue.put(None)
            
            await send_to_sqs(
                mock_sqs_client, "TEST", "1.0", result_queue, 
                "https://sqs.test.com", 1, send_stats
            )
            
            # Should have called send_message_batch at least once
            assert mock_sqs_client.send_message_batch.call_count >= 1
            assert send_stats["total"] == 10
        
        asyncio.run(run_test())

    def test_send_to_sqs_error_handling(self):
        """Test SQS error handling during batch send"""
        from src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler import send_to_sqs
        
        async def run_test():
            mock_sqs_client = AsyncMock()
            mock_sqs_client.send_message_batch = AsyncMock(side_effect=Exception("SQS Error"))
            
            result_queue = asyncio.Queue()
            send_stats = {"total": 0}
            
            # Add test message and stop signal
            await result_queue.put({"Id": "1", "MessageBody": "test1"})
            await result_queue.put(None)
            
            # Should not raise exception, logs error
            with patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.logger') as mock_logger:
                await send_to_sqs(
                    mock_sqs_client, "TEST", "1.0", result_queue, 
                    "https://sqs.test.com", 1, send_stats
                )
                
                mock_logger.error.assert_called()
        
        asyncio.run(run_test())

    def test_process_collection_success(self):
        """Test successful collection processing orchestration"""
        from src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler import process_collection
        
        async def run_test():
            partitions = [("2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z")]
            result_queue = asyncio.Queue(maxsize=100)
            
            # Mock the async functions
            with patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.fetch_cmr_range', new_callable=AsyncMock) as mock_fetch, \
                 patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.send_to_sqs', new_callable=AsyncMock) as mock_send, \
                 patch('aiohttp.ClientSession') as mock_session, \
                 patch('aioboto3.Session') as mock_boto_session:
                
                # Setup mocks
                mock_session.return_value.__aenter__ = AsyncMock()
                mock_session.return_value.__aexit__ = AsyncMock()
                
                mock_sqs_client = AsyncMock()
                mock_boto_session.return_value.client.return_value.__aenter__ = AsyncMock(return_value=mock_sqs_client)
                mock_boto_session.return_value.client.return_value.__aexit__ = AsyncMock()
                
                await process_collection(
                    partitions, "TEST", "1.0", result_queue, 
                    "https://sqs.test.com", 2, 1000
                )
                
                # Verify functions were called
                assert mock_fetch.call_count == len(partitions)
                assert mock_send.call_count == 2
        
        asyncio.run(run_test())

    def test_process_collection_exception_handling(self):
        """Test process_collection exception handling"""
        from src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler import process_collection
        
        async def run_test():
            partitions = [("2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z")]
            result_queue = asyncio.Queue(maxsize=100)
            
            # Mock fetch_cmr_range to raise an exception
            with patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.fetch_cmr_range', new_callable=AsyncMock) as mock_fetch, \
                 patch('aiohttp.ClientSession') as mock_session, \
                 patch('aioboto3.Session') as mock_boto_session, \
                 patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.logger') as mock_logger:
                
                mock_fetch.side_effect = Exception("CMR fetch failed")
                
                # Setup session mocks
                mock_session.return_value.__aenter__ = AsyncMock()
                mock_session.return_value.__aexit__ = AsyncMock()
                
                mock_sqs_client = AsyncMock()
                mock_boto_session.return_value.client.return_value.__aenter__ = AsyncMock(return_value=mock_sqs_client)
                mock_boto_session.return_value.client.return_value.__aexit__ = AsyncMock()

                await process_collection(
                    partitions, "TEST", "1.0", result_queue, 
                    "https://sqs.test.com", 2, 1000
                )
                
                # Verify that the error was logged when the exception occurred
                mock_logger.error.assert_called()
                
                # Verify the error message contains the expected text
                error_calls = [call for call in mock_logger.error.call_args_list 
                              if "Failed to process collection" in str(call)]
                assert len(error_calls) > 0, "Expected error log about failed collection processing"
        
        asyncio.run(run_test())

    def test_build_message_missing_fields(self):
        """Test build_message with missing granule fields"""
        granule = {"id": "G1"}  # Missing time fields
        short_name = "TEST_COLLECTION"
        version = "1_0"
        
        message = build_message(granule, short_name, version)
        
        assert message["Id"] == "G1"
        message_body = json.loads(message["MessageBody"])
        inner_message = json.loads(message_body["Message"])
        assert inner_message["record"]["beginningDateTime"] == ""
        assert inner_message["record"]["endingDateTime"] == ""

    def test_split_date_ranges_single_range(self):
        """Test split_date_ranges with num_ranges=1"""
        start_date = "2000-01-01T00:00:00Z"
        end_date = "2000-01-05T00:00:00Z"
        
        ranges = split_date_ranges(start_date, end_date, 1)
        
        assert len(ranges) == 1
        assert ranges[0][0] == start_date
        assert ranges[0][1] == end_date

    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.CollectionQuery')
    def test_get_params_no_end_date(self, mock_collection_query):
        """Test get_params when collection has no end date"""
        with patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.GranuleQuery') as mock_granule_query:
            # Mock GranuleQuery
            mock_granule_api = MagicMock()
            mock_granule_api.hits.return_value = 500
            mock_granule_query.return_value = mock_granule_api
            
            # Mock CollectionQuery with no end date
            mock_collection_api = MagicMock()
            mock_collection_api.get_all.return_value = [{
                "time_start": "2020-01-01T00:00:00Z",
                "time_end": None  # No end date
            }]
            mock_collection_query.return_value = mock_collection_api
            
            with patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.datetime') as mock_datetime:
                mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T00:00:00"
                
                date_ranges, n_consumers, queue_size, num_granules = get_params("TEST", "1.0")
                
                assert date_ranges is not None
                assert num_granules == 500

    def test_send_to_sqs_logs_batch_exceptions(self):
        """Test that SQS batch sending exceptions are logged."""
        from src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler import send_to_sqs
        
        async def run_test():
            mock_sqs_client = AsyncMock()
            mock_sqs_client.send_message_batch.side_effect = Exception("SQS Batch Error")
            
            result_queue = asyncio.Queue()
            
            # Add exactly 10 messages to trigger batch processing (len(batch) >= 10)
            for i in range(10):
                await result_queue.put({"Id": str(i), "MessageBody": f"test{i}"})
            await result_queue.put(None)  # Stop signal
            
            with patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.logger') as mock_logger:
                await send_to_sqs(mock_sqs_client, "TEST", "1.0", result_queue, "url", 1, {"total": 0})
                mock_logger.error.assert_called_with("Error sending batch to SQS: SQS Batch Error")
        
        asyncio.run(run_test())

@pytest.mark.asyncio 
async def test_max_retries_exceeded():
    """Test max retries exceeded raises exception."""
    params = {"short_name": "TEST", "version": "001"}
    result_queue = AsyncMock()
    fetch_stats = {"total": 0}
    
    # Mock error response
    error_response = MagicMock()
    error_response.status = 404
    error_response.text = AsyncMock(return_value="Not Found")
    
    # Mock all requests fail
    mock_session = MagicMock()
    mock_session.get.return_value = MagicMock(__aenter__=AsyncMock(return_value=error_response))
    
    with patch('asyncio.sleep', new_callable=AsyncMock):
        with pytest.raises(Exception, match="Max retries reached"):
            await fetch_cmr_range(mock_session, "url", params, result_queue, fetch_stats)

@pytest.mark.asyncio
async def test_empty_granules_return():
    """Test early return when no granules found."""
    params = {"short_name": "TEST", "version": "001"}
    result_queue = AsyncMock()
    fetch_stats = {"total": 0}
    
    # Mock response with empty granules
    empty_response = MagicMock()
    empty_response.status = 200
    empty_response.json = AsyncMock(return_value={"feed": {"entry": []}})
    
    mock_session = MagicMock()
    mock_session.get.return_value = MagicMock(__aenter__=AsyncMock(return_value=empty_response))
    
    await fetch_cmr_range(mock_session, "url", params, result_queue, fetch_stats)
    
    assert result_queue.put.call_count == 0
    assert fetch_stats["total"] == 0


@pytest.mark.asyncio
async def test_exception_retry_then_success():
    """Test exception retry logic."""
    params = {"short_name": "TEST", "version": "001"}
    result_queue = AsyncMock()
    fetch_stats = {"total": 0}
    
    # Mock successful response
    success_response = MagicMock()
    success_response.status = 200
    success_response.json = AsyncMock(return_value={"feed": {"entry": []}})
    
    mock_session = MagicMock()
    # First call raises exception, second succeeds
    mock_session.get.side_effect = [
        Exception("Connection failed"),
        MagicMock(__aenter__=AsyncMock(return_value=success_response))
    ]
    
    with patch('asyncio.sleep', new_callable=AsyncMock):
        await fetch_cmr_range(mock_session, "url", params, result_queue, fetch_stats)


@pytest.mark.asyncio
async def test_exception_max_retries():
    """Test exception max retries exceeded."""
    params = {"short_name": "TEST", "version": "001"}
    result_queue = AsyncMock()
    fetch_stats = {"total": 0}
    
    mock_session = MagicMock()
    mock_session.get.side_effect = Exception("Connection failed")
    
    with patch('asyncio.sleep', new_callable=AsyncMock):
        with pytest.raises(Exception, match="Max retries reached"):
            await fetch_cmr_range(mock_session, "url", params, result_queue, fetch_stats)


@pytest.mark.asyncio
async def test_granule_processing_section():
    """Test the granule processing and enqueuing section."""
    params = {"short_name": "TEST", "version": "001"}
    result_queue = AsyncMock()
    fetch_stats = {"total": 0}
    
    # Mock successful response with 2 granules
    success_response = MagicMock()
    success_response.status = 200
    success_response.json = AsyncMock(return_value={
        "feed": {
            "entry": [
                {"id": "granule1"},
                {"id": "granule2"}
            ]
        }
    })
    success_response.headers.get.return_value = None  # No search_after triggers return
    
    # Mock session with proper async context manager
    mock_session = MagicMock()
    mock_session.get.return_value = MagicMock(
        __aenter__=AsyncMock(return_value=success_response),
        __aexit__=AsyncMock(return_value=None)
    )
    
    # Mock build_message to return predictable values
    with patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.build_message') as mock_build:
        mock_build.side_effect = [{"msg": "1"}, {"msg": "2"}]
        
        await fetch_cmr_range(mock_session, "url", params, result_queue, fetch_stats)
    
    # Verify granule processing
    assert result_queue.put.call_count == 2  # One call per granule
    assert fetch_stats["total"] == 2  # len(granules) = 2
    
    # Verify build_message was called correctly for each granule
    mock_build.assert_any_call({"id": "granule1"}, "TEST", "001")
    mock_build.assert_any_call({"id": "granule2"}, "TEST", "001")
    
    # Verify messages were enqueued
    result_queue.put.assert_any_call({"msg": "1"})
    result_queue.put.assert_any_call({"msg": "2"})


@pytest.mark.asyncio
async def test_break_statement_coverage():
    """Test to hit the break statement in the granule processing section."""
    params = {"short_name": "TEST", "version": "001"}
    result_queue = AsyncMock()
    fetch_stats = {"total": 0}
    
    # First response with search_after header (triggers break, not return)
    first_response = MagicMock()
    first_response.status = 200
    first_response.json = AsyncMock(return_value={
        "feed": {"entry": [{"id": "granule1"}]}
    })
    first_response.headers.get.return_value = "token123"  # Has search_after
    
    # Second response without search_after (triggers return)
    second_response = MagicMock()
    second_response.status = 200
    second_response.json = AsyncMock(return_value={
        "feed": {"entry": [{"id": "granule2"}]}
    })
    second_response.headers.get.return_value = None  # No search_after
    
    # Mock session to return different responses
    mock_session = MagicMock()
    mock_session.get.side_effect = [
        MagicMock(__aenter__=AsyncMock(return_value=first_response), __aexit__=AsyncMock()),
        MagicMock(__aenter__=AsyncMock(return_value=second_response), __aexit__=AsyncMock())
    ]
    
    with patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.build_message') as mock_build:
        mock_build.side_effect = [{"msg": "1"}, {"msg": "2"}]
        
        await fetch_cmr_range(mock_session, "url", params, result_queue, fetch_stats)
    
    # Verify both pages were processed
    assert result_queue.put.call_count == 2
    assert fetch_stats["total"] == 2