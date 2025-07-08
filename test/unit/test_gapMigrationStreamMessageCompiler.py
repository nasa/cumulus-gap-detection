import pytest
import json
import os
from unittest.mock import patch, MagicMock

from src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler import (
    lambda_handler, split_date_ranges, build_message
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
    
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.get_params')
    @patch('src.gapMigrationStreamMessageCompiler.gapMigrationStreamMessageCompiler.loop')
    @patch('os.getenv')
    def test_lambda_handler_success(self, mock_getenv, mock_loop, mock_get_params, event, context):
        mock_getenv.return_value = "https://sqs.example.com/test-queue"
        partitions = [
            ("2000-01-01T00:00:00Z", "2000-01-02T00:00:00Z"),
            ("2000-01-02T00:00:00Z", "2000-01-03T00:00:00Z")
        ]
        mock_get_params.return_value = (partitions, 2, 4000, 100)
        
        result = lambda_handler(event, context)
        
        assert result["statusCode"] == 200
        assert "Processing complete" in result["body"]
        mock_loop.run_until_complete.assert_called_once()
    
    @patch('os.getenv')
    def test_lambda_handler_missing_params(self, mock_getenv, context):
        mock_getenv.return_value = "https://sqs.example.com/test-queue"
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


