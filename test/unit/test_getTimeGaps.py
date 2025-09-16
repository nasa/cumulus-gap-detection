import os
import sys
import json
import boto3
from botocore.exceptions import ClientError
import pytest
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import re

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.append(project_root)
from src.getTimeGaps.getTimeGaps import lambda_handler, check_gap_config, check_date_format, fetch_time_gaps, sanitize_versionid, get_granule_gap, compare_dates

def normalize_sql(sql):
    return re.sub(r'\s+', ' ', sql).strip()

class Test_sanitize_version_id(unittest.TestCase):
    
    # Check if version ID is properly sanitized of '.'
    def test_sanitize_versionid(self):
        assert sanitize_versionid('1.0') == '1_0'
        assert sanitize_versionid('v1.2.3') == 'v1_2_3'

class Test_check_date_format(unittest.TestCase):
    
    # Checks if date is in format YEAR-MONTH-DAY
    def test_check_date_format(self):
        assert check_date_format('2021-01-01') == True
        assert check_date_format('false') == False
        assert check_date_format('01-01-2021') == False

class Test_fetch_time_gaps(unittest.TestCase):
    
    # Checks if SQL query for time gaps is valid when all fields are passed
    import os
import sys
import json
import boto3
from botocore.exceptions import ClientError
import pytest
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import re

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.append(project_root)

from src.getTimeGaps.getTimeGaps import lambda_handler, check_gap_config, check_date_format, fetch_time_gaps, sanitize_versionid, get_granule_gap, compare_dates

def normalize_sql(sql):
    return re.sub(r'\s+', ' ', sql).strip()

class Test_sanitize_version_id(unittest.TestCase):
    # Check if version ID is properly sanitized of '.'
    def test_sanitize_versionid(self):
        assert sanitize_versionid('1.0') == '1_0'
        assert sanitize_versionid('v1.2.3') == 'v1_2_3'

class Test_check_date_format(unittest.TestCase):
    # Checks if date is in format YEAR-MONTH-DAY
    def test_check_date_format(self):
        assert check_date_format('2021-01-01') == True
        assert check_date_format('false') == False
        assert check_date_format('01-01-2021') == False

class Test_fetch_time_gaps(unittest.TestCase):
    # Checks if SQL query for time gaps is valid when all fields are passed
    def test_fetch_time_gaps(self):
        short_name = "test_short_name"
        versionid = "1.0"
        sanitized_name = "test_short_name___1_0"
        startDate = "2020-01-01"
        endDate = "2023-01-01"
        granuleGap = 2
        knownCheck = True
        format_string = "%Y-%m-%d"

        cursor_mock = MagicMock()
        cursor_mock.fetchall.return_value = [
            (datetime.strptime('2021-01-01', format_string), datetime.strptime('2021-01-02', format_string), None),
            (datetime.strptime('2021-02-01', format_string), datetime.strptime('2021-02-02', format_string), None)
        ]

        # Call fetch_time_gaps
        result = fetch_time_gaps(short_name, versionid, granuleGap, cursor_mock, knownCheck, startDate, endDate)

        # Verify the function was called with correct parameters
        cursor_mock.execute.assert_called_once()
        actual_sql, actual_params = cursor_mock.execute.call_args[0]

        # Check that the SQL contains expected components based on the actual query structure
        assert "WITH params AS" in actual_sql
        assert "SELECT %s as collection_id" in actual_sql
        assert "FROM gaps g" in actual_sql
        assert "LEFT JOIN reasons r ON" in actual_sql
        assert "CROSS JOIN params p" in actual_sql
        assert "WHERE g.collection_id = p.collection_id" in actual_sql
        assert "EXTRACT(EPOCH FROM (g.end_ts - g.start_ts)) >= p.tolerance" in actual_sql
        
        # Since knownCheck is True, should filter for gaps without reasons
        assert "r.reason IS NULL" in actual_sql
        
        # Should order by start timestamp
        assert "ORDER BY start_ts" in actual_sql

        # Check parameters are passed correctly
        expected_params = [sanitized_name, startDate, endDate, granuleGap]
        assert actual_params == expected_params

        # Verify the result is returned correctly
        assert len(result) == 2

    def test_fetch_time_gaps_year_9999_replacement(self):
        """Test that year 9999 is replaced with current datetime"""
        short_name = "test_short_name"
        versionid = "1.0"
        granuleGap = 2
        knownCheck = False
        cursor_mock = MagicMock()
        
        # Mock result with year 9999 in the last row
        mock_end_time = datetime(9999, 2, 2)
        cursor_mock.fetchall.return_value = [
            (datetime(2021, 1, 1), datetime(2021, 1, 2), None),
            (datetime(2021, 2, 1), mock_end_time, None)  # Year 9999 case
        ]
        
        result = fetch_time_gaps(short_name, versionid, granuleGap, cursor_mock, knownCheck)
        
        # Verify that the year 9999 was replaced with current datetime
        assert len(result) == 2
        assert result[0] == (datetime(2021, 1, 1), datetime(2021, 1, 2), None)
        # The last row should have its end_ts replaced with current datetime
        assert result[1][0] == datetime(2021, 2, 1)
        assert result[1][1].year != 9999  # Should be replaced
        assert abs((result[1][1] - datetime.now()).total_seconds()) < 5  # Should be very recent

class Test_check_gap_config(unittest.TestCase):
    
    # Tests gap config for true result
    def test_check_gap_config_true(self):
        collection_id = "testCollection___123"
        cursor_mock = MagicMock()
        cursor_mock.fetchone.return_value = [True]
        
        expected_sql = """ SELECT EXISTS (
        SELECT 1 
        FROM collections
        WHERE collection_id = %s
        )"""
        
        result = check_gap_config(collection_id, cursor_mock)
        
        actual_sql, actual_params = cursor_mock.execute.call_args[0]
        
        assert normalize_sql(actual_sql) == normalize_sql(expected_sql)
        assert actual_params == (collection_id,)
        assert result == True

    # Tests gap config for false result
    def test_check_gap_config_false(self):
        collection_id = "testCollection___123"
        cursor_mock = MagicMock()
        cursor_mock.fetchone.return_value = [False]
        
        expected_sql = """ SELECT EXISTS (
        SELECT 1 
        FROM collections 
        WHERE collection_id = %s
        )"""
        
        result = check_gap_config(collection_id, cursor_mock)
        actual_sql, actual_params = cursor_mock.execute.call_args[0]
        
        assert normalize_sql(actual_sql) == normalize_sql(expected_sql)
        assert actual_params == (collection_id,)
        assert result == False

class Test_get_granule_gap(unittest.TestCase):
    
    # Test if tolerance value is available in table
    @patch.dict(os.environ, {'TOLERANCE_TABLE': 'test-table'})
    @patch('boto3.resource')
    def test_get_granule_gap_success(self, mock_boto):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            'Item': {
                'short_name': 'testCollection',
                'versionid': '123',
                'granulegap': 2
            }
        }
        mock_boto.return_value.Table.return_value = mock_table
        result = get_granule_gap('testCollection', '123')
        
        assert result == 2

    # Test if tolerance value is missing from table
    @patch.dict(os.environ, {'TOLERANCE_TABLE': 'test-table'})
    @patch('boto3.resource')
    def test_get_granule_gap_no_result(self, mock_boto):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {} # Empty response
        mock_boto.return_value.Table.return_value = mock_table
        result = get_granule_gap('testCollection', '123')
        
        assert result == 0

    # Test if it returns error
    @patch.dict(os.environ, {'TOLERANCE_TABLE': 'test-table'})
    @patch('boto3.resource')
    def test_get_granule_gap_error(self, mock_boto):
        mock_table = MagicMock()
        mock_table.get_item.side_effect = ClientError(
                error_response={'Error': {'Code': '500', 'Message': 'Internal error'}},
                operation_name='GetItem'
            )
        
        mock_boto.return_value.Table.return_value = mock_table
        with self.assertRaises(ClientError):
            get_granule_gap('testCollection', '123')

class Test_compare_dates(unittest.TestCase):
    
    def test_compare_dates(self):
        startDate = "2024-10-31"
        endDate = "2024-12-25"
        
        assert compare_dates(startDate, endDate) == True
        assert compare_dates(startDate, startDate) == True
        assert compare_dates(endDate, startDate) == False

class Test_get_presigned_url(unittest.TestCase):
    
    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-gap-bucket', 'AWS_REGION': 'us-west-2'})
    @patch('boto3.client')
    @patch('src.getTimeGaps.getTimeGaps.datetime')
    def test_get_presigned_url_success(self, mock_datetime, mock_boto_client):
        from src.getTimeGaps.getTimeGaps import get_presigned_url
        
        # Mock datetime.now() to return predictable timestamp
        mock_datetime.now.return_value.strftime.return_value = "20230101120000"
        
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        
        # Mock presigned URL response
        expected_url = "https://test-gap-bucket.s3.amazonaws.com/gaps/test_collection/20230101120000.json?signature=abc123"
        mock_s3_client.generate_presigned_url.return_value = expected_url
        
        # Test data
        test_data = {"timeGaps": [["2023-01-01", "2023-01-02"]], "gapTolerance": 5}
        collection_id = "test_collection"
        
        # Call function
        result = get_presigned_url(test_data, collection_id)
        
        # Verify S3 client was created correctly
        mock_boto_client.assert_called_once_with("s3")
        
        # Verify put_object was called with correct parameters
        mock_s3_client.put_object.assert_called_once()
        put_object_call = mock_s3_client.put_object.call_args
        
        assert put_object_call[1]['Bucket'] == 'test-gap-bucket'
        assert put_object_call[1]['Key'] == 'gaps/test_collection/20230101120000.json'
        assert put_object_call[1]['ContentType'] == 'application/json'
        
        # Verify the body contains the JSON data
        import json
        body_data = json.loads(put_object_call[1]['Body'])
        assert body_data == test_data
        
        # Verify generate_presigned_url was called
        mock_s3_client.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": "test-gap-bucket", "Key": "gaps/test_collection/20230101120000.json"},
            ExpiresIn=3600
        )
        
        # Verify the function returns the presigned URL
        assert result == expected_url

    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-gap-bucket', 'AWS_REGION': 'us-west-2'})
    @patch('boto3.client')
    @patch('src.getTimeGaps.getTimeGaps.datetime')
    def test_get_presigned_url_s3_error(self, mock_datetime, mock_boto_client):
        from src.getTimeGaps.getTimeGaps import get_presigned_url
        
        # Mock datetime
        mock_datetime.now.return_value.strftime.return_value = "20230101120000"
        
        # Mock S3 client to raise an exception
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.put_object.side_effect = Exception("S3 Error")
        
        # Test data
        test_data = {"timeGaps": [["2023-01-01", "2023-01-02"]]}
        collection_id = "test_collection"
        
        # Verify exception is raised
        with self.assertRaises(Exception) as context:
            get_presigned_url(test_data, collection_id)
        
        assert str(context.exception) == "S3 Error"
        
        # Verify put_object was attempted
        mock_s3_client.put_object.assert_called_once()

    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-gap-bucket', 'AWS_REGION': 'us-west-2'})
    @patch('boto3.client')
    @patch('src.getTimeGaps.getTimeGaps.datetime')
    def test_get_presigned_url_generate_url_error(self, mock_datetime, mock_boto_client):
        from src.getTimeGaps.getTimeGaps import get_presigned_url
        
        # Mock datetime
        mock_datetime.now.return_value.strftime.return_value = "20230101120000"
        
        # Mock S3 client - put_object succeeds but generate_presigned_url fails
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.generate_presigned_url.side_effect = Exception("URL Generation Error")
        
        # Test data
        test_data = {"timeGaps": [["2023-01-01", "2023-01-02"]]}
        collection_id = "test_collection"
        
        # Verify exception is raised
        with self.assertRaises(Exception) as context:
            get_presigned_url(test_data, collection_id)
        
        assert str(context.exception) == "URL Generation Error"
        
        # Verify both operations were attempted
        mock_s3_client.put_object.assert_called_once()
        mock_s3_client.generate_presigned_url.assert_called_once()
    
class Test_lambda_handler(unittest.TestCase):
    
    # Test if lambda filters are applied and it succeeds
    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    @patch('src.getTimeGaps.getTimeGaps.get_db_connection')
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    # Test if all filters are applied and the query is successful 
    def test_lambda_handler_all_filters_success(self, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps, mock_get_db_connection):
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123",
                "knownGap": "True",
                "tolerance": "True",
                "startDate": "2021-01-01",
                "endDate": "2025-01-01"
            }
        }
        
        mock_check_gap_config.return_value = True
        mock_get_granule_gap.return_value = 3
        mock_fetch_time_gaps.return_value = [['2025-01-01 00:00:00', '2025-01-01 01:00:00']]
        
        # Mock database connection
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 200,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "timeGaps": [['2025-01-01 00:00:00', '2025-01-01 01:00:00']],
                "gapTolerance": 3
            }, default=str)
        }
    
    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    @patch('src.getTimeGaps.getTimeGaps.get_db_connection')
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    # Test if all filters are disabled and the query is successful
    def test_lambda_handler_all_filters_disabled_success(self, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps, mock_get_db_connection):
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123",
                "knownGap": "False",
                "tolerance": "False"
            }
        }
        
        mock_check_gap_config.return_value = True
        mock_get_granule_gap.return_value = 3
        mock_fetch_time_gaps.return_value = [['2025-01-01 00:00:00', '2025-01-01 01:00:00']]
        
        # Mock database connection
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 200,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "timeGaps": [['2025-01-01 00:00:00', '2025-01-01 01:00:00']],
                "gapTolerance": 0
            }, default=str)
        }
    
    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    @patch('src.getTimeGaps.getTimeGaps.get_db_connection')
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    # Test if there are no gaps returned 
    def test_lambda_handler_no_gaps(self, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps, mock_get_db_connection):
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123",
                "knownGap": "False",
                "tolerance": "False"
            }
        }
        
        mock_check_gap_config.return_value = True
        mock_get_granule_gap.return_value = 3
        mock_fetch_time_gaps.return_value = []
        
        # Mock database connection
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 200,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({'message': "No qualifying time gaps found."}, default=str)
        }
    
    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    @patch('src.getTimeGaps.getTimeGaps.get_db_connection')
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    # Test if collection is not configured yet
    def test_lambda_handler_collection_not_configured(self, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps, mock_get_db_connection):
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123"
            }
        }
        
        mock_check_gap_config.return_value = False
        
        # Mock database connection
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 400,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({'message': "Collection testCollection___123 has not been initialized for gap detection."}, default=str)
        }
    
    # Test if tolerance value is invalid
    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    def test_lambda_handler_invalid_tolerance(self):
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123",
                "tolerance": "Invalid"
            }
        }
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 400,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Bad request: Tolerance flag should be either 'true' or 'false'"
            }, default=str)
        }
    
    # Test if known value is invalid    
    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    def test_lambda_handler_invalid_known(self):
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123",
                "knownGap": "Invalid"
            }
        }
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 400,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Bad request: Known gap flag should be either 'true' or 'false'"
            }, default=str)
        }
    
    # Test if startDate is invalid
    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    def test_lambda_handler_invalid_startDate(self):
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123",
                "startDate": "Invalid"
            }
        }
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 400,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Bad request: Start date needs to be in format YEAR-MONTH-DAY"
            }, default=str)
        }
    
    # Test if endDate is invalid
    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    def test_lambda_handler_invalid_endDate(self):
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123",
                "endDate": "Invalid"
            }
        }
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 400,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Bad request: End date needs to be in format YEAR-MONTH-DAY"
            }, default=str)
        }
    
    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    # Test if startDate > endDate
    def test_lambda_handler_startDate_greater_than_endDate(self, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps):
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123",
                "startDate": "2025-01-01",
                "endDate": "2021-01-01"
            }
        }
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 400,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Bad request: Start date is greater than end date"
            }, default=str)
        }

    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    @patch('src.getTimeGaps.getTimeGaps.get_db_connection')
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    # Test if fetch_time_gaps returns an error
    def test_lambda_handler_fetch_time_gaps_error(self, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps, mock_get_db_connection):
    
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123",
                "tolerance": "True"
            }
        }
        
        mock_check_gap_config.return_value = True
        mock_get_granule_gap.return_value = 3
        mock_fetch_time_gaps.side_effect = Exception("Fetch_time_gaps error")
        
        # Mock database connection
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 500,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Failed to fetch time gaps: Fetch_time_gaps error"
            }, default=str)
        }

    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    @patch('src.getTimeGaps.getTimeGaps.get_db_connection')
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    @patch('src.getTimeGaps.getTimeGaps.get_presigned_url')
    def test_lambda_handler_large_response_presigned_url_success(self, mock_get_presigned_url, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps, mock_get_db_connection):
        """Test lambda handler when response size exceeds threshold and presigned URL is generated successfully"""
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123",
                "tolerance": "True"
            }
        }
        
        mock_check_gap_config.return_value = True
        mock_get_granule_gap.return_value = 3
        
        # Create a large response that definitely exceeds 6MB threshold
        large_gaps = []
        for i in range(150000):  # exceed 6MB
            large_gaps.append([f'2025-01-{i%28+1:02d} 00:00:00.000000', f'2025-01-{i%28+1:02d} 01:00:00.000000'])
        
        mock_fetch_time_gaps.return_value = large_gaps
        mock_get_presigned_url.return_value = "https://test-bucket.s3.amazonaws.com/presigned-url"
        
        # Mock database connection
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        
        result = lambda_handler(test_event, context={})
        
        # Verify presigned URL was called
        mock_get_presigned_url.assert_called_once()
        
        # Check the arguments passed to get_presigned_url
        call_args = mock_get_presigned_url.call_args[0]
        expected_body = {"timeGaps": large_gaps, "gapTolerance": 3}
        expected_collection_id = "testCollection___123"
        
        assert call_args[0] == expected_body
        assert call_args[1] == expected_collection_id
        
        # Verify the response
        assert result == {
            'statusCode': 200,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Too many results for response, use the presigned URL",
                "presigned_url": "https://test-bucket.s3.amazonaws.com/presigned-url"
            }, default=str)
        }

    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    @patch('src.getTimeGaps.getTimeGaps.get_db_connection')
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    @patch('src.getTimeGaps.getTimeGaps.get_presigned_url')
    def test_lambda_handler_large_response_presigned_url_error(self, mock_get_presigned_url, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps, mock_get_db_connection):
        """Test lambda handler when response size exceeds threshold but presigned URL generation fails"""
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123",
                "tolerance": "True"
            }
        }
        
        mock_check_gap_config.return_value = True
        mock_get_granule_gap.return_value = 3
        
        # Create a large response that definitely exceeds 6MB threshold
        large_gaps = []
        for i in range(150000):  # exceed 6MB
            large_gaps.append([f'2025-01-{i%28+1:02d} 00:00:00.000000', f'2025-01-{i%28+1:02d} 01:00:00.000000'])
        
        mock_fetch_time_gaps.return_value = large_gaps
        mock_get_presigned_url.side_effect = Exception("S3 upload failed")
        
        # Mock database connection
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db_connection.return_value.__enter__.return_value = mock_conn
        
        result = lambda_handler(test_event, context={})
        
        # Verify presigned URL was attempted
        mock_get_presigned_url.assert_called_once()
        
        # Verify the error response
        assert result == {
            'statusCode': 500,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Failed to generate results URL: S3 upload failed"
            }, default=str)
        }

    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    def test_lambda_handler_missing_shortname(self):
        """Test lambda handler when shortname parameter is missing"""
        test_event = {
            'queryStringParameters': {
                "version": "123"
                # Missing short_name
            }
        }
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 400,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Missing query parameters: shortname or versionid"
            }, default=str)
        }

    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    def test_lambda_handler_missing_version(self):
        """Test lambda handler when version parameter is missing"""
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection"
                # Missing version
            }
        }
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 400,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Missing query parameters: shortname or versionid"
            }, default=str)
        }

    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    def test_lambda_handler_missing_both_parameters(self):
        """Test lambda handler when both shortname and version parameters are missing"""
        test_event = {
            'queryStringParameters': {
                # Missing both short_name and version
            }
        }
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 400,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Missing query parameters: shortname or versionid"
            }, default=str)
        }

    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    def test_lambda_handler_null_query_parameters(self):
        """Test lambda handler when queryStringParameters is None"""
        test_event = {
            'queryStringParameters': None
        }
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 400,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Missing query parameters: shortname or versionid"
            }, default=str)
        }

    @patch.dict(os.environ, {'GAP_RESPONSE_BUCKET': 'test-bucket'})
    def test_lambda_handler_empty_string_parameters(self):
        """Test lambda handler when parameters are empty strings"""
        test_event = {
            'queryStringParameters': {
                "short_name": "",
                "version": ""
            }
        }
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 400,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({
                "message": "Missing query parameters: shortname or versionid"
            }, default=str)
        }