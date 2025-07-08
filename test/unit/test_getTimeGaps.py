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

# Cleans sql of white spaces to do clean comparison
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
        
        expected_sql = f"""
        SELECT start_ts, end_ts FROM gaps
        WHERE collection_id = %s
        AND end_ts - start_ts > %s::INTERVAL
        AND reason IS NULL
        AND start_ts > '{startDate}'
        AND end_ts < '{endDate}'
        ORDER BY start_ts;
        """
        
        cursor_mock = MagicMock()
        
        cursor_mock.fetchall.return_value = [(datetime.strptime('2021-01-01', format_string), datetime.strptime('2021-01-02', format_string)), (datetime.strptime('2021-02-01', format_string), datetime.strptime('9999-02-02', format_string))]
        
        result = fetch_time_gaps(short_name, versionid, granuleGap, knownCheck, startDate, endDate, cursor_mock)
        
        actual_sql, actual_params = cursor_mock.execute.call_args[0]
        
        assert normalize_sql(expected_sql) == normalize_sql(actual_sql)
        assert actual_params == (sanitized_name, f"{granuleGap} seconds")

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
    
class Test_lambda_handler(unittest.TestCase):
    
    # Test if lambda filters are applied and it succeeds
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    # Test if all filters are applied and the query is successful 
    def test_lambda_handler_all_filters_success(self, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps):
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
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
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
    
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    # Test if all filters are disabled and the query is successful
    def test_lambda_handler_all_filters_disabled_success(self, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps):
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
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
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
    
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    # Test if there are no gaps returned 
    def test_lambda_handler_no_gaps(self, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps):
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
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        result = lambda_handler(test_event, context={})
        
        assert result == {
            'statusCode': 200,
            'headers': {
                'content-type': 'application/json',
                'access-control-allow-origin': '*'
            },
            'body': json.dumps({'message': "No qualifying time gaps found."}, default=str)
        }
    
    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    # Test if collection is not configured yet
    def test_lambda_handler_collection_not_configured(self, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps):
        test_event = {
            'queryStringParameters': {
                "short_name": "testCollection",
                "version": "123"
            }
        }
        
        mock_check_gap_config.return_value = False
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
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

    @patch('src.getTimeGaps.getTimeGaps.fetch_time_gaps')
    @patch('src.getTimeGaps.getTimeGaps.get_granule_gap')
    @patch('src.getTimeGaps.getTimeGaps.check_gap_config')
    # Test if fetch_time_gaps returns an error
    def test_lambda_handler_fetch_time_gaps_error(self, mock_check_gap_config, mock_get_granule_gap, mock_fetch_time_gaps):
    
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
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
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
