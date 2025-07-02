import os
import pytest
from unittest.mock import patch, mock_open

from src.gapCreateTable.gapCreateTable import lambda_handler

@patch("src.gapCreateTable.gapCreateTable.validate_environment_variables")
@patch("builtins.open", new_callable=mock_open, read_data="CREATE TABLE test_table (id SERIAL PRIMARY KEY);")
@patch("os.path.abspath")
@patch("os.path.dirname")
def test_lambda_handler_success(
    mock_dirname,
    mock_abspath,
    mock_open_file,
    mock_validate_env,
    setup_test_data
):
    mock_dirname.return_value = "/fakepath"
    mock_abspath.return_value = "/fakepath/lambda.py"
    
    lambda_handler({}, {})

    mock_validate_env.assert_called_once_with(["RDS_SECRET", "RDS_PROXY_HOST"])

    # Check the correct file was opened
    mock_open_file.assert_called_once_with("/fakepath/gap_schema.sql")
