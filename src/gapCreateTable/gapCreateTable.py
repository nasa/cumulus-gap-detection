import boto3
import csv
import os
import psycopg
from datetime import datetime
import logging
import json
from botocore.exceptions import ClientError
from utils import get_db_connection, validate_environment_variables

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Lambda handler function
def lambda_handler(event, context):

    validate_environment_variables(["RDS_SECRET", "RDS_PROXY_HOST"])
    current_dir = os.path.dirname(os.path.abspath(__file__))
    query_path = os.path.join(current_dir, "gap_schema.sql")
    with open(query_path) as f:
        init_gaps = f.read()

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(init_gaps)
