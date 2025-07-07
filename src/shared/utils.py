import boto3
import time
import json
import os
import psycopg
import logging
from typing import Dict, List
from datetime import datetime
from psycopg_pool import ConnectionPool, PoolTimeout
from contextlib import contextmanager

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Module-level connection pool
_pool = None


def validate_environment_variables(required_vars: List[str]) -> None:
    """Validate that required environment variables are present."""
    for var in required_vars:
        if var not in os.environ:
            logger.error(f"Required variable {var} not in environment")
            raise KeyError(f"Required variable {var} not in environment")

    if "CMR_ENV" in required_vars:
        cmr_envs = ["SIT", "UAT", "PROD"]
        cmr_env = os.getenv("CMR_ENV")
        if cmr_env not in cmr_envs:
            logger.error(f"CMR environnemt not recognized: {cmr_env} not in {cmr_envs}")
            raise KeyError(
                f"CMR environnemt not recognized: {cmr_env} not in {cmr_envs}"
            )


def get_db_config(db_secret_id: str) -> Dict[str, str]:
    """Retrieve database configuration from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=os.getenv("AWS_REGION")
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=db_secret_id)
    except Exception as e:
        logger.error(f"Failed to retrieve secret from Secrets Manager: {e}")
        raise e

    secret = get_secret_value_response["SecretString"]
    return json.loads(secret)


def get_connection_pool() -> ConnectionPool:
    """Get or initialize the connection pool."""
    global _pool

    if _pool is None:
        db_config = get_db_config(os.getenv("RDS_SECRET"))
        logger.info(f"Initializing connection pool to: {os.getenv('RDS_PROXY_HOST')}")

        conn_str = (
            f"host={os.getenv('RDS_PROXY_HOST')} "
            f"dbname={db_config['database']} "
            f"user={db_config['username']} "
            f"password={db_config['password']} "
            f"connect_timeout=5 "
            f"keepalives=1 "
            f"keepalives_idle=15 "
            f"keepalives_interval=5 "
            f"keepalives_count=3 "
            f"application_name=gap_update_lambda"
        )

        _pool = ConnectionPool(
            conninfo=conn_str,
            min_size=1,  # Maintain an idle connection
            max_size=10,
            max_lifetime=7200,  # 2 hours
            max_idle=900,  # 15 minute idle timeout
        )

        logger.info("Connection pool initialized")

    return _pool


@contextmanager
def get_db_connection(retry_count=3):
    """Get a connection from the pool with retry and ensure it's returned when done.

    This function should be used with a 'with' statement.

    Yields:
        psycopg.Connection: A database connection from the pool
    """
    pool = get_connection_pool()
    conn = None

    for attempt in range(retry_count):
        try:
            conn = pool.getconn(timeout=10)

            # Validate connection is usable
            with conn.cursor() as cur:
                cur.execute("SELECT 1")

            # Connection is good, yield it
            try:
                yield conn
                if not conn.closed:
                    conn.commit()
            except Exception as e:
                if not conn.closed:
                    conn.rollback()
                raise
            finally:
                if conn and not conn.closed:
                    pool.putconn(conn)

            return

        except (psycopg.OperationalError, PoolTimeout) as e:
            if conn and not conn.closed:
                try:
                    pool.putconn(conn)
                except:
                    pass

            if attempt < retry_count - 1:
                logger.warning(
                    f"DB connection error (attempt {attempt+1}/{retry_count}): {str(e)}"
                )
                time.sleep(0.2 * (2**attempt))
            else:
                logger.error(
                    f"Failed to get working connection after {retry_count} attempts"
                )
                raise


def sanitize_versionid(versionid) -> str:
    """
    Converts a collection ID by replacing '.' with '_'.

    Args:
        versionid (str): The version ID to sanitize.

    Returns:
        str: The sanitized version ID.
    """
    return versionid.replace(".", "_")


def get_granule_gap(shortname: str, versionid: str) -> int:
    """
    Fetches the granule gap value from DynamoDB based on shortname and version ID.

    Args:
        shortname (str): The collection shortname.
        versionid (str): The collection ID.

    Returns:
        int: The granule gap value in seconds, or 0 if not found.

    Raises:
        ClientError: If there is an issue retrieving data from DynamoDB.
    """
    dynamodb = boto3.resource(
        "dynamodb", region_name=os.environ.get("AWS_REGION", "us-west-2")
    )
    table_name = os.environ["TOLERANCE_TABLE"]
    table = dynamodb.Table(table_name)

    logger.info(
        f"Querying DynamoDB with shortname='{shortname}', versionid='{versionid}'"
    )

    try:
        response = table.get_item(Key={"shortname": shortname, "versionid": versionid})
        logger.info(f"DynamoDB Response: {response}")

        return (
            int(response["Item"]["granulegap"])
            if "Item" in response and "granulegap" in response["Item"]
            else 0
        )

    except Exception as e:
        logger.error(f"Error fetching granulegap from DynamoDB: {e}")
        raise e


def fetch_time_gaps(
    shortname,
    versionid,
    granulegap,
    cursor,
    knownCheck=False,
    startDate=None,
    endDate=None,
):
    """
    Fetches time gaps from the database table sorted by start_ts.

    Args:
        shortname (str): The collection shortname.
        versionid (str): The collection ID.
        cursor (psycopg2.cursor): The PostgreSQL cursor object.
        granulegap (int): The granule gap value in seconds.
        knownCheck (bool): Condition whether we filter out known gaps

    Returns:
        list: A list of tuples containing (start_ts, end_ts).
    """
    collection_id = f"{shortname}___{sanitize_versionid(versionid)}"
    logger.info(
        f"Fetching time gaps for {collection_id} with granule gap > {granulegap} seconds"
    )

    query = """
        SELECT start_ts, end_ts 
        FROM gaps
        WHERE collection_id = %s
        AND end_ts - start_ts > %s::INTERVAL
    """

    if knownCheck:
        query += """
        AND NOT EXISTS (
            SELECT 1 FROM reasons r 
            WHERE r.collection_id = gaps.collection_id 
            -- @> is range containment operator
            AND tsrange(r.start_ts, r.end_ts) @> tsrange(gaps.start_ts, gaps.end_ts)
        )"""
    
    if startDate:
        query += " AND start_ts > %s"
    if endDate:
        query += " AND end_ts < %s"

    params = [collection_id, f"{granulegap} seconds"]
    if startDate:
        params.append(startDate)
    if endDate:
        params.append(endDate)

    query += " ORDER BY start_ts;"

    cursor.execute(query, params)
    rows = cursor.fetchall()

    if rows and rows[-1][1].year == 9999:
        logger.info("Replacing end_ts with current datetime due to 9999 year.")
        rows[-1] = (rows[-1][0], datetime.now())

    return rows


def check_gap_config(collection_id: str, cursor) -> bool:
    """
    Checks granule config table to see if collection has been initialized for gap detection

    Args:
        collection_id (str): The collection ID with sanitized version ID
        cursor (str): The postgres SQL cursor object

    Returns:
        bool: Returns whether object exists in gap config table
    """

    query = """
        SELECT EXISTS (
            SELECT 1 FROM collections WHERE collection_id = %s
        )
    """
    cursor.execute(query, (collection_id,))
    return cursor.fetchone()[0]
