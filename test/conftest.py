import pytest
import os
import re
import json
from unittest.mock import patch
from psycopg.sql import SQL, Identifier, Literal
from io import StringIO
import sys

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

TEST_COLLECTION_ID = "TEST_COLLECTION___1_0"
SECOND_COLLECTION_ID = "M2T1NXSLV___5_12_7"
DEFAULT_DATE = "2000-01-01 00:00:00"
DEFAULT_END_DATE = "2100-01-01 00:00:00"

@pytest.fixture(scope="session", autouse=True)
def patch_db_config():
    """Patch the database config function for tests."""
    import utils
    
    test_db_config = {
        'database': os.getenv('TEST_DB_NAME', 'testdb'),
        'username': os.getenv('TEST_DB_USER', 'testuser'),
        'password': os.getenv('TEST_DB_PASSWORD', 'testpass')
    }
   
    # Create patching function
    def mock_get_db_config(*args, **kwargs):
        return test_db_config
    with patch.object(utils, 'get_db_config', side_effect=mock_get_db_config):
        yield

@pytest.fixture(scope="session", autouse=True)
def setup_database():
    from utils import get_db_connection
    
    with get_db_connection() as conn:
        with open("gap_schema.sql") as f:
            init_sql = f.read()
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS gaps CASCADE;")
            cur.execute("DROP TABLE IF EXISTS reasons CASCADE;")
            cur.execute("DROP TABLE IF EXISTS collections CASCADE;")
            cur.execute(init_sql)
        
        test_collections = [
            TEST_COLLECTION_ID, SECOND_COLLECTION_ID,
            "complete_collection___1_0", "incomplete_collection___1_0"
        ]
        for collection_id in test_collections:
            create_test_partition(collection_id, conn)
    
    yield

@pytest.fixture
def setup_test_data():
    from utils import get_db_connection
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Clear existing data
            cur.execute("TRUNCATE TABLE gaps CASCADE")
            cur.execute("TRUNCATE TABLE reasons CASCADE")
            cur.execute("TRUNCATE TABLE collections CASCADE")
            
            # Disable trigger to prevent automatic gap creation
            cur.execute("ALTER TABLE collections DISABLE TRIGGER collection_insert_trigger")
            
            # Insert test collections
            cur.execute("""
                INSERT INTO collections (collection_id, temporal_extent_start, temporal_extent_end)
                VALUES (%s, %s, %s), (%s, %s, %s)
            """, (
                TEST_COLLECTION_ID, DEFAULT_DATE, DEFAULT_END_DATE,
                SECOND_COLLECTION_ID, DEFAULT_DATE, DEFAULT_END_DATE
            ))
            
            # Re-enable trigger
            cur.execute("ALTER TABLE collections ENABLE TRIGGER collection_insert_trigger")
        conn.commit()
    
    yield

@pytest.fixture
def mock_sql():
    from unittest.mock import mock_open
    
    with patch('os.path.join', return_value='update_gaps.sql'):
        with open("src/gapUpdate/update_gaps.sql", "r") as f:
            actual_sql = f.read()
        with patch('builtins.open', mock_open(read_data=actual_sql)) as m:
            yield m

# Helper Functions
def create_test_partition(collection_id, conn):
    """Create test partitions for both gaps and reasons tables."""
    with conn.cursor() as cur:
        safe_collection_id = re.sub(r'\W+', '_', collection_id)
        
        # Create gaps partition
        gaps_partition_name = f"gaps_{safe_collection_id}"
        cur.execute("""
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = %s AND n.nspname = 'public'
        """, (gaps_partition_name,))
        
        if cur.fetchone() is None:
            cur.execute(
                SQL("CREATE TABLE {} PARTITION OF gaps FOR VALUES IN ({})").format(
                    Identifier(gaps_partition_name), Literal(collection_id)
                )
            )
            # Add exclusion constraint to prevent overlapping time ranges
            constraint_name = f"{gaps_partition_name}_no_overlap"
            cur.execute(
                SQL("ALTER TABLE {} ADD CONSTRAINT {} EXCLUDE USING gist (tsrange(start_ts, end_ts) WITH &&)").format(
                    Identifier(gaps_partition_name), Identifier(constraint_name)
                )
            )
        
        # Create reasons partition
        reasons_partition_name = f"reasons_{safe_collection_id}"
        cur.execute("""
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = %s AND n.nspname = 'public'
        """, (reasons_partition_name,))
        
        if cur.fetchone() is None:
            cur.execute(
                SQL("CREATE TABLE {} PARTITION OF reasons FOR VALUES IN ({})").format(
                    Identifier(reasons_partition_name), Literal(collection_id)
                )
            )
    
    return gaps_partition_name, reasons_partition_name

def create_granule(start, end, collection_id=TEST_COLLECTION_ID):
    return {
        "beginningDateTime": start,
        "endingDateTime": end,
        "collectionId": collection_id
    }

def create_buffer(test_data):
    buffer = StringIO()
    for r in test_data:
        buffer.write(f"{r['collectionId']}\t{r['beginningDateTime']}\t{r['endingDateTime']}\n")
    buffer.seek(0)
    return buffer

def create_sqs_event(records_data):
    sqs_records = []
    for record in records_data:
        collection_id = record.get("collectionId", TEST_COLLECTION_ID)
        start_time = record.get("beginningDateTime", "2000-01-01T00:00:00.000Z")
        end_time = record.get("endingDateTime", "2000-02-01T00:00:00.000Z")
        sqs_records.append({
            "body": json.dumps({
                "Message": json.dumps({
                    "record": {
                        "beginningDateTime": start_time,
                        "endingDateTime": end_time,
                        "collectionId": collection_id,
                        "granuleId": "granule_id"
                    }
                })
            })
        })
    return {"Records": sqs_records}

def insert_gap(collection_id, start_ts, end_ts):
    """Insert a gap without a reason."""
    from utils import get_db_connection
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gaps (collection_id, start_ts, end_ts)
                VALUES (%s, %s, %s)
            """, (collection_id, start_ts, end_ts))

def insert_gap_with_reason(collection_id, start_ts, end_ts, reason):
    """Insert a gap and its associated reason."""
    from utils import get_db_connection
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Insert the gap
            cur.execute("""
                INSERT INTO gaps (collection_id, start_ts, end_ts)
                VALUES (%s, %s, %s)
            """, (collection_id, start_ts, end_ts))
            
            # Insert the reason
            cur.execute("""
                INSERT INTO reasons (collection_id, start_ts, end_ts, reason)
                VALUES (%s, %s, %s, %s)
            """, (collection_id, start_ts, end_ts, reason))

def get_gaps(collection_id):
    from utils import get_db_connection
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT start_ts, end_ts FROM gaps WHERE collection_id = %s ORDER BY start_ts",
                (collection_id,)
            )
            return cur.fetchall()

def get_gaps_with_reasons(collection_id):
    """Get gaps with their associated reasons."""
    from utils import get_db_connection
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT g.start_ts, g.end_ts, r.reason
                FROM gaps g
                LEFT JOIN reasons r ON (
                    g.collection_id = r.collection_id 
                    AND g.start_ts = r.start_ts 
                    AND g.end_ts = r.end_ts
                )
                WHERE g.collection_id = %s 
                ORDER BY g.start_ts
            """, (collection_id,))
            return cur.fetchall()

def get_gap_count(collection_id):
    from utils import get_db_connection
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gaps WHERE collection_id = %s", (collection_id,))
            return cur.fetchone()[0]

def get_sql_query():
    with open("src/gapUpdate/update_gaps.sql", "r") as f:
        return f.read()

# Additional helper functions for API testing
def create_api_test_event(http_method, path, body=None, query_string_parameters=None):
    event = {
        "httpMethod": http_method,
        "path": path,
        "headers": {
            "Content-Type": "application/json"
        }
    }
    
    if body:
        event["body"] = json.dumps(body)
    
    if query_string_parameters:
        event["queryStringParameters"] = query_string_parameters
    
    return event

def get_record(conn, collection_id, start_ts, end_ts):
    """Get a gap record with its reason if available."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT g.gap_id, g.collection_id, g.start_ts, g.end_ts, r.reason 
            FROM gaps g
            LEFT JOIN reasons r ON (
                g.collection_id = r.collection_id 
                AND g.start_ts = r.start_ts 
                AND g.end_ts = r.end_ts
            )
            WHERE g.collection_id = %s
            AND g.start_ts = %s
            AND g.end_ts = %s
        """, (collection_id, start_ts, end_ts))
        return cur.fetchone()

def seed_test_data(test_data, collection_id=TEST_COLLECTION_ID):
    """Seed test data for knownGap tests."""
    from utils import get_db_connection
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Ensure collection exists
            cur.execute("SELECT 1 FROM collections WHERE collection_id = %s", (collection_id,))
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO collections (collection_id, temporal_extent_start, temporal_extent_end) VALUES (%s, %s, %s)",
                    (collection_id, DEFAULT_DATE, DEFAULT_END_DATE)
                )
            
            # Ensure partitions exist
            create_test_partition(collection_id, conn)
            
            # Clear existing test data
            cur.execute("DELETE FROM gaps WHERE collection_id = %s", (collection_id,))
            cur.execute("DELETE FROM reasons WHERE collection_id = %s", (collection_id,))
            
            # Insert test data
            for data in test_data:
                start_ts = data.get('start_ts')
                end_ts = data.get('end_ts')
                reason = data.get('reason')
                
                # Insert gap
                cur.execute(
                    "INSERT INTO gaps (collection_id, start_ts, end_ts) VALUES (%s, %s, %s)", 
                    (collection_id, start_ts, end_ts)
                )
                
                # Insert reason if provided
                if reason:
                    cur.execute(
                        "INSERT INTO reasons (collection_id, start_ts, end_ts, reason) VALUES (%s, %s, %s, %s)",
                        (collection_id, start_ts, end_ts, reason)
                    )
            
        conn.commit()

def get_reason(collection_id, start_ts, end_ts):
    """Get the reason for a specific gap."""
    from utils import get_db_connection
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT reason FROM reasons 
                WHERE collection_id = %s AND start_ts = %s AND end_ts = %s
            """, (collection_id, start_ts, end_ts))
            result = cur.fetchone()
            return result[0] if result else None

def insert_reason(collection_id, start_ts, end_ts, reason):
    """Insert a reason for an existing gap."""
    from utils import get_db_connection
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reasons (collection_id, start_ts, end_ts, reason)
                VALUES (%s, %s, %s, %s)
            """, (collection_id, start_ts, end_ts, reason))
        conn.commit()