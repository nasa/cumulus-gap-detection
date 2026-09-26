"""Connection retries must finish before the caller's transaction starts."""

from unittest.mock import MagicMock, patch
from uuid import uuid4

import psycopg
import pytest
from psycopg_pool import PoolTimeout

import utils


def connection():
    conn = MagicMock()
    conn.closed = False
    return conn


@pytest.fixture
def pool(monkeypatch):
    value = MagicMock()
    monkeypatch.setattr(utils, "get_connection_pool", lambda: value)
    monkeypatch.setattr(utils.time, "sleep", MagicMock())
    return value


@pytest.mark.parametrize(
    "error_type",
    [
        psycopg.OperationalError,
        PoolTimeout,
        ValueError,
        KeyboardInterrupt,
    ],
)
def test_transaction_errors_are_not_acquisition_retries(pool, error_type):
    conn = connection()
    pool.getconn.return_value = conn
    failure = error_type("transaction failed")
    with pytest.raises(error_type) as caught:
        with utils.get_db_connection() as actual:
            assert actual is conn
            raise failure
    assert caught.value is failure
    pool.getconn.assert_called_once_with(timeout=10)
    pool.putconn.assert_called_once_with(conn)
    conn.rollback.assert_called_once_with()
    conn.commit.assert_not_called()
    utils.time.sleep.assert_not_called()


def test_commit_error_is_not_retried(pool):
    conn = connection()
    pool.getconn.return_value = conn
    failure = psycopg.OperationalError("commit failed")
    conn.commit.side_effect = failure
    with pytest.raises(psycopg.OperationalError) as caught:
        with utils.get_db_connection():
            pass
    assert caught.value is failure
    pool.getconn.assert_called_once_with(timeout=10)
    pool.putconn.assert_called_once_with(conn)
    conn.rollback.assert_called_once_with()
    conn.commit.assert_called_once_with()


@pytest.mark.parametrize("raise_error", [False, True])
def test_closed_connection_is_returned_to_the_pool(pool, raise_error):
    conn = connection()
    pool.getconn.return_value = conn
    failure = psycopg.OperationalError("connection closed")

    def transaction():
        with utils.get_db_connection():
            conn.closed = True
            if raise_error:
                raise failure

    if raise_error:
        with pytest.raises(psycopg.OperationalError) as caught:
            transaction()
        assert caught.value is failure
    else:
        transaction()
    pool.getconn.assert_called_once_with(timeout=10)
    pool.putconn.assert_called_once_with(conn)
    conn.commit.assert_not_called()
    conn.rollback.assert_not_called()


@pytest.mark.parametrize("closed", [False, True])
def test_validation_retry_does_not_return_a_stale_connection(pool, closed):
    first = connection()
    first.closed = closed
    first.cursor.return_value.__enter__.return_value.execute.side_effect = (
        psycopg.OperationalError("validation failed")
    )
    good = connection()
    pool.getconn.side_effect = [first, PoolTimeout("pool busy"), good]
    with utils.get_db_connection() as actual:
        assert actual is good
    assert pool.getconn.call_count == 3
    assert [call.args[0] for call in pool.putconn.call_args_list] == [first, good]
    assert [call.args[0] for call in utils.time.sleep.call_args_list] == [0.2, 0.4]
    good.commit.assert_called_once_with()


@pytest.mark.parametrize("error_type", [psycopg.ProgrammingError, KeyboardInterrupt])
def test_unexpected_validation_error_releases_connection(pool, error_type):
    conn = connection()
    pool.getconn.return_value = conn
    failure = error_type("validation interrupted")
    conn.cursor.return_value.__enter__.return_value.execute.side_effect = failure
    with pytest.raises(error_type) as caught:
        with utils.get_db_connection():
            pytest.fail("transaction must not start")
    assert caught.value is failure
    pool.getconn.assert_called_once_with(timeout=10)
    pool.putconn.assert_called_once_with(conn)
    utils.time.sleep.assert_not_called()


def test_acquisition_timeout_exhausts_retries_without_returning_connection(pool):
    failure = PoolTimeout("pool unavailable")
    pool.getconn.side_effect = failure
    with pytest.raises(PoolTimeout) as caught:
        with utils.get_db_connection():
            pytest.fail("transaction must not start")
    assert caught.value is failure
    assert pool.getconn.call_count == 3
    pool.putconn.assert_not_called()
    assert utils.time.sleep.call_count == 2


def test_success_commits_and_returns_once(pool):
    conn = connection()
    pool.getconn.return_value = conn
    with utils.get_db_connection() as actual:
        assert actual is conn
    conn.cursor.return_value.__enter__.return_value.execute.assert_called_once_with(
        "SELECT 1"
    )
    conn.commit.assert_called_once_with()
    conn.rollback.assert_not_called()
    pool.putconn.assert_called_once_with(conn)


def test_real_transaction_rolls_back_without_retrying():
    """Use the disposable PostgreSQL test database, not a mocked connection."""
    name = psycopg.sql.Identifier("connection_test_" + uuid4().hex)
    with utils.get_db_connection() as conn:
        conn.execute(psycopg.sql.SQL("CREATE TABLE {} (value integer)").format(name))
        conn.execute(psycopg.sql.SQL("INSERT INTO {} VALUES (1)").format(name))
    try:
        failure = psycopg.OperationalError("application transaction failed")
        real_pool = utils.get_connection_pool()
        with (
            patch.object(real_pool, "getconn", wraps=real_pool.getconn) as acquire,
            patch.object(real_pool, "putconn", wraps=real_pool.putconn) as release,
        ):
            with pytest.raises(psycopg.OperationalError) as caught:
                with utils.get_db_connection() as conn:
                    conn.execute(
                        psycopg.sql.SQL("INSERT INTO {} VALUES (2)").format(name)
                    )
                    raise failure
            assert caught.value is failure
            acquire.assert_called_once()
            release.assert_called_once_with(conn)
        with utils.get_db_connection() as conn:
            rows = conn.execute(
                psycopg.sql.SQL("SELECT value FROM {}").format(name)
            ).fetchall()
            assert rows == [(1,)]
    finally:
        with utils.get_db_connection() as conn:
            conn.execute(psycopg.sql.SQL("DROP TABLE {}").format(name))
