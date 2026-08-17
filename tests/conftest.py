import os
import pytest
import psycopg
from psycopg.rows import dict_row

DSN = os.environ["DATABASE_URL"]

@pytest.fixture
def conn():
    with psycopg.connect(DSN, autocommit=True, row_factory=dict_row) as c:
        c.execute("TRUNCATE jobs, results RESTART IDENTITY")
        yield c