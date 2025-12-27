import grpc
import pytest

from sqlmodel import Session
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database, drop_database

DB_BASE_URL = "postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_name}"


class GrpcFakeContext:
    def __init__(self):
        self.code = grpc.StatusCode.OK
        self.details = None

    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details = details


def create_test_engine(metadata, test_db_url: str):
    if database_exists(test_db_url):
        drop_database(test_db_url)

    create_database(test_db_url)
    engine = create_engine(test_db_url)

    # Collect schemas used by models
    schemas = {table.schema for table in metadata.tables.values() if table.schema}

    # Create schemas if needed
    with engine.begin() as conn:
        for schema in schemas:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

    metadata.create_all(engine)

    return engine


@pytest.fixture
def db_session(test_engine):
    connection = test_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    # Use nested transaction for automatic rollback
    connection.begin_nested()
    session.begin_nested()

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()
