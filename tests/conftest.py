import os
import pytest
import psycopg2
from dotenv import load_dotenv

# Load the environment variables
load_dotenv()

# connect function for PostgreSQL database server
@pytest.fixture
def db_connect():
    # Set up the connection parameters:
    conn_params_dict = {
        "host": os.getenv('PG_HOST'),
        "database": os.getenv('PG_DATABASE_GEORGIA'),
        "user": os.getenv('PG_USER'),
        "password": os.getenv('PG_PASSWORD')}
    
    conn = psycopg2.connect(**conn_params_dict)
    
    yield conn
    
    # Tear down resources after testing
    conn.close()