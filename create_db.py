import os

import logging
import mysql.connector

from mysql.connector import Error
from dotenv import load_dotenv
from queries import TRANSACTION_TABLE_QUERY, CREATE_DATABASE_QUERY, USE_DATABASE_QUERY

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_server_connection():
    try:
        connection = mysql.connector.connect(
            host = os.getenv("DB_HOST"),
            user = os.getenv("DB_USER"),
            password = os.getenv("DB_PASSWORD")
        )
        return connection
    except Error as err:
        raise Exception(f"create_server_connection: Failed to stablish connect: '{err}'")

def create_database(connection, env='prod'):
    try:
        if env == 'test':
            db_name = os.getenv('DB_NAME_TEST')
        else:
            db_name = os.getenv("DB_NAME")

        cursor = connection.cursor()
        cursor.execute(CREATE_DATABASE_QUERY, (db_name,))
        cursor.execute(USE_DATABASE_QUERY, (db_name,))
        print(f"create_database: connected to {db_name}")
    except Error as err:
        raise Exception(f"create_database: Failed to create database: '{err}'")

def create_transactions_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute(TRANSACTION_TABLE_QUERY)
        print("create_transactions_table: Table 'history' created successfully")
    except Error as err:
        raise Exception(f"create_transactions_table: Failed to create table: '{err}'")

def get_database_connection(env='prod'):
    connection = create_server_connection()
    create_database(connection, env)
    create_transactions_table(connection)
    return connection