import unittest
import os
from manage import handle_bulk_transaction_create, delete_all_transactions, transaction_total_count
from create_db import get_database_connection

from unittest.mock import MagicMock
import argparse
import mysql.connector

class TestDatabaseOperations(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.original_db_name = os.getenv('DB_NAME')

        self.connection = get_database_connection(env='prod')
        self.cursor = self.connection.cursor()
        self.cursor.execute("TRUNCATE TABLE history")


    def test_handle_bulk_transaction_create(self):
        connection = get_database_connection(env='prod')
        args = argparse.Namespace(file='./test_transactions.csv')

        handle_bulk_transaction_create(args)
        cursor = connection.cursor()

        cursor.execute("SELECT COUNT(*) FROM history")
        row_count = cursor.fetchone()[0]
        
        self.assertEqual(row_count, 2)
    
    def test_delete_all_transactions(self):
        connection = get_database_connection(env='test')
        cursor = connection.cursor()

        args = argparse.Namespace(file='./test_transactions.csv')

        handle_bulk_transaction_create(args)

        delete_all_transactions(self.connection)

        cursor.execute("SELECT COUNT(*) FROM history")
        row_count = cursor.fetchone()[0]
        
        self.assertEqual(row_count, 0) 
    
    

    @classmethod
    def tearDownClass(self):
        # Restore the original DB_NAME
        if self.original_db_name is not None:
            os.environ['DB_NAME'] = self.original_db_name
        else:
            del os.environ['DB_NAME']

        self.connection.close()

if __name__ == '__main__':
    unittest.main()
