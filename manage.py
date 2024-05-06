import os 
import csv
import math
import logging
import numpy as np
import pandas as pd
import argparse

from tqdm import tqdm
from dotenv import load_dotenv
from create_db import get_database_connection
from queries import (
    INSERT_TRANSACTION_RECORD, 
    DELETE_ALL_TRANSACTIONS_QUERY, 
    GET_TOTAL_COUNT_QUERY, 
    FETCH_ALL_TRANSACTION_DATA_QUERY
)
from prettytable import PrettyTable
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


db_connection = get_database_connection()

DESCRIPTION_INDEX = 9
AMOUNT_INDEX = 5
MAIN_DESC_INDEX = 2
SUPPLIERS_MATCH_STRING = "FRIDGETNP"


def export_transactions_to_csv(connection, csv_file_path):
    """
    Export transactions to a CSV file from the database.

    This function executes a predefined SQL query to fetch all transaction data from the database,
    excludes the 'id' field from the data, and writes the remaining data to a CSV file.

    Args:
    connection: A database connection object.
    csv_file_path: The file path where the CSV file will be saved.

    Raises:
    Exception: If an error occurs during the database fetch or file write process.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(FETCH_ALL_TRANSACTION_DATA_QUERY)
        
        columns = [column[0] for column in cursor.description if column[0].lower() != 'id']
        id_column_index = [column[0] for column in cursor.description].index('id')

        rows = cursor.fetchall()
        filtered_rows = [tuple(item for idx, item in enumerate(row) if idx != id_column_index) for row in rows]
        
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(columns) 
            csv_writer.writerows(filtered_rows)

        logger.info(f"export_transactions_to_csv: Data successfully exported to {csv_file_path}")
    except Exception as e:
        logger.error(f"export_transactions_to_csv: An error occurred: {e}")
        raise
    finally:
        cursor.close()


def delete_all_transactions(connection):
    """
    Delete all transactions from the database.

    This function executes a SQL command to delete all records from the transaction table.
    It logs the number of deleted rows.

    Args:
    connection: A database connection object.

    Raises:
    Exception: If the SQL execution fails or the connection cannot be established.
    """
    cursor = connection.cursor()
    
    try:
        cursor.execute(DELETE_ALL_TRANSACTIONS_QUERY)

        table = PrettyTable()
        table.field_names = ["Deleted Rows"]
        table.add_row([ cursor.rowcount])

        logger.info(f"delete_all_transactions: \n {table}")

        connection.commit()
    except Exception as err:
        logger.error(f"delete_all_transactions: Could not delete transactions: {err}")
        raise
    finally:
        cursor.close()


def transaction_total_count(connection):
    """
    Calculate the total count of transactions in the database.

    This function executes a SQL query that counts all records in the transaction table
    and logs the count in a formatted table.

    Args:
    connection: A database connection object.

    Raises:
    Exception: If the SQL execution fails or the connection cannot be established.
    """
    cursor = connection.cursorf()
    
    try:
        cursor.execute(GET_TOTAL_COUNT_QUERY)
        records = cursor.fetchall()

        table = PrettyTable()
        table.field_names = ["Total Count"]
        table.add_row([records[0][0]])

        logger.info(f"fetch_all_history_records: \n {table}") 
    except Exception as err:
        logger.error(f"fetch_all_history_records: An error occurred: {err}")
        raise
    finally:
        cursor.close()


def insert_transaction_record(connection, record, row_position):
    """
    Insert a single transaction record into the database.

    Args:
    connection: A database connection object.
    record: A tuple containing the transaction data to insert.
    row_position: The position of the row in the source file, used for error logging.

    Raises:
    Exception: If the transaction record cannot be inserted into the database.
    """
    cursor = db_connection.cursor()
    try:
        cursor.execute(INSERT_TRANSACTION_RECORD, record)
    except Exception as err:
        logger.error(f"insert_transaction_record: error at row {row_position} with records: {record}: {err}")
        raise 

def read_file_to_dataframe(file_path):
    """
    Read a CSV or XLSX file and return a pandas DataFrame.

    Args:
    file_path: The file path to the CSV or XLSX file.

    Returns:
    A pandas DataFrame containing the data from the file.

    Raises:
    ValueError: If the file format is not supported.
    """
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path)
    else:
        raise ValueError("read_file_to_dataframe: Unsupported file format. Please provide a .csv or .xlsx file.")

def convert_nan_to_null(row):
    """
    Convert NaN values in a row to None.

    Args:
    row: A row of data from a pandas DataFrame.

    Returns:
    A new row with NaN values replaced by None.
    """
    
    return [None if pd.isnull(x) else x for x in row]

def assign_description_to_transactions(transaction_data):    
    if transaction_data[AMOUNT_INDEX] > 0:
        transaction_data[DESCRIPTION_INDEX] = "Revenue"
    else:
        transaction_data[DESCRIPTION_INDEX] = "Expenses"

    if SUPPLIERS_MATCH_STRING in transaction_data[MAIN_DESC_INDEX]:
        transaction_data[DESCRIPTION_INDEX] = "Suppliers"

    return tuple(transaction_data)

def update_csv_insert_progress_bar(processed_rows, total_rows):
    if 'tqdm_bar' not in update_csv_insert_progress_bar.__dict__:
        update_csv_insert_progress_bar.tqdm_bar = tqdm(total=total_rows, unit='row')
    
    update_csv_insert_progress_bar.tqdm_bar.update(1)

    if processed_rows >= total_rows:
        update_csv_insert_progress_bar.tqdm_bar.close()
        del update_csv_insert_progress_bar.tqdm_bar


def handle_bulk_transaction_create(args):
    """
    Handle the bulk creation of transactions from a CSV or XLSX file.

    This function reads transaction data from a file, processes each transaction, and inserts
    the data into the database provided that insert_transaction_record hasn't raised any exceptions,
    if so, no records will be created.

    Args:
    args: Command line arguments that include the file path to the CSV or XLSX file.

    The function logs the success or failure of the operation and manages the database connection.
    """
    file_path = args.file
    total_size = os.path.getsize(file_path)
    
    try:

        row_position = 2
        transaction_data_df = read_file_to_dataframe(file_path)
        total_rows = len(transaction_data_df)
        processed_rows = 0

        for index, row in transaction_data_df.iterrows():
            processed_rows += 1
            progress_percentage = (processed_rows / total_rows) * 100
            update_csv_insert_progress_bar(progress_percentage, total_rows)

            cleaned_trans_data = convert_nan_to_null(tuple(row))
            formated_trans_data = assign_description_to_transactions(cleaned_trans_data)

            insert_transaction_record(db_connection, formated_trans_data, row_position)

        # for atomization, we whether insert all or just let it crash
        db_connection.commit()
        
        logger.info("handle_bulk_transaction_create: Data inserted successfully")

    except Exception as e:
        logger.error(f"handle_bulk_transaction_create: An error occurred: {e}")
        raise


def parse_args():
    """
    Parse command line arguments for the script.

    Returns:
    An object containing the parsed command line arguments.
    """
    parser = argparse.ArgumentParser(description='Upload CSV or XLSX data to MySQL database.')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    bulk_transaction_create_parser = subparsers.add_parser('bulk_transaction_create', help='Create bulk transactions')
    bulk_transaction_create_parser.add_argument('--file', type=str, help='Path to the CSV or XLSX file to upload.', required=True)

    bulk_export_transaction_parser = subparsers.add_parser('bulk_transaction_export', help='Exports database transaction data to csv format')
    bulk_export_transaction_parser.add_argument('--path', type=str, help='Path to the CSV or XLSX file to upload.', required=True)
    bulk_export_transaction_parser.add_argument('--email', type=str, help='Email address to send the CSV file to.', required=False)

    delete_all_parser = subparsers.add_parser('delete_all', help='Delete all transactions')
    total_amount_parser = subparsers.add_parser('total_amount', help='Returns total amount of transactions')


    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    if args.command == 'bulk_transaction_create':
        handle_bulk_transaction_create(args)
    if args.command == 'delete_all':
        delete_confirmation = input("Are you sure you want to delete all transactions? Type 'yes' to confirm: ")
        if delete_confirmation.lower() == 'yes':
            delete_all_transactions(db_connection)
        else:
            logger.info("main: Deletion aborted")
    if args.command == 'total_amount':
        transaction_total_count(db_connection)
    if args.command == 'bulk_export_transactions':
        export_transactions_to_csv(db_connection, args.path)
    
    db_connection.close()
