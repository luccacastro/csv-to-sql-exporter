import os

TRANSACTION_TABLE_QUERY = f"""
    CREATE TABLE IF NOT EXISTS history (
        id INT AUTO_INCREMENT PRIMARY KEY,
        AccNo VARCHAR(255),
        Date DATE,
        MainDesc VARCHAR(255),
        AddDesc VARCHAR(255),
        TransactionType VARCHAR(50),
        Amount DECIMAL(10, 2),
        Balance DECIMAL(10, 2),
        SpendCategory VARCHAR(100),
        Currency VARCHAR(50),
        Description TEXT
    );
    """

INSERT_TRANSACTION_RECORD = """
        INSERT INTO history (AccNo, Date, MainDesc, AddDesc, TransactionType, Amount, Balance, SpendCategory, Currency, Description) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

CREATE_DATABASE_QUERY = """
    CREATE DATABASE IF NOT EXISTS `%s`
"""

USE_DATABASE_QUERY = """ 
    USE `%s`
"""

DELETE_ALL_TRANSACTIONS_QUERY = """
    DELETE FROM history;
"""

GET_TOTAL_COUNT_QUERY = """
    SELECT COUNT(*) FROM history
"""

FETCH_ALL_TRANSACTION_DATA_QUERY = """
    SELECT * FROM history
"""