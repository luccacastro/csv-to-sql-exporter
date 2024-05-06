# Project Title

CSV to SQL exporter

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

What things you need to install the software:

- Python 3.11
- pip (Python package installer)

### Setting Up a Virtual Environment

It's recommended to use a virtual environment to manage the dependencies for your project. To set up and activate a virtual environment, follow these steps:

```bash
# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
# On Windows
venv\Scripts\activate
# On Unix or MacOS
source venv/bin/activate
```

### Installing Dependencies

Install all dependencies that are required for the project by running:

```bash
pip install -r requirements.txt
```

### Configuration

- Configure your `.env` file with the necessary environment variables (e.g., `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`).

## Running the Script

To run the script, you can use the following command:

```bash
python manage.py
```

### Commands and Flags

The script supports various commands and flags:

- **bulk_transaction_create**: Create bulk transactions from a CSV or XLSX file.
  ```bash
  python manage.py bulk_transaction_create --file path/to/yourfile.csv
  ```

- **delete_all**: Delete all transactions from the database. Requires confirmation.
  ```bash
  python manage.py delete_all
  ```

- **total_amount**: Returns the total amount of transactions.
  ```bash
  python manage.py total_amount
  ```

- **bulk_transaction_export**: Exports database transaction data to a CSV format. O
  ```bash
  python manage.py bulk_transaction_export --path path/to/output.csv
  ```

### Testing

To run tests, execute:

```bash
python -m unittest
```

Ensure you have a test database configured to prevent interference with your production data.

## Built With

- Python 3.11.
- MySQL 
- Pandas

## Authors

- **Lucca Zutin**

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
