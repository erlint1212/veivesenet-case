import unittest
from unittest.mock import patch, mock_open, MagicMock
import pandas as pd
import os

# Add the parent directory to sys.path
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import functions from your script
try:
    from load_and_check import (
        get_db_engine,
        load_csv_to_hendelser,
        check_vegobjekter_data
    )
except ImportError:
    print("Failed to import from load_and_check.py. Ensure script exists and is in correct path.")
    def get_db_engine(*args, **kwargs): pass
    def load_csv_to_hendelser(*args, **kwargs): pass
    def check_vegobjekter_data(*args, **kwargs): pass


class TestLoadAndCheck(unittest.TestCase):

    @patch('load_and_check.create_engine')
    @patch.dict(os.environ, {
        "POSTGRES_USER": "testuser", "POSTGRES_PASSWORD": "testpassword",
        "POSTGRES_HOST": "testhost", "POSTGRES_PORT": "5432", "POSTGRES_DB": "testdb"
    })
    def test_get_db_engine(self, mock_create_engine):
        get_db_engine("testuser", "testpassword", "testhost", "5432", "testdb")
        mock_create_engine.assert_called_once_with(
            "postgresql://testuser:testpassword@testhost:5432/testdb?sslmode=disable"
        )

    @patch('load_and_check.os.path.exists', return_value=True)
    @patch('load_and_check.pd.read_csv')
    @patch('load_and_check.pd.DataFrame.to_sql')
    def test_load_csv_to_hendelser_success(self, mock_to_sql, mock_read_csv, mock_path_exists):
        """Tests successful CSV loading."""
        mock_read_csv.return_value = pd.DataFrame({
            'veglenkesekvensid': [1, 2],
            'relativ_posisjon': [0.1, 0.2],
            'vegvedlikehold': ['ja', 'nei'],
            'rand_float': [0.5, 0.6],
            'year': [2023, 2023]
        })
        mock_engine = MagicMock()
        load_csv_to_hendelser(mock_engine, "dummy_path.csv")
        
        mock_path_exists.assert_called_once_with("dummy_path.csv")
        mock_read_csv.assert_called_once_with("dummy_path.csv")
        mock_to_sql.assert_called_once_with(
            "hendelser", mock_engine, schema="nvdb", if_exists="append", index=False, chunksize=1000
        )

    @patch('load_and_check.os.path.exists', return_value=False)
    @patch('builtins.print') # To check print output
    def test_load_csv_to_hendelser_file_not_found(self, mock_print, mock_path_exists):
        """Tests scenario where CSV file is not found."""
        mock_engine = MagicMock()
        load_csv_to_hendelser(mock_engine, "non_existent.csv")
        mock_path_exists.assert_called_once_with("non_existent.csv")
        mock_print.assert_any_call("Error: CSV file not found at non_existent.csv")

    @patch('load_and_check.os.path.exists', return_value=True)
    @patch('load_and_check.pd.read_csv')
    @patch('builtins.print')
    def test_load_csv_to_hendelser_missing_year_column(self, mock_print, mock_read_csv, mock_path_exists):
        """Tests CSV loading when 'year' column is missing."""
        mock_read_csv.return_value = pd.DataFrame({'veglenkesekvensid': [1]}) # Missing 'year'
        mock_engine = MagicMock()
        load_csv_to_hendelser(mock_engine, "dummy_path.csv")
        mock_print.assert_any_call("Error: 'year' column not found in CSV. Cannot load into partitioned table.")


    @patch('load_and_check.pd.read_sql_query')
    @patch('builtins.print')
    def test_check_vegobjekter_data_with_data(self, mock_print, mock_read_sql_query):
        """Tests data checking when table has data."""
        # Simulate two calls to read_sql_query: one for count, one for sample
        mock_read_sql_query.side_effect = [
            pd.DataFrame({'count': [10]}), # Result for COUNT(*)
            pd.DataFrame({'nvdb_id': [1, 2], 'fartsgrense': [80, 60]}) # Sample data
        ]
        mock_engine = MagicMock()
        check_vegobjekter_data(mock_engine)
        
        self.assertEqual(mock_read_sql_query.call_count, 2)
        mock_print.assert_any_call("Found 10 rows in nvdb.vegobjekter_fartsgrense.")
        mock_print.assert_any_call("\nSample data:")

    @patch('load_and_check.pd.read_sql_query')
    @patch('builtins.print')
    def test_check_vegobjekter_data_no_data(self, mock_print, mock_read_sql_query):
        """Tests data checking when table is empty."""
        mock_read_sql_query.return_value = pd.DataFrame({'count': [0]}) # Result for COUNT(*)
        mock_engine = MagicMock()
        check_vegobjekter_data(mock_engine)
        
        mock_read_sql_query.assert_called_once() # Only count query should run
        mock_print.assert_any_call("Found 0 rows in nvdb.vegobjekter_fartsgrense.")
        mock_print.assert_any_call("Table appears to be empty.")


if __name__ == '__main__':
    unittest.main()
