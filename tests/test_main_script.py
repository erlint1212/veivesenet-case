import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import os

# Add the parent directory to sys.path
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import functions from your script
# Assuming your script is named main.py
try:
    from main import (
        get_db_engine,
        sql_request,
        plot_incidents_per_year,
        main as main_function # Alias to avoid conflict if running test itself as main
    )
except ImportError:
    print("Failed to import from main.py. Ensure script exists and is in correct path.")
    def get_db_engine(*args, **kwargs): pass
    def sql_request(*args, **kwargs): return pd.DataFrame()
    def plot_incidents_per_year(*args, **kwargs): pass
    def main_function(*args, **kwargs): pass


class TestMainScript(unittest.TestCase):

    @patch('main.create_engine')
    @patch.dict(os.environ, {
        "POSTGRES_USER": "testuser", "POSTGRES_PASSWORD": "testpassword",
        "POSTGRES_HOST": "testhost", "POSTGRES_PORT": "5432", "POSTGRES_DB": "testdb"
    })
    def test_get_db_engine(self, mock_create_engine):
        """Test db engine creation with mocked connection test"""
        mock_engine_instance = MagicMock()
        mock_connection = MagicMock()
        mock_engine_instance.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine_instance

        get_db_engine("testuser", "testpassword", "testhost", "5432", "testdb")
        mock_create_engine.assert_called_once_with(
            "postgresql://testuser:testpassword@testhost:5432/testdb?sslmode=disable"
        )
        mock_engine_instance.connect.assert_called_once()


    @patch('main.pd.read_sql_query')
    def test_sql_request_success(self, mock_read_sql):
        """Tests successful SQL request."""
        expected_df = pd.DataFrame({'col1': [1]})
        mock_read_sql.return_value = expected_df
        mock_engine = MagicMock()
        
        df = sql_request("SELECT * FROM dummy", mock_engine)
        mock_read_sql.assert_called_once_with("SELECT * FROM dummy", mock_engine)
        pd.testing.assert_frame_equal(df, expected_df)

    @patch('main.pd.read_sql_query', side_effect=Exception("DB error"))
    @patch('builtins.print')
    def test_sql_request_failure(self, mock_print, mock_read_sql):
        """Tests SQL request failure."""
        mock_engine = MagicMock()
        df = sql_request("SELECT * FROM dummy", mock_engine)
        
        self.assertTrue(df.empty)
        mock_print.assert_any_call("Error during SQL query: DB error")

    @patch('main.plt.show')
    @patch('main.plt.close')
    @patch('main.os.makedirs')
    @patch('main.plt.subplots') # To get a mock fig and ax
    def test_plot_incidents_per_year_with_data(self, mock_subplots, mock_makedirs, mock_close, mock_show):
        """Tests plotting function with valid data."""
        mock_fig = MagicMock()
        mock_ax = MagicMock()
        mock_subplots.return_value = (mock_fig, mock_ax)

        df = pd.DataFrame({
            'year': [2022, 2022, 2023],
            'vegkategori': ['E', 'F', 'E']
        })
        plot_incidents_per_year(df)
        
        mock_makedirs.assert_called_once_with("images", exist_ok=True)
        mock_fig.savefig.assert_called_once()
        mock_show.assert_called_once()
        mock_close.assert_called_once_with(mock_fig)

    def test_plot_incidents_per_year_empty_df(self):
        """Tests plotting with an empty DataFrame."""
        with patch('builtins.print') as mock_print:
            plot_incidents_per_year(pd.DataFrame())
            mock_print.assert_any_call("Input DataFrame is empty or invalid. Skipping plot.")

    def test_plot_incidents_per_year_missing_columns(self):
        """Tests plotting with DataFrame missing required columns."""
        with patch('builtins.print') as mock_print:
            plot_incidents_per_year(pd.DataFrame({'year': [2022]})) # Missing vegkategori
            mock_print.assert_any_call("DataFrame is missing 'year' and/or 'vegkategori' columns. Skipping plot.")

    @patch('main.get_db_engine')
    @patch('main.sql_request')
    @patch('main.plot_incidents_per_year')
    def test_main_function_flow(self, mock_plot, mock_sql, mock_get_engine):
        """Tests the main execution flow by mocking helper functions."""
        mock_engine_instance = MagicMock()
        mock_get_engine.return_value = mock_engine_instance
        
        sample_df = pd.DataFrame({
            'year': [2022, 2022, 2023],
            'vegkategori': ['E', 'F', 'E'],
            'nvdb_id': [1,2,3],
            'veglenkesekvensid': [10,20,10],
            'fartsgrense': [80,60,80],
            'relativ_posisjon': [0.1,0.2,0.3],
            'vegvedlikehold': ['x','y','z']
        })
        mock_sql.return_value = sample_df
        
        main_function() # Call the aliased main
        
        mock_get_engine.assert_called_once()
        mock_sql.assert_called_once_with(unittest.mock.ANY, mock_engine_instance)
        # Check if mapping logic was applied (vegkategori 'E' should become 'Europaveg')
        # The DataFrame passed to plot should have mapped values
        # This assertion is a bit more complex as it checks the argument passed to a mock
        call_args = mock_plot.call_args[0][0] # Gets the first positional argument (the DataFrame)
        self.assertIn('Europaveg', call_args['vegkategori'].unique())
        mock_plot.assert_called_once()
        mock_engine_instance.dispose.assert_called_once()

if __name__ == '__main__':
    unittest.main()
