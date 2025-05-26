import unittest
from unittest.mock import patch, Mock, MagicMock
import pandas as pd
import os

# Add the parent directory to sys.path to allow imports from case_junior
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import functions from your script
# Assuming your script is named api_to_database.py
# If you've renamed it, adjust the import accordingly.
try:
    from api_to_database import (
        get_db_engine,
        fetch_nvdb_data_paginated,
        get_veglenke,
        get_property,
        process_nvdb_objects,
        load_df_to_postgres
    )
except ImportError:
    print("Failed to import from api_to_database.py. Ensure the script exists and is in the correct path.")
    # Define dummy functions if import fails, so test structure can be shown
    def get_db_engine(*args, **kwargs): pass
    def fetch_nvdb_data_paginated(*args, **kwargs): return []
    def get_veglenke(*args, **kwargs): return None
    def get_property(*args, **kwargs): return None
    def process_nvdb_objects(*args, **kwargs): return pd.DataFrame()
    def load_df_to_postgres(*args, **kwargs): pass


class TestApiToDatabase(unittest.TestCase):

    @patch('api_to_database.create_engine')
    @patch.dict(os.environ, {
        "POSTGRES_USER": "testuser", "POSTGRES_PASSWORD": "testpassword",
        "POSTGRES_HOST": "testhost", "POSTGRES_PORT": "5432", "POSTGRES_DB": "testdb"
    })
    def test_get_db_engine_url_construction(self, mock_create_engine):
        """Tests if the database URL is constructed correctly."""
        get_db_engine("testuser", "testpassword", "testhost", "5432", "testdb")
        mock_create_engine.assert_called_once_with(
            "postgresql://testuser:testpassword@testhost:5432/testdb?sslmode=disable"
        )

    @patch('api_to_database.requests.get')
    def test_fetch_nvdb_data_paginated_single_page(self, mock_requests_get):
        """Tests fetching data that fits on a single page."""
        mock_response = Mock()
        mock_response.json.return_value = {
            'objekter': [{'id': 1}, {'id': 2}],
            'metadata': {'neste': {}} # No next page
        }
        mock_response.raise_for_status = Mock()
        mock_requests_get.return_value = mock_response

        data = fetch_nvdb_data_paginated("test_id", {})
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['id'], 1)
        mock_requests_get.assert_called_once()

    @patch('api_to_database.requests.get')
    def test_fetch_nvdb_data_paginated_multiple_pages(self, mock_requests_get):
        """Tests fetching data with pagination."""
        # Response for the first page
        mock_response_page1 = Mock()
        mock_response_page1.json.return_value = {
            'objekter': [{'id': 1}],
            'metadata': {'neste': {'href': '[http://nextpage.com](http://nextpage.com)'}}
        }
        mock_response_page1.raise_for_status = Mock()

        # Response for the second page
        mock_response_page2 = Mock()
        mock_response_page2.json.return_value = {
            'objekter': [{'id': 2}],
            'metadata': {'neste': {}} # No next page
        }
        mock_response_page2.raise_for_status = Mock()

        mock_requests_get.side_effect = [mock_response_page1, mock_response_page2]

        data = fetch_nvdb_data_paginated("test_id", {'param': 'value'})
        self.assertEqual(len(data), 2)
        self.assertEqual(data[1]['id'], 2)
        self.assertEqual(mock_requests_get.call_count, 2)

    def test_get_veglenke_correct_extraction(self):
        """Tests get_veglenke with correct data structure."""
        obj = {'lokasjon': {'stedfestinger': [{'veglenkesekvensid': 12345}]}}
        self.assertEqual(get_veglenke(obj), 12345)

    def test_get_veglenke_missing_data(self):
        """Tests get_veglenke with missing keys."""
        self.assertIsNone(get_veglenke({}))
        self.assertIsNone(get_veglenke({'lokasjon': {}}))
        self.assertIsNone(get_veglenke({'lokasjon': {'stedfestinger': []}}))
        self.assertIsNone(get_veglenke({'lokasjon': {'stedfestinger': [{}]}}))

    def test_get_property_correct_extraction(self):
        """Tests get_property with correct data structure."""
        obj = {'egenskaper': [{'navn': 'Fartsgrense', 'verdi': 80}]}
        self.assertEqual(get_property(obj, 'Fartsgrense'), 80)

    def test_get_property_case_insensitivity(self):
        """Tests get_property is case insensitive for prop_name."""
        obj = {'egenskaper': [{'navn': 'Fartsgrense', 'verdi': 80}]}
        self.assertEqual(get_property(obj, 'fartsgrense'), 80)

    def test_get_property_not_found(self):
        """Tests get_property when property is not found."""
        obj = {'egenskaper': [{'navn': 'AnnenEgenskap', 'verdi': 'test'}]}
        self.assertIsNone(get_property(obj, 'Fartsgrense'))

    def test_process_nvdb_objects(self):
        """Tests the basic processing of NVDB objects into a DataFrame."""
        objects = [
            {
                'id': 1,
                'lokasjon': {
                    'vegsystemreferanser': [{'vegsystem': {'vegkategori': 'E'}}],
                    'fylker': [50], 'kommuner': [5001],
                    'stedfestinger': [{'veglenkesekvensid': 100}]
                },
                'metadata': {'startdato': '2023-01-01', 'sist_modifisert': '2023-01-02'},
                'geometri': {'wkt': 'LINESTRING(...)ബ'},
                'egenskaper': [{'navn': 'Fartsgrense', 'verdi': 80}]
            }
        ]
        df = process_nvdb_objects(objects)
        self.assertEqual(len(df), 1)
        self.assertIn('nvdb_id', df.columns)
        self.assertIn('veglenkesekvensid', df.columns)
        self.assertEqual(df['nvdb_id'].iloc[0], 1)
        self.assertEqual(df['veglenkesekvensid'].iloc[0], 100)
        self.assertEqual(df['fartsgrense'].iloc[0], 80)

    @patch('api_to_database.pd.DataFrame.to_sql')
    def test_load_df_to_postgres(self, mock_to_sql):
        """Tests that to_sql is called with correct parameters."""
        df = pd.DataFrame({'col1': [1], 'col2': ['a']})
        mock_engine = MagicMock() # Using MagicMock for engine as it's passed around
        load_df_to_postgres(df, "test_table", mock_engine, "test_schema", "replace")
        mock_to_sql.assert_called_once_with(
            "test_table", mock_engine, schema="test_schema", if_exists="replace", index=False, chunksize=1000
        )

if __name__ == '__main__':
    unittest.main()
