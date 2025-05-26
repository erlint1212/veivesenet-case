import os
import sys
import requests
import pandas as pd
from sqlalchemy import create_engine, Engine, text
from dotenv import load_dotenv
from typing import Optional, Any
import json

# --- Load Environment Variables ---
load_dotenv()

# --- Get DB Credentials (Global) ---
username = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DB")

# --- Get NVDB Config (Global) ---
nvdb_base_url = os.getenv("NVDB_BASE_URL", "https://nvdbapiles-v3.atlas.vegvesen.no")
nvdb_object_id = os.getenv("NVDB_OBJECT_ID")
nvdb_param_inkluder = os.getenv("NVDB_PARAM_INKLUDER", "alle") # Ensure 'alle' or 'lokasjon' is included
nvdb_param_srid = os.getenv("NVDB_PARAM_SRID")
nvdb_param_segmentering = os.getenv("NVDB_PARAM_SEGMENTERING")
nvdb_param_trafikantgruppe = os.getenv("NVDB_PARAM_TRAFIKANTGRUPPE")
nvdb_param_fylke = os.getenv("NVDB_PARAM_FYLKE")
nvdb_param_endret_etter = os.getenv("NVDB_PARAM_ENDRET_ETTER")

# --- Check required variables ---
if not all([username, password, host, port, database]):
    print("Error: Database environment variables missing. Check .env file.")
    sys.exit(1)
if not nvdb_object_id:
    print("Error: NVDB_OBJECT_ID environment variable missing. Check .env file.")
    sys.exit(1)

# --- Function Definitions ---

def fetch_nvdb_data_paginated(object_id: str, params: dict) -> list:
    """ Fetches NVDB data, handling pagination and stopping on 0 results. """
    all_objects = []
    current_url = f"{nvdb_base_url}/vegobjekter/{object_id}"
    headers = {'Accept': 'application/vnd.vegvesen.nvdb-v3-rev1+json'}
    print(f"Fetching data from: {current_url} with params: {params}")
    
    while current_url:
        try:
            response = requests.get(current_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            objects_on_page = data.get('objekter', [])
            
            if not objects_on_page and len(all_objects) > 0: # Check if empty *after* getting some data
                print("Fetched 0 objects, assuming end of data. Stopping pagination.")
                break
            elif not objects_on_page: # Handle case where first page is empty
                 print("Fetched 0 objects on the first page. No data found for these criteria.")
                 break

            all_objects.extend(objects_on_page)
            print(f"Fetched {len(objects_on_page)} objects. Total: {len(all_objects)}")
            current_url = data.get('metadata', {}).get('neste', {}).get('href')
            params = {}
        except requests.exceptions.RequestException as e:
            print(f"Error during API request: {e}")
            current_url = None
    print(f"Finished fetching. Total objects: {len(all_objects)}")
    return all_objects

def get_veglenke(obj: dict) -> Optional[int]:
    """Safely extracts the primary veglenkesekvensid from 'stedfestinger'."""
    try:
        # Get the 'stedfestinger' list, default to empty list if not found
        stedfestinger = obj.get('lokasjon', {}).get('stedfestinger', [])
        
        # If the list is not empty, get the 'veglenkesekvensid' 
        # from the *first* element.
        if stedfestinger:
            return stedfestinger[0].get('veglenkesekvensid')
            
        # If the list is empty or key not found, return None
        return None
        
    except Exception as e:
        # Catch any unexpected errors during extraction
        print(f"Warning: Error extracting veglenke for obj {obj.get('id')}: {e}")
        return None

def get_property(obj: dict, prop_name: str) -> Optional[Any]:
    """Safely extracts a specific property value."""
    try:
        for prop in obj.get('egenskaper', []):
            if prop.get('navn', '').lower() == prop_name.lower():
                return prop.get('verdi')
        return None
    except Exception:
        return None

def process_nvdb_objects(objects: list) -> pd.DataFrame:
    """ Processes NVDB objects into a DataFrame matching the table structure. """
    processed_list = []

    for obj in objects:

        processed_list.append({
            'nvdb_id': obj.get('id'),
            'vegkategori': obj.get('lokasjon', {}).get('vegsystemreferanser', [{}])[0].get('vegsystem', {}).get('vegkategori'),
            'fylke': obj.get('lokasjon', {}).get('fylker', [None])[0],
            'kommune': obj.get('lokasjon', {}).get('kommuner', [None])[0],
            'veglenkesekvensid': get_veglenke(obj), # **ADDED/IMPROVED**
            'startdato': obj.get('metadata', {}).get('startdato'),
            'sist_modifisert': obj.get('metadata', {}).get('sist_modifisert'),
            'geometri_wkt': obj.get('geometri', {}).get('wkt'),
            'fartsgrense': get_property(obj, 'Fartsgrense') # **IMPROVED**
        })

    if not processed_list: return pd.DataFrame()
    df = pd.DataFrame(processed_list)
    df['startdato'] = pd.to_datetime(df['startdato'], errors='coerce')
    df['sist_modifisert'] = pd.to_datetime(df['sist_modifisert'], errors='coerce')
    # Ensure fartsgrense is numeric, set errors to None (NULL)
    df['fartsgrense'] = pd.to_numeric(df['fartsgrense'], errors='coerce') 
    # Ensure IDs are numeric (handle potential None before converting)
    df['nvdb_id'] = pd.to_numeric(df['nvdb_id'], errors='coerce').astype('Int64')
    df['veglenkesekvensid'] = pd.to_numeric(df['veglenkesekvensid'], errors='coerce').astype('Int64')
    df['fylke'] = pd.to_numeric(df['fylke'], errors='coerce').astype('Int64')
    df['kommune'] = pd.to_numeric(df['kommune'], errors='coerce').astype('Int64')

    return df

def get_db_engine(user, pwd, hst, p, db):
    """ Creates and returns a SQLAlchemy engine with sslmode=disable. """
    try:
        # --- MODIFIED LINE ---
        db_url = f"postgresql://{user}:{pwd}@{hst}:{p}/{db}?sslmode=disable"
        # -------------------
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        print(f"Error creating database engine: {e}")
        return None

def load_df_to_postgres(df: pd.DataFrame, table_name: str, engine: Engine, schema: str, if_exists: str = 'append'):
    """ Loads a Pandas DataFrame into a PostgreSQL table. """
    if df.empty:
        print("DataFrame is empty. Nothing to load.")
        return

    print(f"Loading {len(df)} rows into {schema}.{table_name}...")
    try:
        df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists=if_exists, 
            index=False,      
            chunksize=1000      
        )
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")

# --- Main Execution Logic ---
def main():
    """ Builds API params, fetches, processes, and loads data. """
    api_params = {}
    if nvdb_param_inkluder: api_params['inkluder'] = nvdb_param_inkluder
    if nvdb_param_srid: api_params['srid'] = nvdb_param_srid
    if nvdb_param_segmentering: api_params['segmentering'] = nvdb_param_segmentering
    if nvdb_param_trafikantgruppe: api_params['trafikantgruppe'] = nvdb_param_trafikantgruppe
    if nvdb_param_fylke: api_params['fylke'] = nvdb_param_fylke
    if nvdb_param_endret_etter: api_params['endret_etter'] = nvdb_param_endret_etter

    print(f"--- Configuration ---")
    print(f"Fetching Object ID: {nvdb_object_id}")
    print(f"Using API Params: {api_params}")
    print(f"---------------------")

    nvdb_objects = fetch_nvdb_data_paginated(nvdb_object_id, api_params)
    if not nvdb_objects: return

    df_nvdb = process_nvdb_objects(nvdb_objects)
    if df_nvdb.empty: return

    print("\nProcessed Data Sample (first 5 rows):")
    print(df_nvdb.head())

    db_engine = get_db_engine(username, password, host, port, database)
    
    if db_engine:
        target_schema = "nvdb"
        target_table = "vegobjekter_fartsgrense" # **UPDATED**
        
        # Ensure schema exists (Goose should do this, but doesn't hurt)
        try:
            with db_engine.connect() as connection:
                print(f"Ensuring '{target_schema}' schema exists...")
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {target_schema};"))
                connection.commit() 
                print(f"'{target_schema}' schema confirmed.")
        except Exception as e:
            print(f"Error creating/checking schema: {e}")
            db_engine.dispose()
            return

        # Load data - Using 'replace' for easy re-runs during testing. 
        # Change to 'append' if you want to add data incrementally.
        load_df_to_postgres(df_nvdb, target_table, db_engine, schema=target_schema, if_exists='replace') 
        
        db_engine.dispose()
        print("\nDatabase connection closed.")

# --- Script Entry Point ---
if __name__ == "__main__":
    main()
