import os
import sys
import pandas as pd
from sqlalchemy import create_engine, Engine
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()

# --- Get DB Credentials (Global) ---
username = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DB")

# --- Check if all variables are loaded ---
if not all([username, password, host, port, database]):
    print("Error: Database environment variables missing. Check .env file.")
    sys.exit(1)

# --- Database Functions ---
def get_db_engine(user, pwd, hst, p, db):
    """ Creates and returns a SQLAlchemy engine with sslmode=disable. """
    try:
        db_url = f"postgresql://{user}:{pwd}@{hst}:{p}/{db}?sslmode=disable"
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        print(f"Error creating database engine: {e}")
        return None

# --- CSV Loader Function ---
def load_csv_to_hendelser(engine: Engine, csv_path: str):
    """
    Loads data from hendelser.csv into the nvdb.hendelser table.

    **IMPORTANT ASSUMPTION:** This function assumes your hendelser.csv 
    file contains columns that match the target nvdb.hendelser table, 
    specifically: 'veglenkesekvensid', 'relativ_posisjon', 
    'vegvedlikehold', 'rand_float', and 'year'.
    It assumes it DOES NOT contain 'id', 'created_at', 'updated_at',
    as these have defaults in the DB.
    *** You MAY need to adjust this function based on your CSV! ***
    """
    print(f"\n--- Loading {csv_path} ---")
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return

    try:
        df_hendelser = pd.read_csv(csv_path)
        print(f"Read {len(df_hendelser)} rows from {csv_path}.")
        print("CSV columns found:", df_hendelser.columns.tolist())

        # *** POTENTIAL ADJUSTMENT POINT ***
        # If your CSV columns don't match, you might need to:
        # 1. Select specific columns: 
        #    df_hendelser = df_hendelser[['csv_col1', 'csv_col5', ...]]
        # 2. Rename columns: 
        #    df_hendelser.rename(columns={'csv_col1': 'veglenkesekvensid', ...}, inplace=True)
        # 3. Ensure 'year' exists and is an integer. If not, derive it.
        
        # Check if 'year' column exists - CRUCIAL for partitioning
        if 'year' not in df_hendelser.columns:
            print("Error: 'year' column not found in CSV. Cannot load into partitioned table.")
            return

        # Define target table details
        table_name = "hendelser"
        schema_name = "nvdb"

        print(f"Attempting to load data into {schema_name}.{table_name}...")
        df_hendelser.to_sql(
            table_name,
            engine,
            schema=schema_name,
            if_exists='append', # Use 'append' to add to existing table
            index=False,
            chunksize=1000
        )
        print(f"Successfully loaded data into {schema_name}.{table_name}.")

    except FileNotFoundError:
        print(f"Error: Could not find the CSV file at {csv_path}")
    except Exception as e:
        print(f"An error occurred while loading CSV data: {e}")

# --- Data Checker Function ---
def check_vegobjekter_data(engine: Engine):
    """ Checks for data in nvdb.vegobjekter_fartsgrense and prints a sample. """
    print("\n--- Checking nvdb.vegobjekter_fartsgrense ---")
    table_name = "vegobjekter_fartsgrense"
    schema_name = "nvdb"
    
    try:
        # Check count
        count_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name};"
        df_count = pd.read_sql_query(count_query, engine)
        count = df_count.iloc[0, 0]
        print(f"Found {count} rows in {schema_name}.{table_name}.")

        # Show sample if data exists
        if count > 0:
            sample_query = f"SELECT * FROM {schema_name}.{table_name} LIMIT 5;"
            df_sample = pd.read_sql_query(sample_query, engine)
            print("\nSample data:")
            print(df_sample)
        else:
            print("Table appears to be empty.")

    except Exception as e:
        print(f"An error occurred while checking data: {e}")


# --- Main Execution ---
def main():
    """ Runs the CSV loading and data checking tasks. """
    db_engine = get_db_engine(username, password, host, port, database)

    if not db_engine:
        print("Could not connect to database. Exiting.")
        return

    try:
        # 1. Load the CSV data
        csv_file_path = 'sql/hendelser.csv'
        load_csv_to_hendelser(db_engine, csv_file_path)

        # 2. Check the API data
        check_vegobjekter_data(db_engine)

    finally:
        # Ensure connection is closed
        if db_engine:
            db_engine.dispose()
            print("\nDatabase connection closed.")


# --- Script Entry Point ---
if __name__ == "__main__":
    main()
