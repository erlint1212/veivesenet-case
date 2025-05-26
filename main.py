import os
import pandas as pd
from sqlalchemy import create_engine, Engine
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# --- Load Environment Variables ---
load_dotenv()

# --- Get DB Credentials (Global) ---
username = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DB")

# Check if all variables are loaded
if not all([username, password, host, port, database]):
    print("Error: Database environment variables missing. Check .env file.")
    exit()

# --- Functions ---

def get_db_engine(user, pwd, hst, p, db):
    """ Creates and returns a SQLAlchemy engine with sslmode=disable. """
    try:
        # Using sslmode=disable as per previous request
        db_url = f"postgresql://{user}:{pwd}@{hst}:{p}/{db}?sslmode=disable"
        engine = create_engine(db_url)
        # Test connection
        with engine.connect() as connection:
            print("Database connection successful.")
        return engine
    except Exception as e:
        print(f"Error creating database engine: {e}")
        return None

def sql_request(sql_code: str, db_engine: Engine) -> pd.DataFrame:
    """ Executes an SQL query and returns the result as a Pandas DataFrame. """
    try:
        df = pd.read_sql_query(sql_code, db_engine)
        print(f"Query executed successfully, {len(df)} rows returned.")
        return df
    except Exception as e:
        print(f"Error during SQL query: {e}")
        return pd.DataFrame() # Return an empty DataFrame on error

def plot_incidents_per_year(df: pd.DataFrame) -> None:
    """ Plots the number of incidents per year per road category. """
    if not isinstance(df, pd.DataFrame) or df.empty:
        print("Input DataFrame is empty or invalid. Skipping plot.")
        return

    if not {'year', 'vegkategori'}.issubset(df.columns):
        print("DataFrame is missing 'year' and/or 'vegkategori' columns. Skipping plot.")
        return

    try:
        print("Preparing data for plotting...")
        # Group by `year` and `vegkategori`, and count occurrences
        # *** FIX: Use 'df' instead of 'joined_dataframe' ***
        counts_per_year_category = df.groupby(['year', 'vegkategori']).size()

        # Unstack, moving `vegkategori` to columns
        plot_data = counts_per_year_category.unstack(fill_value=0)

        if plot_data.empty:
            print("No data to plot after grouping.")
            return

        print("Making plot...")
        fig, ax = plt.subplots(figsize=(14, 8)) # Renamed 'fix' to 'fig'

        plot_data.plot(kind='bar', ax=ax, width=0.8)

        ax.set_title("Antall hendelser per år og vegkategori", fontsize=18, pad=20)
        ax.set_xlabel("År", fontsize=14)
        ax.set_ylabel("Antall hendelser", fontsize=14)
        ax.tick_params(axis='x', rotation=45, labelsize=12)
        ax.tick_params(axis='y', labelsize=12)
        ax.legend(title='Vegkategori', fontsize=11, title_fontsize=13)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))

        plt.tight_layout()
        plt.show()
        print("Plot displayed.")

    except Exception as e:
        print(f"An error occurred during plotting: {e}")
    
    print("Save plot...")
    try:
        output_folder = "images"
        output_filename = "hendelser_per_aar.png"
        output_path = os.path.join(output_folder, output_filename)
        
        # Create the 'images' folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)

        fig.savefig(output_path, bbox_inches='tight', dpi=150) 
        print("Plot saved.")
    except Exception as e:
        print(f"An error occurred during plot saving: {e}")

    plt.close()


def main() -> None:
    """ Main function to connect, query, and plot data. """
    engine = get_db_engine(username, password, host, port, database)

    if not engine:
        return

    join_sql = """
    SELECT
        vf.nvdb_id,
        vf.veglenkesekvensid,  -- Fellesnøkkelen (Using the name from your DB)
        vf.vegkategori,
        vf.fartsgrense,
        h.relativ_posisjon,
        h.vegvedlikehold,
        h.year
    FROM
        nvdb.vegobjekter_fartsgrense vf 
    INNER JOIN
        nvdb.hendelser h
    ON
        vf.veglenkesekvensid = h.veglenkesekvensid; -- *** FIX: Use 'veglenkesekvensid' on both sides ***
    """

    print("\n--- Running JOIN Query ---")
    joined_dataframe = sql_request(join_sql, engine)

    if not joined_dataframe.empty:
        print("\nJoined DataFrame sample:")
        print(joined_dataframe.head())

        print("\nMapping 'vegkategori' to long names...")
        vegkategori_map = {
            'E': 'Europaveg',
            'F': 'Fylkesveg',
            'K': 'Kommunal veg',
            'P': 'Privat veg',
            'R': 'Riksveg',
            'S': 'Skogsveg'
        }
        # Use .map() to replace codes. .fillna() keeps original if no map found.
        joined_dataframe['vegkategori'] = joined_dataframe['vegkategori'].map(vegkategori_map).fillna(joined_dataframe['vegkategori'])
        print("\nJoined DataFrame sample (after mapping):")
        print(joined_dataframe.head())
    else:
        print("Joined DataFrame is empty - check if both tables have data and if join keys match.")

    # --- Plotting ---
    plot_incidents_per_year(joined_dataframe)

    # --- Clean up ---
    engine.dispose()
    print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()
