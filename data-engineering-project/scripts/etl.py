import pandas as pd
from sqlalchemy import create_engine # type: ignore
import logging
import os

# Ensure logs directory exists
logs_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure logging
logging.basicConfig(filename=os.path.join(logs_dir, 'etl.log'), level=logging.INFO)

def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError as e:
        logging.error(f"File not found: {e.filename}")
        raise
    except Exception as e:
        logging.error(f"Error occurred during data extraction: {str(e)}", exc_info=True)
        raise

def clean_data(df):
    try:
        df.dropna(inplace=True)
        df['rating'] = df['rating'].astype(float)
        return df
    except Exception as e:
        logging.error(f"Error occurred during data cleaning: {str(e)}", exc_info=True)
        raise

def aggregate_data(df):
    try:
        avg_ratings = df.groupby('movieId')['rating'].mean().reset_index()
        return avg_ratings
    except Exception as e:
        logging.error(f"Error occurred during data aggregation: {str(e)}", exc_info=True)
        raise

def load_data(df, table_name, db_url):
    try:
        engine = create_engine(db_url)
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        return engine
    except Exception as e:
        logging.error(f"Error occurred during data loading: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        # Parameters
        file_path = r"C:\Users\hurri\OneDrive\Desktop\Data Engineering Projects\data-engineering-project\data\movies.csv"  # Adjust the path as per your directory structure
        table_name = "movies"
        db_url = "sqlite:///../data/movies.db"  # Adjust the path as per your directory structure

        # Extract data
        data = extract_data(file_path)

        # Clean data
        clean_data = clean_data(data)

        # Aggregate data
        aggregated_data = aggregate_data(clean_data)

        # Load data into database
        engine = load_data(aggregated_data, table_name, db_url)
        logging.info("Data loaded successfully!")

        # Optional: Verify data
        result = engine.execute(f"SELECT * FROM {table_name}").fetchall()
        logging.info(f"Loaded {len(result)} records from {table_name}")

        print("ETL process completed successfully!")

    except Exception as e:
        logging.error("Error occurred during ETL process: ", exc_info=True)
        print("ETL process failed. Check logs for details.")
