# Script to perform ETL process: extract data from CSV, transform it, and load into PostgreSQL

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import logging
from datetime import datetime


# ============================================
# Setup logging
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================
# Database connection
# ============================================
load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )


# ============================================
# Extract
# ============================================
def extract(filepath: str) -> pd.DataFrame:
    logger.info(f"Extracting data from {filepath}")
    
    # Skip comments at the top of the raw file
    df = pd.read_csv(
        filepath,
        comment='#',
        skipinitialspace=True,
        header=None,  # geen header in de data
        names=['STN', 'YYYYMMDD', 'FG', 'TG', 'TN', 'TX', 'SQ', 'SP', 'RH']
    )
    
    # Clean column names
    df.columns = df.columns.str.strip()
    logger.info(f"Extracted {len(df)} rows, {len(df.columns)} columns")
    return df


# ============================================
# Transform
# ============================================
def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Transforming data...")
    
    # Rename columns
    df = df.rename(columns={
        'STN': 'station_id',
        'YYYYMMDD': 'date',
        'FG': 'wind_speed_avg',
        'TG': 'temp_avg',
        'TN': 'temp_min',
        'TX': 'temp_max',
        'SQ': 'sunshine_hrs',
        'SP': 'sunshine_pct',
        'RH': 'precipitation'
    })

    # Parse date
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    
    # Convert tenths to actual values
    df['wind_speed_avg'] = df['wind_speed_avg'] / 10
    df['temp_avg']       = df['temp_avg'] / 10
    df['temp_min']       = df['temp_min'] / 10
    df['temp_max']       = df['temp_max'] / 10
    df['sunshine_hrs']   = df['sunshine_hrs'] / 10
    df['precipitation']  = df['precipitation'] / 10
    
    # Fix -1 values (meaning <0.05) -> 0
    df['sunshine_hrs']  = df['sunshine_hrs'].clip(lower=0)
    df['precipitation'] = df['precipitation'].clip(lower=0)
    
    # Build dim_date
    dates = df['date'].drop_duplicates().sort_values()


    def get_season(month):
        if month in [12, 1, 2]:  return 'Winter'
        if month in [3, 4, 5]:   return 'Lente'
        if month in [6, 7, 8]:   return 'Zomer'
        return 'Herfst'
    
    dim_date = pd.DataFrame({
        'date':         dates,
        'year':         dates.dt.year,
        'month':        dates.dt.month,
        'month_name':   dates.dt.strftime('%B'),
        'quarter':      dates.dt.quarter,
        'season':       dates.dt.month.map(get_season),
        'weekday':      dates.dt.weekday + 1,  # 1=maandag
        'weekday_name': dates.dt.strftime('%A'),
        'is_weekend':   dates.dt.weekday >= 5
    })
    
    logger.info(f"Built dim_date with {len(dim_date)} rows")
    return df, dim_date

# ============================================
# Load
# ============================================
def load_stations(conn):
    stations = [
        (240, 'Schiphol Airport',        4.790, 52.318, -3.30, 'Noord-Holland', 'coastal'),
        (260, 'De Bilt',                 5.180, 52.100,  1.90, 'Utrecht',       'inland'),
        (280, 'Groningen Airport Eelde', 6.585, 53.125,  5.20, 'Groningen',     'inland'),
        (310, 'Vlissingen',              3.596, 51.442,  8.00, 'Zeeland',       'coastal'),
        (370, 'Eindhoven Airport',       5.377, 51.451, 22.60, 'Noord-Brabant', 'inland'),
    ]
    
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO dim_station 
                (station_id, name, longitude, latitude, altitude_m, region, coast_type)
            VALUES %s
            ON CONFLICT (station_id) DO NOTHING
        """, stations)
    conn.commit()
    logger.info(f"Loaded {len(stations)} stations")

def load_dim_date(conn, dim_date: pd.DataFrame):
    records = [tuple(row) for row in dim_date.itertuples(index=False)]
    
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO dim_date
                (date, year, month, month_name, quarter, season, weekday, weekday_name, is_weekend)
            VALUES %s
            ON CONFLICT (date) DO NOTHING
        """, records)
    conn.commit()
    logger.info(f"Loaded {len(records)} date records")


def load_fact_weather(conn, df: pd.DataFrame):
    # Get date_id mapping
    with conn.cursor() as cur:
        cur.execute("SELECT date, date_id FROM dim_date")
        date_map = {row[0]: row[1] for row in cur.fetchall()}
    
    df['date_id'] = df['date'].map(date_map)

    # Explicit type conversion 
    df['date_id']      = df['date_id'].astype(int)
    df['station_id']   = df['station_id'].astype(int)
    df['sunshine_pct'] = df['sunshine_pct'].fillna(0).astype(int)
    
    records = df[[
        'station_id', 'date_id', 'wind_speed_avg',
        'temp_avg', 'temp_min', 'temp_max',
        'sunshine_hrs', 'sunshine_pct', 'precipitation'
    ]].itertuples(index=False)

    
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fact_weather
                (station_id, date_id, wind_speed_avg, temp_avg, temp_min, temp_max,
                 sunshine_hrs, sunshine_pct, precipitation)
            VALUES %s
            ON CONFLICT (station_id, date_id) DO NOTHING
        """, list(records))
    conn.commit()
    logger.info(f"Loaded {len(df)} weather records")


# ============================================
# Main
# ============================================
def main():
    filepath = 'data/raw/result.txt'
    
    conn = get_connection()
    logger.info("Connected to database")
    
    try:
        df = extract(filepath)
        df, dim_date = transform(df)
        
        load_stations(conn)
        load_dim_date(conn, dim_date)
        load_fact_weather(conn, df)
        
        logger.info("ETL completed successfully!")
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    main()

