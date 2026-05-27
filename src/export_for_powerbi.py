# This python script checks in the sql exports folder for the right file and converts them to csv file.
# You can add files to be converted to csv by simply adding an extra tuple in the exports list.
# The output will be found in data/processed.

import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

exports = [
    ('src/exports/rolling_average_30d.sql', 'data/processed/rolling_avg_30d.csv'),
]

for sql_file, output_file in exports:
    with open(sql_file, 'r') as f:
        query = f.read()
    df = pd.read_sql(query, conn)
    df.to_csv(output_file, index=False)
    print(f"Exported {len(df)} rows to {output_file}")

conn.close()