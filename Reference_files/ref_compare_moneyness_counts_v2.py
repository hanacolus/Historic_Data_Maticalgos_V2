import duckdb
import pandas as pd

# File Configuration
DB_FILE = 'nifty_data_2023-12.duckdb'
OUTPUT_FILE = 'nifty_data_2023-12_com.parquet'
CSV_OUTPUT_FILE = 'none_moneyness_records_v2.csv'  # New CSV output file

# Connect to DuckDB
db_conn = duckdb.connect(DB_FILE)

# Query moneyness counts from cal_data table
cal_data_counts = db_conn.execute("""
    SELECT moneyness, COUNT(*) as count
    FROM cal_data
    GROUP BY moneyness
""").fetchall()

# Convert to dictionary for easy comparison
cal_data_dict = {row[0]: row[1] for row in cal_data_counts}

# Read the output Parquet file
output_df = pd.read_parquet(OUTPUT_FILE)

# Get moneyness counts from the Parquet file
output_counts = output_df['moneyness'].value_counts().to_dict()

# Compare the counts
print("Comparison of moneyness counts:")
for moneyness in set(cal_data_dict.keys()).union(output_counts.keys()):
    cal_count = cal_data_dict.get(moneyness, 0)
    output_count = output_counts.get(moneyness, 0)
    print(f"{moneyness}: cal_data = {cal_count}, output_file = {output_count}")

# Query all columns for records with None in moneyness
none_moneyness_records = db_conn.execute("""
    SELECT *
    FROM cal_data
    WHERE moneyness IS NULL
""").fetchdf()

# Export the entire data to CSV
none_moneyness_records.to_csv(CSV_OUTPUT_FILE, index=False)
print(f"\nExported entire data for records with None in moneyness to {CSV_OUTPUT_FILE}")

# Close the database connection
db_conn.close() 