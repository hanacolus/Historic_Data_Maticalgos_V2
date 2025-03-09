import pandas as pd
import sys

###########################################
# Input File Section
# This script takes a parquet file path as
# command line argument
# Input file: 01_NIFTY_Jan2024.parquet
###########################################

# Default input file
input_file = "nifty_data_2023-12.parquet"

def display_results(file_name, num_rows):
    print("\n" + "="*50)
    print("PARQUET FILE ROW COUNT RESULTS")
    print("="*50)
    print(f"File name: {file_name}")
    print(f"Total rows: {num_rows:,}")
    print("="*50 + "\n")

def count_parquet_rows(parquet_file):
    try:
        # Read the parquet file
        df = pd.read_parquet(parquet_file)
        
        # Get the number of rows
        num_rows = len(df)
        
        # Display results in a formatted way
        display_results(parquet_file, num_rows)
        return num_rows
    
    except Exception as e:
        print("\nERROR" + "!"*20)
        print(f"Error reading parquet file: {str(e)}")
        print("!"*25 + "\n")
        return None

# Check command line arguments
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python count_parquet_rows.py <parquet_file_path>")
        print(f"Using default file: {input_file}")
        parquet_file = input_file
    else:
        parquet_file = sys.argv[1]
    
    count_parquet_rows(parquet_file) 