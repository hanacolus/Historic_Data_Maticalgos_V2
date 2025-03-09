import pyarrow.parquet as pq

def inspect_parquet_file(file_path):
    # Open the Parquet file
    parquet_file = pq.ParquetFile(file_path)
    
    # Get the schema of the Parquet file
    schema = parquet_file.schema
    
    # Print the table name (Parquet files don't have a table name, but you can use the file name)
    print(f"Table Name: {file_path}")
    
    # Print the column names
    print("Column Names:")
    for name in schema.names:
        print(f"  - {name}")

# Example usage
parquet_file_path = 'nifty_data_2023-12.parquet'
inspect_parquet_file(parquet_file_path) 