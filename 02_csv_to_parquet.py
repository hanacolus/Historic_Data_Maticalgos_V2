import pandas as pd
import os

# Define the input and output directories
input_dir = r'C:\Users\Professor\Desktop\excel in'
output_dir = r'C:\Users\Professor\Desktop\par out'

# Create output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Loop through all CSV files in the input directory
for filename in os.listdir(input_dir):
    if filename.endswith('.csv'):
        csv_file_path = os.path.join(input_dir, filename)
        parquet_file_path = os.path.join(output_dir, filename.replace('.csv', '.parquet'))
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file_path)
            # Convert to Parquet format
            df.to_parquet(parquet_file_path, index=False)
            print(f'Converted {filename} to Parquet format.')
        except Exception as e:
            print(f'Error converting {filename}: {e}')
