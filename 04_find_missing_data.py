import os
import pandas as pd
import glob
from tqdm import tqdm

def find_missing_data_and_copy_to_validation(parquet_dir, validation_csv="validation_missing_data.csv", report_csv="missing_data_report.csv"):
    """
    Finds rows with missing data in parquet files and:
    1. Creates a detailed report of missing data
    2. Copies the entire rows with missing data to a validation CSV file
    
    Args:
        parquet_dir (str): Directory containing parquet files
        validation_csv (str): Output CSV file for rows with missing data
        report_csv (str): Output CSV file for the detailed report
    """
    print(f"Checking directory: {parquet_dir}")
    
    # Ensure the directory exists
    if not os.path.exists(parquet_dir):
        print(f"Error: Directory '{parquet_dir}' does not exist.")
        return
    
    # Find all parquet files in the directory
    parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
    
    if not parquet_files:
        print(f"No parquet files found in '{parquet_dir}'.")
        try:
            print("Directory contents:")
            for item in os.listdir(parquet_dir):
                print(f"  - {item}")
        except Exception as e:
            print(f"Error listing directory: {str(e)}")
        return
    
    print(f"Found {len(parquet_files)} parquet files. Checking for missing data...")
    
    # Create DataFrames to store results
    report_data = []
    missing_rows_df = pd.DataFrame()
    
    # Track statistics
    total_files_with_missing = 0
    total_rows_with_missing = 0
    
    # Process each parquet file
    for parquet_file in tqdm(parquet_files, desc="Processing files"):
        filename = os.path.basename(parquet_file)
        
        try:
            # Read the parquet file
            df = pd.read_parquet(parquet_file)
            
            # Find rows with missing data (any column has a null value)
            null_mask = df.isnull().any(axis=1)
            rows_with_missing = df[null_mask]
            
            if len(rows_with_missing) > 0:
                total_files_with_missing += 1
                total_rows_with_missing += len(rows_with_missing)
                
                # Add source filename column to the rows with missing data
                rows_with_missing['source_file'] = filename
                
                # Append to the validation DataFrame
                missing_rows_df = pd.concat([missing_rows_df, rows_with_missing], ignore_index=True)
                
                # Process each row with missing data for the report
                for row_idx, row in rows_with_missing.iterrows():
                    missing_cols = row.index[row.isnull()].tolist()
                    report_data.append({
                        'Filename': filename,
                        'Row Number': row_idx,
                        'Missing Columns': ', '.join(missing_cols)
                    })
                
                print(f"File {filename}: {len(rows_with_missing)} rows with missing data out of {len(df)}")
            
        except Exception as e:
            print(f"Error processing file {filename}: {str(e)}")
            report_data.append({
                'Filename': filename,
                'Row Number': "ERROR",
                'Missing Columns': str(e)
            })
    
    # Save the report
    if report_data:
        pd.DataFrame(report_data).to_csv(report_csv, index=False)
        print(f"Detailed report saved to {report_csv}")
    
    # Save the validation CSV with all rows that have missing data
    if not missing_rows_df.empty:
        missing_rows_df.to_csv(validation_csv, index=False)
        print(f"All rows with missing data saved to {validation_csv}")
    
    # Print summary
    print("\nSummary:")
    print(f"Total files checked: {len(parquet_files)}")
    print(f"Files with missing data: {total_files_with_missing}")
    print(f"Total rows with missing data: {total_rows_with_missing}")
    print(f"Analysis complete.")

if __name__ == "__main__":
    # Directory containing parquet files
    parquet_directory = r"C:\Codes\02_parquet_database\parquet\parquet_pro"
    
    # Output CSV files
    validation_file = "validation_missing_data.csv"
    report_file = "missing_data_report.csv"
    
    # Run the analysis
    find_missing_data_and_copy_to_validation(parquet_directory, validation_file, report_file)
    
    # Keep console open to see results
    input("Press Enter to exit...") 