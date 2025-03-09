# CSV to Parquet Conversion Script

## Overview
This script is designed to convert CSV files into Parquet format. It processes all CSV files located in a specified input directory and outputs the converted Parquet files to a specified output directory. This conversion is useful for optimizing storage and improving data processing performance, as Parquet is a columnar storage file format that is highly efficient for analytical queries.

## Process Flow
1. **Define Directories**:
   - The script begins by defining the input and output directories.
   - `input_dir`: Directory containing the CSV files to be converted.
   - `output_dir`: Directory where the converted Parquet files will be saved.

2. **Create Output Directory**:
   - If the output directory does not exist, the script creates it to ensure that the converted files have a destination.

3. **Iterate Over CSV Files**:
   - The script loops through all files in the input directory.
   - It checks if each file has a `.csv` extension to ensure only CSV files are processed.

4. **Convert CSV to Parquet**:
   - For each CSV file, the script reads the file into a Pandas DataFrame.
   - It then converts the DataFrame to Parquet format using the `to_parquet` method.
   - The Parquet file is saved in the output directory with the same name as the original CSV file, but with a `.parquet` extension.

5. **Error Handling**:
   - The script includes a try-except block to catch and report any errors that occur during the conversion process.
   - If an error occurs, it prints an error message indicating which file failed to convert and the reason for the failure.

## Usage
- Ensure that the input directory contains the CSV files you wish to convert.
- Run the script to automatically convert all CSV files in the input directory to Parquet format in the output directory.

## Dependencies
- `pandas`: Used for reading CSV files and writing Parquet files.
- `os`: Used for file and directory operations.

## Notes
- The script assumes that all files in the input directory with a `.csv` extension are valid CSV files.
- Ensure that the necessary permissions are set for reading from the input directory and writing to the output directory. 