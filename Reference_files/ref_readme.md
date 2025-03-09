# Moneyness Data Processing

This project consists of a set of Python scripts designed to process and analyze moneyness data stored in DuckDB and Parquet files. The scripts perform various tasks such as inspecting file schemas, counting rows, comparing data, and exporting specific records.

## Scripts Overview

1. **`ref_compare_moneyness_counts_v2.py`**:
   - Connects to a DuckDB database and retrieves moneyness counts from the `cal_data` table.
   - Reads a Parquet file and compares moneyness counts with those from the database.
   - Exports records with `None` in the moneyness column to a CSV file.

2. **`ref_count_parquet_rows.py`**:
   - Counts the number of rows in a specified Parquet file.
   - Displays the results in a formatted manner.

3. **`ref_inspect_parquet.py`**:
   - Inspects a Parquet file to display its schema and column names.

4. **`ref_moneyness_count.py`**:
   - Counts the number of rows for each moneyness category in a Parquet file.

## Process Flow

1. **Inspect Parquet File**:
   - Use `ref_inspect_parquet.py` to inspect the Parquet file (`nifty_data_2023-12.parquet`) to understand its schema and column names.

2. **Count Rows in Parquet File**:
   - Use `ref_count_parquet_rows.py` to count the total number of rows in the Parquet file. This provides a quick overview of the data size.

3. **Compare Moneyness Counts**:
   - Use `ref_compare_moneyness_counts_v2.py` to:
     - Connect to the DuckDB database and retrieve moneyness counts.
     - Compare these counts with those in the Parquet file.
     - Export records with `None` in the moneyness column to a CSV file for further analysis.

4. **Count Moneyness Categories**:
   - Use `ref_moneyness_count.py` to count the number of rows for each moneyness category in the Parquet file. This provides detailed insights into the distribution of moneyness values.

## Requirements

- Python 3.x
- DuckDB
- Pandas
- PyArrow

## Setup

1. Install the required Python packages:
   ```bash
   pip install duckdb pandas pyarrow
   ```

2. Ensure the Parquet and DuckDB files are in the same directory as the scripts or provide the correct paths.

## License

This project is licensed under the MIT License.