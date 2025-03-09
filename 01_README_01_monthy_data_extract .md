# README for `01_monthy_data_extract.py`

## Overview

The `01_monthy_data_extract.py` script is designed to fetch and process historical financial data for a specified instrument over a given date range. The data is fetched on a daily basis, excluding weekends, and is saved as monthly CSV files.

## Process Flow

1. **Configuration**:
   - The script is configured with a start date, end date, and the name of the financial instrument (e.g., "nifty").
   - Logging is set up to track the script's progress and any errors.

2. **Initialization and Login**:
   - The script initializes a connection to the data source using the `historical` class from the `maticalgos` package.
   - It logs in using a specified email and password.

3. **Data Fetching**:
   - The script iterates over each month within the specified date range.
   - For each month, it calculates the total number of weekdays (excluding weekends).
   - It fetches data for each weekday, updating a progress bar to indicate progress.

4. **Data Processing**:
   - Fetched data is stored in a list and combined into a single DataFrame for each month.
   - The DataFrame is checked to ensure it contains all required columns.

5. **Data Export**:
   - The processed data is saved as a CSV file named according to the instrument and month.
   - If no data is collected for a month, an empty CSV file is created.

6. **Error Handling**:
   - The script includes error handling to manage issues during login and data fetching.

## Logic Overview

- **Date Management**: The script calculates the start and end dates for each month and iterates over each day, skipping weekends.
- **Data Fetching**: Utilizes the `get_data` method to retrieve data for each day.
- **Data Validation**: Ensures that the fetched data contains all necessary columns before processing.
- **Progress Tracking**: Uses the `tqdm` library to provide a visual progress bar for data fetching.
- **Logging**: Provides detailed logging of the script's execution, including successful data fetches and any errors encountered. 