from maticalgos.historical import historical
import datetime
import pandas as pd
from tqdm import tqdm
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
start_date = datetime.date(2023, 12, 1)  # Updated start date
end_date = datetime.date(2025, 2, 28)
instrument_name = "nifty"

# Initialize and login with verification
try:
    logging.info("Attempting to initialize and login...")
    ma = historical('jtutanota@gmail.com')
    ma.login("965124")
    logging.info("Login successful.")
    
except Exception as e:
    logging.error(f"Error during initialization/login: {e}")
    sys.exit(1)

# Define the required columns for the final CSV
required_columns = ['symbol', 'date', 'time', 'open', 'high', 'low', 'close', 'volume', 'oi']

# Loop through each month in the date range
current_month = start_date.replace(day=1)
while current_month <= end_date:
    # Determine the last day of the current month
    next_month = current_month.month % 12 + 1
    last_day_of_month = (current_month.replace(month=next_month, day=1) - datetime.timedelta(days=1)).day
    month_start_date = current_month
    month_end_date = current_month.replace(day=last_day_of_month)

    # Initialize an empty list to store data for the current month
    all_data = []

    # Calculate total number of days in the current month (excluding weekends)
    total_days = sum(1 for d in (month_start_date + datetime.timedelta(days=x) for x in range((month_end_date - month_start_date).days + 1))
                     if d.weekday() < 5)

    # Loop through the date range with progress bar
    current_date = month_start_date
    with tqdm(total=total_days, desc=f"Fetching data for {current_month.strftime('%B %Y')}") as pbar:
        while current_date <= month_end_date:
            # Skip Saturdays (5) and Sundays (6)
            if current_date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
                print(f"Skipping weekend date: {current_date.strftime('%Y-%m-%d')}")
                current_date += datetime.timedelta(days=1)  # Move to the next day
                continue  # Skip the rest of the loop for weekends
            
            try:
                # Fetch data for the current date
                data = ma.get_data(instrument_name, current_date)
                
                # Check if data is not empty
                if not data.empty:
                    all_data.append(data)
                
                print(f"Data fetched for {current_date.strftime('%Y-%m-%d')}")
            except Exception as e:
                print(f"Error fetching data for {current_date.strftime('%Y-%m-%d')}: {e}")
            
            # Update progress bar
            pbar.update(1)
            # Move to the next day
            current_date += datetime.timedelta(days=1)

    # Combine all DataFrames into one for the current month
    if all_data:
        print("Combining all data for the month...")
        df = pd.concat(all_data, ignore_index=True)
        
        # Ensure the DataFrame has the required columns
        if not all(col in df.columns for col in required_columns):
            raise ValueError("Fetched data does not contain all required columns.")
        
        # Select only the required columns
        df = df[required_columns]
        
        # Generate filename for the current month
        csv_filename = f"{instrument_name}_data_{month_start_date.strftime('%Y-%m')}.csv"
        
        print(f"Saving {len(df)} records to {csv_filename}...")
        df.to_csv(csv_filename, index=False)
        print(f"Data successfully exported to {csv_filename}")
    else:
        print("No data was collected for the month. Creating empty DataFrame.")
        df = pd.DataFrame(columns=required_columns)
        csv_filename = f"{instrument_name}_data_{month_start_date.strftime('%Y-%m')}.csv"
        df.to_csv(csv_filename, index=False)

    # Move to the next month
    current_month = current_month.replace(day=1, month=current_month.month % 12 + 1, year=current_month.year + (current_month.month // 12))

print("Data export completed!")