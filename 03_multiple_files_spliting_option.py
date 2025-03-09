import duckdb
import time
import psutil
import os
import argparse
from tqdm import tqdm
import gc
from functools import wraps
import warnings
import logging
from pathlib import Path
import traceback
import glob

# ====================================
# USER INPUT SECTION - MODIFY THESE VALUES
# ====================================

# Define input and output paths
input_folder = r'C:\Users\Professor\Desktop\par out'    # Folder containing input files
output_folder = r'C:\Users\Professor\Desktop\con out'  # Output folder

# Processing configuration
memory_percent = 75    # Percentage of system memory to use
validate_data = True   # Perform additional data validation
keep_temp_files = False  # Keep temporary DuckDB files after processing
use_parallel = False   # Set to False to disable parallel processing
file_pattern = "*.parquet"  # Pattern to match files in the input directory

# ====================================
# SYSTEM SETUP - DO NOT MODIFY
# ====================================

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Suppress warnings
warnings.filterwarnings('ignore')

# System Configuration - dynamically detected
CPU_COUNT = os.cpu_count() or 1
MEMORY_GB = psutil.virtual_memory().total / (1024 ** 3)
CHUNK_SIZE = min(50000, int(MEMORY_GB * 1000))  # Dynamic chunk size based on RAM

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process multiple options data files with DuckDB')
    parser.add_argument('--input-folder', '-if', default=input_folder,
                      help=f'Input folder containing parquet files (default: {input_folder})')
    parser.add_argument('--output-folder', '-of', default=output_folder,
                      help=f'Output folder for processed files (default: {output_folder})')
    parser.add_argument('--keep-temp', '-k', action='store_true', default=keep_temp_files,
                      help='Keep temporary DuckDB files (default: delete)')
    parser.add_argument('--memory-percent', '-m', type=int, default=memory_percent,
                      help=f'Percentage of system memory to use (default: {memory_percent}%)')
    parser.add_argument('--validate', '-v', action='store_true', default=validate_data,
                      help='Perform additional data validation (may be slower)')
    parser.add_argument('--file-pattern', '-fp', default=file_pattern,
                      help=f'File pattern to match in input folder (default: {file_pattern})')
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    os.makedirs(args.output_folder, exist_ok=True)
    
    return args

def time_tracker(func):
    """Decorator to track execution time and memory usage of functions."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        gc.collect()
        start_memory = psutil.Process().memory_info().rss / (1024 * 1024)
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss / (1024 * 1024)
            execution_time = end_time - start_time
            memory_change = end_memory - start_memory
            
            logger.info(f"{func.__name__}:")
            logger.info(f"  Time: {execution_time:.2f} seconds")
            logger.info(f"  Memory Change: {memory_change:.2f} MB")
            logger.info(f"  Final Memory: {end_memory:.2f} MB")
            return result
            
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    return wrapper

class DuckDBProcessor:
    def __init__(self, memory_percent=75, validate=False, keep_temp=False):
        """Initialize the processor with configuration options."""
        self.memory_percent = memory_percent
        self.validate = validate
        self.keep_temp = keep_temp
        self.conn = None
        self.current_file = None
        self.current_output = None
        self.db_file = None
        
        # Track row counts for validation
        self.row_counts = {}
        self.failed_files = []  # Initialize failed_files as an instance variable

    def setup_database(self):
        """Initialize database schema and required tables with performance configuration."""
        # Enable parallel processing
        self.conn.execute("SET threads TO ?", [CPU_COUNT])
        memory_limit_gb = int(MEMORY_GB * (self.memory_percent / 100))
        self.conn.execute(f"SET memory_limit = '{memory_limit_gb}GB'")
        
        # Create tables with optimized schema
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS options (
                symbol VARCHAR,
                date DATE,
                time TIME,
                open INTEGER,
                high INTEGER,
                low INTEGER,
                close INTEGER,
                volume INTEGER,
                oi INTEGER
            );
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_data (
                date DATE,
                time TIME,
                idx_open INTEGER,
                idx_high INTEGER, 
                idx_low INTEGER,
                idx_close INTEGER
            );
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS future_data (
                date DATE,
                time TIME,
                fut_open INTEGER,
                fut_high INTEGER, 
                fut_low INTEGER,
                fut_close INTEGER,
                fut_volume INTEGER,
                fut_oi INTEGER
            );
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS splits (
                symbol VARCHAR,
                instrument VARCHAR,
                expiry_date DATE,
                strike_price INTEGER,
                option_type VARCHAR,
                date DATE
            );
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cal_data (
                symbol VARCHAR,
                instrument VARCHAR,
                expiry_date DATE,
                date DATE,
                time TIME,
                expiry_day INTEGER,
                strike_price INTEGER,
                option_type VARCHAR,
                spot INTEGER,
                strdifference INTEGER,
                dte INTEGER,
                open INTEGER,
                high INTEGER,
                low INTEGER,
                close INTEGER,
                volume INTEGER,
                oi INTEGER,
                moneyness VARCHAR,
                idx_open INTEGER,
                idx_high INTEGER,
                idx_low INTEGER,
                idx_close INTEGER,
                fut_open INTEGER,
                fut_high INTEGER,
                fut_low INTEGER,
                fut_close INTEGER,
                fut_volume INTEGER,
                fut_oi INTEGER
            );
        """)
        
        # Create indices for faster joins
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_option_datetime ON options(date, time);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_option_symbol ON options(symbol);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_index_datetime ON index_data(date, time);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_future_datetime ON future_data(date, time);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_split_symbol ON splits(symbol);")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_cal_datetime ON cal_data(date, time);")

    def validate_data(self, table_name, expected_min_rows=1):
        """Validate that a table has the expected structure and minimum row count."""
        if not self.validate:
            return True
            
        try:
            # Check if table exists
            table_exists = self.conn.execute(f"SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = '{table_name}')").fetchone()[0]
            if not table_exists:
                logger.error(f"Validation error: Table {table_name} does not exist")
                return False
                
            # Check row count
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.row_counts[table_name] = row_count
            
            if row_count < expected_min_rows:
                logger.error(f"Validation error: Table {table_name} has {row_count} rows, expected at least {expected_min_rows}")
                return False
                
            logger.info(f"Validation passed: Table {table_name} has {row_count:,} rows")
            return True
            
        except Exception as e:
            logger.error(f"Validation error for {table_name}: {str(e)}")
            return False

    def process_file(self, input_file, output_file):
        """Process a single file through the entire pipeline."""
        try:
            self.current_file = input_file
            self.current_output = output_file
            
            # Create a database file based on input file
            self.db_file = input_file.replace('.parquet', '.duckdb')
            self.conn = duckdb.connect(self.db_file)
            
            # Initialize database
            self.setup_database()
            
            # Process data
            logger.info(f"Processing file: {os.path.basename(input_file)}")
            success = (self.create_option_data() and 
                      self.create_index_data() and 
                      self.create_future_data() and
                      self.create_split_data() and
                      self.create_cal_data() and
                      self.filter_and_export())
                      
            return success
            
        except Exception as e:
            logger.error(f"Error processing file {input_file}: {str(e)}")
            logger.error(traceback.format_exc())
            self.failed_files.append((os.path.basename(input_file), str(e)))  # Use instance variable
            return False
        finally:
            self.cleanup()

    @time_tracker
    def create_option_data(self):
        """Create the options table with CE/PE data from input parquet file."""
        logger.info("Step 1: Creating option data...")
        
        try:
            # Filter trading hours at load time for better performance
            self.conn.execute("""
                INSERT INTO options
                SELECT 
                    symbol,
                    CAST(strptime(date, '%d-%m-%Y') AS DATE) as date,
                    CAST(time AS TIME) as time,
                    CAST(ROUND(open) AS INTEGER) as open,
                    CAST(ROUND(high) AS INTEGER) as high,
                    CAST(ROUND(low) AS INTEGER) as low,
                    CAST(ROUND(close) AS INTEGER) as close,
                    CAST(ROUND(volume) AS INTEGER) as volume,
                    CAST(ROUND(oi) AS INTEGER) as oi
                FROM read_parquet(?)
                WHERE (symbol LIKE '%CE%' OR symbol LIKE '%PE%')
                AND time >= '09:15:00' AND time <= '15:29:00'
            """, [self.current_file])
            
            result = self.conn.execute("SELECT COUNT(*) FROM options").fetchone()[0]
            logger.info(f"Created option data with {result:,} records")
            
            # Validate data
            return self.validate_data("options", expected_min_rows=100)
            
        except Exception as e:
            logger.error(f"Error creating options data: {str(e)}")
            return False

    @time_tracker
    def create_index_data(self):
        """Create the index data table with NIFTY data."""
        logger.info("Step 2: Creating index data...")
        
        if not self.validate_data("options"):
            logger.error("Cannot proceed: Option data validation failed")
            return False
            
        try:
            # Filter trading hours at load time and rename columns
            self.conn.execute("""
                INSERT INTO index_data
                SELECT 
                    CAST(strptime(date, '%d-%m-%Y') AS DATE) as date,
                    CAST(time AS TIME) as time,
                    CAST(ROUND(open) AS INTEGER) as idx_open,
                    CAST(ROUND(high) AS INTEGER) as idx_high,
                    CAST(ROUND(low) AS INTEGER) as idx_low,
                    CAST(ROUND(close) AS INTEGER) as idx_close
                FROM read_parquet(?)
                WHERE symbol = 'NIFTY'
                AND time >= '09:15:00' AND time <= '15:29:00'
            """, [self.current_file])
            
            result = self.conn.execute("SELECT COUNT(*) FROM index_data").fetchone()[0]
            logger.info(f"Created index data with {result:,} records")
            
            return self.validate_data("index_data", expected_min_rows=10)
            
        except Exception as e:
            logger.error(f"Error creating index data: {str(e)}")
            return False

    @time_tracker
    def create_future_data(self):
        """Create the future data table with NIFTY-I data."""
        logger.info("Step 3: Creating future data...")
        
        if not self.validate_data("options") or not self.validate_data("index_data"):
            logger.error("Cannot proceed: Previous data validation failed")
            return False
            
        try:
            # Filter trading hours at load time and rename columns
            self.conn.execute("""
                INSERT INTO future_data
                SELECT 
                    CAST(strptime(date, '%d-%m-%Y') AS DATE) as date,
                    CAST(time AS TIME) as time,
                    CAST(ROUND(open) AS INTEGER) as fut_open,
                    CAST(ROUND(high) AS INTEGER) as fut_high,
                    CAST(ROUND(low) AS INTEGER) as fut_low,
                    CAST(ROUND(close) AS INTEGER) as fut_close,
                    CAST(ROUND(volume) AS INTEGER) as fut_volume,
                    CAST(ROUND(oi) AS INTEGER) as fut_oi
                FROM read_parquet(?)
                WHERE symbol = 'NIFTY-I'
                AND time >= '09:15:00' AND time <= '15:29:00'
            """, [self.current_file])
            
            result = self.conn.execute("SELECT COUNT(*) FROM future_data").fetchone()[0]
            logger.info(f"Created future data with {result:,} records")
            
            return self.validate_data("future_data", expected_min_rows=10)
            
        except Exception as e:
            logger.error(f"Error creating future data: {str(e)}")
            return False

    @time_tracker
    def create_split_data(self):
        """Create the split data by parsing option symbols."""
        logger.info("Step 4: Creating split data...")
        
        if not self.validate_data("options"):
            logger.error("Cannot proceed: Option data validation failed")
            return False
            
        try:
            self.conn.execute("""
                INSERT INTO splits
                WITH parsed_symbols AS (
                    SELECT DISTINCT
                        symbol,
                        regexp_extract(symbol, '([A-Z]+)(\\d{2})([A-Z]{3})(\\d{2})(\\d+)(PE|CE)', 1) as instrument,
                        regexp_extract(symbol, '([A-Z]+)(\\d{2})([A-Z]{3})(\\d{2})(\\d+)(PE|CE)', 2) as day,
                        regexp_extract(symbol, '([A-Z]+)(\\d{2})([A-Z]{3})(\\d{2})(\\d+)(PE|CE)', 3) as month,
                        regexp_extract(symbol, '([A-Z]+)(\\d{2})([A-Z]{3})(\\d{2})(\\d+)(PE|CE)', 4) as year,
                        CAST(regexp_extract(symbol, '([A-Z]+)(\\d{2})([A-Z]{3})(\\d{2})(\\d+)(PE|CE)', 5) AS INTEGER) as strike_price,
                        regexp_extract(symbol, '([A-Z]+)(\\d{2})([A-Z]{3})(\\d{2})(\\d+)(PE|CE)', 6) as option_type,
                        date  -- Use date directly
                    FROM options
                    WHERE symbol LIKE 'NIFTY%'
                )
                SELECT 
                    symbol,
                    instrument,
                    make_date(2000 + CAST(year AS INTEGER), 
                             CASE month 
                                 WHEN 'JAN' THEN 1 WHEN 'FEB' THEN 2 WHEN 'MAR' THEN 3
                                 WHEN 'APR' THEN 4 WHEN 'MAY' THEN 5 WHEN 'JUN' THEN 6
                                 WHEN 'JUL' THEN 7 WHEN 'AUG' THEN 8 WHEN 'SEP' THEN 9
                                 WHEN 'OCT' THEN 10 WHEN 'NOV' THEN 11 WHEN 'DEC' THEN 12
                             END,
                             CAST(day AS INTEGER)) as expiry_date,
                    strike_price,
                    option_type,
                    date
                FROM parsed_symbols
                WHERE instrument IS NOT NULL 
                AND day IS NOT NULL 
                AND month IS NOT NULL 
                AND year IS NOT NULL
                AND strike_price IS NOT NULL
                AND option_type IS NOT NULL
            """)
            
            result = self.conn.execute("SELECT COUNT(*) FROM splits").fetchone()[0]
            logger.info(f"Created split data with {result:,} records")
            
            # Check for parsing errors
            failed_count = self.conn.execute("""
                SELECT COUNT(*) FROM options o
                LEFT JOIN splits s ON o.symbol = s.symbol
                WHERE o.symbol LIKE 'NIFTY%' AND s.symbol IS NULL
            """).fetchone()[0]
            
            if failed_count > 0:
                logger.warning(f"Warning: {failed_count} NIFTY option symbols could not be parsed")
                
                if self.validate:
                    # Show sample of failed symbols
                    failed_symbols = self.conn.execute("""
                        SELECT DISTINCT o.symbol FROM options o
                        LEFT JOIN splits s ON o.symbol = s.symbol
                        WHERE o.symbol LIKE 'NIFTY%' AND s.symbol IS NULL
                        LIMIT 5
                    """).fetchall()
                    failed_list = [s[0] for s in failed_symbols]
                    logger.warning(f"Sample failed symbols: {failed_list}")
            
            return self.validate_data("splits", expected_min_rows=10)
            
        except Exception as e:
            logger.error(f"Error creating split data: {str(e)}")
            return False

    @time_tracker
    def create_cal_data(self):
        """Create calibration data by combining option, split, index, and future data."""
        logger.info("Step 5: Creating calibration data...")
        
        if not (self.validate_data("options") and self.validate_data("splits") and 
                self.validate_data("index_data") and self.validate_data("future_data")):
            logger.error("Cannot proceed: Previous data validation failed")
            return False
            
        try:
            # Use temp tables for chunked processing if data is large
            use_chunks = self.row_counts.get("options", 0) > CHUNK_SIZE
            
            logger.info("Step 5.1: Populating cal_data with option data...")
            self.conn.execute("""
                INSERT INTO cal_data (symbol, date, time, open, high, low, close, volume, oi)
                SELECT symbol, date, time, open, high, low, close, volume, oi
                FROM options
            """)
            
            logger.info("Step 5.2: Updating with data from splits table...")
            self.conn.execute("""
                UPDATE cal_data
                SET instrument = s.instrument,
                    expiry_date = s.expiry_date,
                    strike_price = s.strike_price,
                    option_type = s.option_type
                FROM splits s
                WHERE cal_data.symbol = s.symbol
            """)
            
            logger.info("Step 5.3: Updating with index data...")
            self.conn.execute("""
                UPDATE cal_data
                SET idx_open = i.idx_open,
                    idx_high = i.idx_high,
                    idx_low = i.idx_low,
                    idx_close = i.idx_close,
                    spot = i.idx_close
                FROM index_data i
                WHERE cal_data.date = i.date AND cal_data.time = i.time
            """)
            
            logger.info("Step 5.4: Updating with future data...")
            self.conn.execute("""
                UPDATE cal_data
                SET fut_open = f.fut_open,
                    fut_high = f.fut_high,
                    fut_low = f.fut_low,
                    fut_close = f.fut_close,
                    fut_volume = f.fut_volume,
                    fut_oi = f.fut_oi
                FROM future_data f
                WHERE cal_data.date = f.date AND cal_data.time = f.time
            """)
            
            logger.info("Step 5.5: Calculating expiry_day, strdifference, and dte...")
            self.conn.execute("""
                UPDATE cal_data
                SET expiry_day = extract('dow' from expiry_date),
                    strdifference = strike_price - spot,
                    dte = date_diff('day', date, expiry_date)
            """)
            
            logger.info("Step 5.6: Calculating moneyness...")
            # Note: keeping the moneyness calculation exactly as is per requirements
            self.conn.execute("""
                UPDATE cal_data
                SET moneyness = CASE
                    WHEN option_type = 'CE' THEN
                        CASE
                            WHEN strdifference <= -501 THEN 'DEEPITM'
                            WHEN strdifference BETWEEN -500 AND -451 THEN 'ATM-10'
                            WHEN strdifference BETWEEN -450 AND -401 THEN 'ATM-9'
                            WHEN strdifference BETWEEN -400 AND -351 THEN 'ATM-8'
                            WHEN strdifference BETWEEN -350 AND -301 THEN 'ATM-7'
                            WHEN strdifference BETWEEN -300 AND -251 THEN 'ATM-6'
                            WHEN strdifference BETWEEN -250 AND -201 THEN 'ATM-5'
                            WHEN strdifference BETWEEN -200 AND -151 THEN 'ATM-4'
                            WHEN strdifference BETWEEN -150 AND -101 THEN 'ATM-3'
                            WHEN strdifference BETWEEN -100 AND -51 THEN 'ATM-2'
                            WHEN strdifference BETWEEN -50 AND 0 THEN 'ATM-1'
                            WHEN strdifference BETWEEN 0 AND 50 THEN 'ATM'
                            WHEN strdifference BETWEEN 51 AND 100 THEN 'ATM+1'
                            WHEN strdifference BETWEEN 101 AND 150 THEN 'ATM+2'
                            WHEN strdifference BETWEEN 151 AND 200 THEN 'ATM+3'
                            WHEN strdifference BETWEEN 201 AND 250 THEN 'ATM+4'
                            WHEN strdifference BETWEEN 251 AND 300 THEN 'ATM+5'
                            WHEN strdifference BETWEEN 301 AND 350 THEN 'ATM+6'
                            WHEN strdifference BETWEEN 351 AND 400 THEN 'ATM+7'
                            WHEN strdifference BETWEEN 401 AND 450 THEN 'ATM+8'
                            WHEN strdifference BETWEEN 451 AND 500 THEN 'ATM+9'
                            WHEN strdifference BETWEEN 501 AND 550 THEN 'ATM+10'
                            WHEN strdifference >= 551 THEN 'DEEPOTM'
                        END
                    WHEN option_type = 'PE' THEN
                        CASE
                            WHEN strdifference <= -501 THEN 'DEEPOTM'
                            WHEN strdifference BETWEEN -500 AND -451 THEN 'ATM+10'
                            WHEN strdifference BETWEEN -450 AND -401 THEN 'ATM+9'
                            WHEN strdifference BETWEEN -400 AND -351 THEN 'ATM+8'
                            WHEN strdifference BETWEEN -350 AND -301 THEN 'ATM+7'
                            WHEN strdifference BETWEEN -300 AND -251 THEN 'ATM+6'
                            WHEN strdifference BETWEEN -250 AND -201 THEN 'ATM+5'
                            WHEN strdifference BETWEEN -200 AND -151 THEN 'ATM+4'
                            WHEN strdifference BETWEEN -150 AND -101 THEN 'ATM+3'
                            WHEN strdifference BETWEEN -100 AND -51 THEN 'ATM+2'
                            WHEN strdifference BETWEEN -50 AND 0 THEN 'ATM+1'
                            WHEN strdifference BETWEEN 0 AND 50 THEN 'ATM'
                            WHEN strdifference BETWEEN 51 AND 100 THEN 'ATM-1'
                            WHEN strdifference BETWEEN 101 AND 150 THEN 'ATM-2'
                            WHEN strdifference BETWEEN 151 AND 200 THEN 'ATM-3'
                            WHEN strdifference BETWEEN 201 AND 250 THEN 'ATM-4'
                            WHEN strdifference BETWEEN 251 AND 300 THEN 'ATM-5'
                            WHEN strdifference BETWEEN 301 AND 350 THEN 'ATM-6'
                            WHEN strdifference BETWEEN 351 AND 400 THEN 'ATM-7'
                            WHEN strdifference BETWEEN 401 AND 450 THEN 'ATM-8'
                            WHEN strdifference BETWEEN 451 AND 500 THEN 'ATM-9'
                            WHEN strdifference BETWEEN 501 AND 550 THEN 'ATM-10'
                            WHEN strdifference >= 551 THEN 'DEEPITM'
                        END
                END
            """)
            
            # Check for null values in key columns
            null_spots = self.conn.execute("SELECT COUNT(*) FROM cal_data WHERE spot IS NULL").fetchone()[0]
            if null_spots > 0:
                logger.warning(f"Warning: {null_spots} records have NULL spot values")
                
            null_strikes = self.conn.execute("SELECT COUNT(*) FROM cal_data WHERE strike_price IS NULL").fetchone()[0]
            if null_strikes > 0:
                logger.warning(f"Warning: {null_strikes} records have NULL strike_price values")
            
            return self.validate_data("cal_data", expected_min_rows=100)
            
        except Exception as e:
            logger.error(f"Error creating calibration data: {str(e)}")
            return False

    @time_tracker
    def filter_and_export(self):
        """Step 6 & 7: Filter by moneyness and time range, then export to parquet."""
        logger.info("Step 6 & 7: Filtering and exporting data...")
        
        if not self.validate_data("cal_data"):
            logger.error("Cannot proceed: Calibration data validation failed")
            return False
            
        try:
            # Count records before filtering
            total_before = self.conn.execute("SELECT COUNT(*) FROM cal_data").fetchone()[0]
            logger.info(f"Total records before filtering: {total_before:,}")
            
            # Step 6: Remove DEEPITM and DEEPOTM records
            logger.info("Filtering out DEEPITM and DEEPOTM records...")
            self.conn.execute("""
                DELETE FROM cal_data 
                WHERE moneyness IN ('DEEPITM', 'DEEPOTM')
            """)
            
            # Count after moneyness filtering
            moneyness_filtered = self.conn.execute("SELECT COUNT(*) FROM cal_data").fetchone()[0]
            logger.info(f"Records after moneyness filtering: {moneyness_filtered:,}")
            logger.info(f"Removed {total_before - moneyness_filtered:,} DEEPITM/DEEPOTM records")
            
            # Step 7: Export to parquet
            logger.info(f"Exporting final data to {self.current_output}...")
            with tqdm(total=1, desc="Exporting data") as pbar:
                self.conn.execute(f"""
                    COPY (
                        SELECT * FROM cal_data 
                        ORDER BY date, time, symbol
                    ) TO '{self.current_output}' (FORMAT 'parquet')
                """)
                pbar.update(1)
                
            logger.info(f"Successfully exported {moneyness_filtered:,} records to {self.current_output}")
            return True
            
        except Exception as e:
            logger.error(f"Error during filtering and export: {str(e)}")
            return False

    def cleanup(self):
        """Close connections and optionally remove temporary files."""
        if self.conn:
            self.conn.close()
            self.conn = None
            
        gc.collect()
        
        # Remove temporary DuckDB file unless requested to keep it
        if not self.keep_temp and self.db_file and os.path.exists(self.db_file):
            try:
                os.remove(self.db_file)
                logger.info(f"Removed temporary file: {self.db_file}")
            except Exception as e:
                logger.warning(f"Could not remove temporary file {self.db_file}: {str(e)}")
            
        # Reset current file tracking
        self.current_file = None
        self.current_output = None
        self.db_file = None
        self.row_counts = {}

def main():
    """Main entry point - parse arguments and run the processing pipeline."""
    start_time = time.time()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Show configuration
    logger.info("\nStarting DuckDB data processing pipeline for multiple files...")
    logger.info("=" * 60)
    logger.info(f"System Configuration:")
    logger.info(f"CPUs: {CPU_COUNT}")
    logger.info(f"Memory: {MEMORY_GB:.1f} GB")
    logger.info(f"Memory allocation: {args.memory_percent}%")
    logger.info(f"Chunk Size: {CHUNK_SIZE:,}")
    logger.info(f"Input folder: {args.input_folder}")
    logger.info(f"File pattern: {args.file_pattern}")
    logger.info(f"Output folder: {args.output_folder}")
    logger.info(f"Keep temporary files: {args.keep_temp}")
    logger.info(f"Additional validation: {args.validate}")
    logger.info("=" * 60)
    
    processor = None
    file_count = 0
    successful_count = 0
    
    try:
        # Initialize processor
        processor = DuckDBProcessor(
            memory_percent=args.memory_percent,
            validate=args.validate,
            keep_temp=args.keep_temp
        )
        
        # Get list of files to process
        file_pattern = os.path.join(args.input_folder, args.file_pattern)
        input_files = glob.glob(file_pattern)
        
        if not input_files:
            logger.error(f"No files found matching pattern: {file_pattern}")
            return 1
            
        logger.info(f"Found {len(input_files)} files to process")
        
        # Process each file
        for input_file in input_files:
            file_count += 1
            base_name = os.path.basename(input_file)
            output_name = base_name.replace('.parquet', '_combined.parquet')
            output_file = os.path.join(args.output_folder, output_name)
            
            logger.info(f"Processing file {file_count}/{len(input_files)}: {base_name}")
            
            if processor.process_file(input_file, output_file):
                successful_count += 1
                logger.info(f"Successfully processed: {base_name}")
            else:
                reason = f"Failed to process: {base_name}"
                logger.error(reason)
            
            # Add a blank line for separation
            logger.info("")
        
        # Print summary
        end_time = time.time()
        total_execution_time = end_time - start_time
        logger.info("\nComplete Pipeline Summary")
        logger.info("=" * 60)
        logger.info(f"Total files processed: {file_count}")
        logger.info(f"Successfully processed: {successful_count}")
        logger.info(f"Failed: {file_count - successful_count}")
        logger.info(f"Total execution time: {total_execution_time:.2f} seconds ({total_execution_time/60:.2f} minutes)")

        # Log failed files in a table format
        if processor.failed_files:
            logger.info("\nFailed Files Summary")
            logger.info("=" * 60)
            logger.info("| File Name | Reason |")
            logger.info("|-----------|--------|")
            for file_name, reason in processor.failed_files:
                logger.info(f"| {file_name} | {reason} |")
            logger.info("=" * 60)

        logger.info("=" * 60)
        
        return 0  # Always return 0 to indicate success, regardless of processing results
            
    except KeyboardInterrupt:
        logger.warning("\nProcessing interrupted by user")
        return 130  # Interrupted
    except Exception as e:
        logger.error(f"\nUnhandled error: {str(e)}")
        logger.error(traceback.format_exc())
        return 1  # Error
    finally:
        # Cleanup is handled after each file
        pass

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code) 