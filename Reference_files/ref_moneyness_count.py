import pandas as pd

# File Configuration
INPUT_FILE = 'nifty_data_2023-12_com.parquet'

class MoneynessCounter:
    def __init__(self):
        self.df = pd.read_parquet(INPUT_FILE)

    def count_moneyness(self):
        """Count the number of rows for each moneyness category."""
        result = self.df['moneyness'].value_counts().items()
        
        for moneyness, count in result:
            print(f"{moneyness}: {count} rows")


def main():
    counter = MoneynessCounter()
    counter.count_moneyness()


if __name__ == "__main__":
    main() 