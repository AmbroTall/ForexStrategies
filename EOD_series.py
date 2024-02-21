from __future__ import print_function
import datetime
import pandas_datareader as pdr

# Define the stock symbol and date range
stock_symbol = "AAPL"  # Example: Apple Inc.
start_date = datetime.datetime(2020, 1, 1)
end_date = datetime.datetime(2023, 1, 1)

if __name__ == "__main__":
    # Fetch stock data from Yahoo Finance
    stock_data = pdr.get_data_yahoo(stock_symbol, start_date, end_date)

    # Display the stock data
    print(stock_data.head())