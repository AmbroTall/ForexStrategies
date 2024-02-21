#!/usr/bin/python
# -*- coding: utf-8 -*-
# sharpe.py

from __future__ import print_function
import datetime
import numpy as np
import pandas as pd
import yfinance as yf


def annualised_sharpe(returns, N=252):
    """
    Calculate the annualised Sharpe ratio of a returns stream
    based on a number of trading periods, N. N defaults to 252,
    which then assumes a stream of daily returns.
    The function assumes that the returns are the excess of
    those compared to a benchmark.
    """
    return np.sqrt(N) * returns.mean() / returns.std()


def equity_sharpe(ticker):
    """
    Calculates the annualised Sharpe ratio based on the daily
    returns of an equity ticker symbol listed in Yahoo Finance.
    """
    start = datetime.datetime(2000, 1, 1)
    end = datetime.datetime(2024, 2, 1)
    # Obtain the equities daily historic data for the desired time period
    # and add to a pandas DataFrame
    pdf = yf.download(ticker, start=start, end=end)
    # Use the percentage change method to easily calculate daily returns
    pdf['daily_ret'] = pdf['Close'].pct_change()
    # Assume an average annual risk-free rate over the period of 5%
    pdf['excess_daily_ret'] = pdf['daily_ret'] - 0.05 / 252
    # Return the annualised Sharpe ratio based on the excess daily returns
    return annualised_sharpe(pdf['excess_daily_ret'])

def market_neutral_sharpe(ticker, benchmark):
    """
    Calculates the annualised Sharpe ratio of a market
    neutral long/short strategy involving the long of 'ticker'
    with a corresponding short of the 'benchmark'.
    """
    start = datetime.datetime(2000, 1, 1)
    end = datetime.datetime(2024, 2, 1)
    # Get historic data for both a symbol/ticker and a benchmark ticker
    # The dates have been hardcoded, but you can modify them as you see fit!
    tick = yf.download(ticker, start=start, end=end)
    bench = yf.download(benchmark, start=start, end=end)
    # Calculate the percentage returns on each of the time series
    tick['daily_ret'] = tick['Close'].pct_change()
    bench['daily_ret'] = bench['Close'].pct_change()
    # Create a new DataFrame to store the strategy information
    # The net returns are (long - short)/2, since there is twice
    # the trading capital for this strategy
    strat = pd.DataFrame(index=tick.index)
    strat['net_ret'] = (tick['daily_ret'] - bench['daily_ret']) / 2.0
    # Return the annualised Sharpe ratio for this strategy
    return annualised_sharpe(strat['net_ret'])

# Example usage:
ticker = "AAPL"
benchmark = "SPY"
sharpe_ratio = market_neutral_sharpe(ticker, benchmark)
print("Market Neutral Sharpe Ratio:", sharpe_ratio)