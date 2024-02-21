#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# intraday_mr.py

import datetime
import os

import numpy as np
import pandas as pd
import yfinance as yf
import statsmodels.api as sm
from strategy import Strategy
from event import SignalEvent
from backtest import Backtest
from hft_data import HistoricCSVDataHandlerHFT
from hft_portfolio import PortfolioHFT
from execution import SimulatedExecutionHandler


class IntradayOLSMRStrategy(Strategy):
    """
    Uses ordinary least squares (OLS) to perform a rolling linear
    regression to determine the hedge ratio between a pair of equities.
    The z-score of the residuals time series is then calculated in a
    rolling fashion and if it exceeds an interval of thresholds
    (defaulting to [0.5, 3.0]) then a long/short signal pair are generated
    (for the high threshold) or an exit signal pair are generated (for the
    low threshold).
    """
    def __init__(self, bars, events, ols_window=100, zscore_low=0.5, zscore_high=3.0):
        """
        Initializes the stat arb strategy.

        Parameters:
        bars (DataHandler): The DataHandler object that provides bar information.
        events (queue.Queue): The Event Queue object.
        ols_window (int): The window size for the rolling linear regression.
        zscore_low (float): The lower threshold for the z-score.
        zscore_high (float): The upper threshold for the z-score.
        """
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.ols_window = ols_window
        self.zscore_low = zscore_low
        self.zscore_high = zscore_high
        self.pair = ('AREX', 'WLL')
        self.datetime = datetime.datetime.utcnow()
        self.long_market = False
        self.short_market = False

    def calculate_xy_signals(self, zscore_last):
        """
        Calculates the actual x, y signal pairings to be sent to the signal generator.

        Parameters:
        zscore_last (float): The current zscore to test against.

        Returns:
        tuple: A tuple containing the y_signal and x_signal.
        """
        y_signal = None
        x_signal = None
        p0, p1 = self.pair
        dt = self.datetime
        hr = abs(self.hedge_ratio)

        # If we’re long the market and below the negative of the high zscore threshold
        if zscore_last <= -self.zscore_high and not self.long_market:
            self.long_market = True
            y_signal = SignalEvent(1, p0, dt, 'LONG', 1.0)
            x_signal = SignalEvent(1, p1, dt, 'SHORT', hr)

        # If we’re long the market and between the absolute value of the low zscore threshold
        elif abs(zscore_last) <= self.zscore_low and self.long_market:
            self.long_market = False
            y_signal = SignalEvent(1, p0, dt, 'EXIT', 1.0)
            x_signal = SignalEvent(1, p1, dt, 'EXIT', 1.0)

        # If we’re short the market and above the high zscore threshold
        elif zscore_last >= self.zscore_high and not self.short_market:
            self.short_market = True
            y_signal = SignalEvent(1, p0, dt, 'SHORT', 1.0)
            x_signal = SignalEvent(1, p1, dt, 'LONG', hr)

        # If we’re short the market and between the absolute value of the low zscore threshold
        elif abs(zscore_last) <= self.zscore_low and self.short_market:
            self.short_market = False
            y_signal = SignalEvent(1, p0, dt, 'EXIT', 1.0)
            x_signal = SignalEvent(1, p1, dt, 'EXIT', 1.0)

        return y_signal, x_signal

    def calculate_signals_for_pairs(self):
        """
        Generates a new set of signals based on the mean reversion strategy.

        Calculates the hedge ratio between the pair of tickers.
        We use OLS for this, although we should ideally use CADF.
        """
        # Obtain the latest window of values for each component of the pair of tickers
        y = self.bars.get_latest_bars_values(self.pair[0], "close", N=self.ols_window)
        x = self.bars.get_latest_bars_values(self.pair[1], "close", N=self.ols_window)

        if y is not None and x is not None:
            # Check that all window periods are available
            if len(y) >= self.ols_window and len(x) >= self.ols_window:
                # Calculate the current hedge ratio using OLS
                self.hedge_ratio = sm.OLS(y, x).fit().params[0]

                # Calculate the current z-score of the residuals
                spread = y - self.hedge_ratio * x
                zscore_last = ((spread - spread.mean()) / spread.std())[-1]

                # Calculate signals and add to events queue
                y_signal, x_signal = self.calculate_xy_signals(zscore_last)
                if y_signal is not None and x_signal is not None:
                    self.events.put(y_signal)
                    self.events.put(x_signal)

    def calculate_signals(self, event):
        """
        Calculate the SignalEvents based on market data.

        Parameters:
        event (Event): The event object containing market data.
        """
        if event.type == 'MARKET':
            self.calculate_signals_for_pairs()


if __name__ == "__main__":
    csv_dir = 'intraday'  # CHANGE THIS!
    symbol_list = ['GOOG', 'AAPL']

    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    # Get today's date
    today = datetime.date.today()
    # Convert today's date to a string in the format required by yfinance
    end_date = today.strftime("%Y-%m-%d")
    # Loop through each symbol in the list
    for symbol in symbol_list:
        try:
            # Download stock data for the symbol
            data = yf.download(symbol, start='2022-01-01', end=end_date)

            # Save the data to a CSV file
            csv_filename = os.path.join(csv_dir, f'{symbol}.csv')
            data.to_csv(csv_filename)

            print(f'Stock data for {symbol} downloaded and saved to {csv_filename}')
        except Exception as e:
            print(f'Error downloading data for {symbol}: {e}')
    initial_capital = 100000.0
    heartbeat = 0.0
    start_date = datetime.datetime(2007, 11, 8, 10, 41, 0)

    backtest = Backtest(
        csv_dir=csv_dir,
        symbol_list=symbol_list,
        initial_capital=initial_capital,
        heartbeat=heartbeat,
        start_date=start_date,
        data_handler_cls=HistoricCSVDataHandlerHFT,
        execution_handler_cls=SimulatedExecutionHandler,
        portfolio_cls=PortfolioHFT,
        strategy_cls=IntradayOLSMRStrategy
    )
    backtest.simulate_trading()


