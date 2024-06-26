#!/usr/bin/python
# -*- coding: utf-8 -*-
# mac.py
import yfinance as yf
import datetime
import os

import numpy as np
import pandas as pd
import statsmodels.api as sm
from strategy import Strategy
from event import SignalEvent
from backtest import Backtest
from data import HistoricCSVDataHandler
from execution import SimulatedExecutionHandler
from portfolio import Portfolio


class MovingAverageCrossStrategy(Strategy):
    """
    Carries out a basic Moving Average Crossover strategy with a
    short/long simple weighted moving average. Default short/long
    windows are 100/400 periods respectively.
    """

    def __init__(self, bars, events, short_window=100, long_window=400):
        """
        Initializes the Moving Average Cross Strategy.

        Parameters:
        bars (DataHandler): The DataHandler object that provides bar information.
        events (queue.Queue): The Event Queue object.
        short_window (int): The short moving average lookback.
        long_window (int): The long moving average lookback.
        """
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.short_window = short_window
        self.long_window = long_window

        # Set to True if a symbol is in the market
        self.bought = self._calculate_initial_bought()

    def _calculate_initial_bought(self):
        """
        Adds keys to the bought dictionary for all symbols
        and sets them to ’OUT’.
        """

        bought = {}
        for s in self.symbol_list:
            bought[s] = 'OUT'
        return bought

    def calculate_signals(self, event):
        """
        Generates a new set of signals based on the MAC
        SMA with the short window crossing the long window
        meaning a long entry and vice versa for a short entry.

        Parameters:
        event (MarketEvent): A MarketEvent object.
        """
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                bars = self.bars.get_latest_bars_values(symbol, "adj_close", N=self.long_window)
                bar_date = self.bars.get_latest_bar_datetime(symbol)
                if bars is not None and bars.size > 0:
                    short_sma = np.mean(bars[-self.short_window:])
                    long_sma = np.mean(bars[-self.long_window:])
                    dt = datetime.datetime.utcnow()
                    sig_dir = ""
                    if short_sma > long_sma and not self.bought[symbol]:
                        print("LONG: %s" % bar_date)
                        sig_dir = 'LONG'
                        signal = SignalEvent(1, symbol, dt, sig_dir, 1.0)
                        self.events.put(signal)
                        self.bought[symbol] = True
                    elif short_sma < long_sma and self.bought[symbol]:
                        print("SHORT: %s" % bar_date)
                        sig_dir = 'EXIT'
                        signal = SignalEvent(1, symbol, dt, sig_dir, 1.0)
                        self.events.put(signal)
                        self.bought[symbol] = False


if __name__ == "__main__":
    # csv_dir = '/path/to/your/csv/file'  # CHANGE THIS!
    symbol_list = ['AAPL']
    initial_capital = 100000.0
    heartbeat = 0.0
    start_date = datetime.datetime(1990, 1, 1, 0, 0, 0)
    end_date = datetime.datetime(2002, 1, 1, 0, 0, 0)
    current_directory = os.getcwd()
    csv_dir = os.path.join(current_directory, "intraday")
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    # Download historical stock data for each symbol in the symbol list
    for symbol in symbol_list:
        # Download data from yfinance
        stock_data = yf.download(symbol, start=start_date, end=end_date)

        # Save data to a CSV file
        csv_file_path = os.path.join(csv_dir, f"{symbol}.csv")
        stock_data.to_csv(csv_file_path)

    backtest = Backtest(
        csv_dir=csv_dir,
        symbol_list=symbol_list,
        initial_capital=initial_capital,
        heartbeat=heartbeat,
        start_date=start_date,
        data_handler_cls=HistoricCSVDataHandler,
        execution_handler_cls=SimulatedExecutionHandler,
        portfolio_cls=Portfolio,
        strategy_cls=MovingAverageCrossStrategy
    )
    backtest.simulate_trading()
