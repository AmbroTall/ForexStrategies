#!/usr/bin/python
# -*- coding: utf-8 -*-
# backtest.py
import datetime
import pprint
try:
    import Queue as queue
except ImportError:
    import queue
import time


class Backtest:
    """
    Encapsulates the settings and components for carrying out
    an event-driven backtest.
    """

    def __init__(self, csv_dir, symbol_list, initial_capital,
                 heartbeat, start_date, data_handler_cls,
                 execution_handler_cls, portfolio_cls, strategy_cls):
        """
        Initializes the backtest.

        Parameters:
        csv_dir (str): The hard root to the CSV data directory.
        symbol_list (list): The list of symbol strings.
        initial_capital (float): The starting capital for the portfolio.
        heartbeat (int): Backtest "heartbeat" in seconds.
        start_date (datetime): The start datetime of the strategy.
        data_handler_cls (class): Handles the market data feed.
        execution_handler_cls (class): Handles the orders/fills for trades.
        portfolio_cls (class): Keeps track of portfolio current
            and prior positions.
        strategy_cls (class): Generates signals based on market data.
        """
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list
        self.initial_capital = initial_capital
        self.heartbeat = heartbeat
        self.start_date = start_date
        self.data_handler_cls = data_handler_cls
        self.execution_handler_cls = execution_handler_cls
        self.portfolio_cls = portfolio_cls
        self.strategy_cls = strategy_cls
        self.events = queue.Queue()
        self.signals = 0
        self.orders = 0
        self.fills = 0
        self.num_strats = 1
        self._generate_trading_instances()

    def _generate_trading_instances(self):
        """
        Instantiates the data handler, strategy,
        portfolio, and execution handler.
        """
        self.data_handler = self.data_handler_cls(self.events, self.csv_dir, self.symbol_list)
        self.strategy = self.strategy_cls(self.data_handler, self.events)
        self.portfolio = self.portfolio_cls(self.data_handler, self.events,
                                            self.start_date, self.initial_capital)
        self.execution_handler = self.execution_handler_cls(self.events)

    def _run_backtest(self):
        """
        Executes the backtest.
        """
        iteration_count = 0
        while True:
            iteration_count += 1
            print(iteration_count)

            # Update the market bars
            if self.data_handler.continue_backtest:
                self.data_handler.update_bars()
            else:
                break

            # Handle the events
            while True:
                try:
                    event = self.events.get(False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            self._handle_market_event(event)
                        elif event.type == 'SIGNAL':
                            self._handle_signal_event(event)
                        elif event.type == 'ORDER':
                            self._handle_order_event(event)
                        elif event.type == 'FILL':
                            self._handle_fill_event(event)

            time.sleep(self.heartbeat)

    def _handle_market_event(self, event):
        """
        Handles MARKET event.
        """
        self.strategy.calculate_signals(event)
        self.portfolio.update_timeindex(event)

    def _handle_signal_event(self, event):
        """
        Handles SIGNAL event.
        """
        self.signals += 1
        self.portfolio.update_signal(event)

    def _handle_order_event(self, event):
        """
        Handles ORDER event.
        """
        self.orders += 1
        self.execution_handler.execute_order(event)

    def _handle_fill_event(self, event):
        """
        Handles FILL event.
        """
        self.fills += 1
        self.portfolio.update_fill(event)

    def _output_performance(self):
        """
        Outputs the strategy performance from the backtest.
        """
        self._create_equity_curve_dataframe()
        print("Creating summary stats...")
        stats = self._output_summary_stats()
        print("Creating equity curve...")
        print(self.portfolio.equity_curve.tail(10))
        pprint.pprint(stats)
        print("Signals: %s" % self.signals)
        print("Orders: %s" % self.orders)
        print("Fills: %s" % self.fills)

    def _create_equity_curve_dataframe(self):
        """
        Creates equity curve dataframe.
        """
        self.portfolio.create_equity_curve_dataframe()

    def _output_summary_stats(self):
        """
        Outputs summary statistics.
        """
        return self.portfolio.output_summary_stats()

    def simulate_trading(self):
        """
        Simulates the backtest and outputs portfolio performance.
        """
        self._run_backtest()
        self._output_performance()

