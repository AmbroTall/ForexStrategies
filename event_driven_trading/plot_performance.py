#!/usr/bin/python
# -*- coding: utf-8 -*-
# plot_performance.py

import os.path
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

if __name__ == "__main__":
    current_directory = os.getcwd()
    csv_dir = os.path.join(current_directory)
    csv_file_path = "/home/ambrose/PycharmProjects/FTTesting/ForexStrategies/event_driven_trading/equity.csv"

    data = pd.read_csv(
        csv_file_path,
        header=0,
        parse_dates=True,
        index_col=0
    ).sort_index()

    # Plot three charts: Equity curve, period returns, drawdowns
    fig = plt.figure()
    # Set the outer colour to white
    fig.patch.set_facecolor('white')

    # Plot the equity curve
    ax1 = fig.add_subplot(311, ylabel='Portfolio value, %')
    data['equity_curve'].plot(ax=ax1, color="blue", lw=2.)
    plt.grid(True)

    # Plot the returns
    ax2 = fig.add_subplot(312, ylabel='Period returns, %')
    data['returns'].plot(ax=ax2, color="black", lw=2.)
    plt.grid(True)

    # Plot the drawdowns
    ax3 = fig.add_subplot(313, ylabel='Drawdowns, %')
    data['drawdown'].plot(ax=ax3, color="red", lw=2.)
    plt.grid(True)

    # Plot the figure
    plt.show()
