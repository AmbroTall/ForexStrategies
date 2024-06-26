#!/usr/bin/python
# -*- coding: utf-8 -*-
# retrieving_data.py
from __future__ import print_function
import pandas as pd
import MySQLdb as mdb

if __name__ == "__main__":
    # Connect to the MySQL instance
    db_host = 'localhost'
    db_user = 'root'
    db_pass = ''
    db_name = 'securities_master'

    try:
        con = mdb.connect(host=db_host, user=db_user, password=db_pass, database=db_name)
    except mdb.Error as e:
        print(f"Error: {e}")
    # Select all of the historic Google adjusted close data
    sql = """SELECT dp.price_date, dp.adj_close_price
    FROM symbol AS sym
    INNER JOIN daily_price AS dp
    ON dp.symbol_id = sym.id
    WHERE sym.ticker = 'GOOG'
    ORDER BY dp.price_date ASC;"""
    # Create a pandas dataframe from the SQL query
    goog = pd.read_sql_query(sql, con=con, index_col='price_date')
    print(goog.tail())
    # Output the dataframe tail