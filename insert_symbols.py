#!/usr/bin/python
# -*- coding: utf-8 -*-
# insert_symbols.py
from __future__ import print_function
import datetime
from math import ceil
import bs4
import mysql.connector  # or import pymysql as mdb
from mysql.connector import errorcode
import requests
def obtain_parse_wiki_snp500():
    """
    Download and parse the Wikipedia list of S&P500
    constituents using requests and BeautifulSoup.
    Returns a list of tuples for to add to MySQL.
    """
    # Stores the current time, for the created_at record
    now = datetime.datetime.utcnow()
    # Use requests and BeautifulSoup to download the
    # list of S&P500 companies and obtain the symbol table
    response = requests.get(
    "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    )
    soup = bs4.BeautifulSoup(response.text, features="lxml")
    # This selects the first table, using CSS Selector syntax
    # and then ignores the header row ([1:])
    symbolslist = soup.select('table')[0].select('tr')[1:]
    # Obtain the symbol information for each
    # row in the S&P500 constituent table
    symbols = []
    for i, symbol in enumerate(symbolslist):
        tds = symbol.select('td')
        symbols.append(
            (tds[0].select('a')[0].text, # Ticker
            'stock',
            tds[1].select('a')[0].text, # Name
            tds[3].text,  # Sector
                'USD', now, now
            )
        )
    return symbols


def insert_snp500_symbols(symbols):
    """
    Insert the S&P500 symbols into the MySQL database.
    """
    # Connect to the MySQL instance
    db_host = 'localhost'
    db_user = 'root'
    db_pass = ''
    db_name = 'securities_master'

    try:
        con = mysql.connector.connect(
            host=db_host, user=db_user, password=db_pass, database=db_name
        )

        # Create the insert strings
        column_str = """ticker, instrument, name, sector,
        currency, created_date, last_updated_date
        """
        insert_str = ("%s, " * 7)[:-2]
        final_str = f"INSERT INTO symbol ({column_str}) VALUES ({insert_str})"

        # Using the MySQL connection, carry out
        # an INSERT INTO for every symbol
        with con:
            cur = con.cursor()
            cur.executemany(final_str, symbols)
            print(f"{cur.rowcount} symbols were successfully added.")
            con.commit()  # Add this line to commit changes

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error: Access denied. Check username and password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Error: Database '{db_name}' does not exist.")
        else:
            print(f"Error: {err}")
    finally:
        if con.is_connected():
            con.close()

if __name__ == "__main__":
    symbols = obtain_parse_wiki_snp500()
    print(symbols)
    insert_snp500_symbols(symbols)
