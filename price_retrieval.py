from __future__ import print_function
import datetime
import warnings
import mysql.connector as mdb
from yahoo_fin import stock_info as si

def connect_to_database():
    """
    Establishes a connection to the MySQL database and returns the connection object.
    """
    db_host = 'localhost'
    db_user = 'root'
    db_pass = ''
    db_name = 'securities_master'

    try:
        con = mdb.connect(host=db_host, user=db_user, password=db_pass, database=db_name)
        return con
    except mdb.Error as e:
        print(f"Error: {e}")
        return None

def close_database_connection(con):
    """
    Closes the MySQL database connection.
    """
    if con and con.is_connected():
        con.close()

def obtain_list_of_db_tickers(con):
    """
    Obtains a list of the ticker symbols in the database.
    """
    with con.cursor() as cur:
        cur.execute("SELECT id, ticker FROM symbol")
        data = cur.fetchall()
        return [(d[0], d[1]) for d in data]

def get_daily_historic_data_yahoo(ticker, start_date=(2000, 1, 1), end_date=datetime.date.today().timetuple()[0:3]):
    """
    Obtains data from Yahoo Finance using yahoo_fin and returns a list of tuples.
    ticker: Yahoo Finance ticker symbol, e.g., "GOOG" for Google, Inc.
    start_date: Start date in (YYYY, M, D) format
    end_date: End date in (YYYY, M, D) format
    """
    start_date_str = f"{start_date[1]}-{start_date[2]}-{start_date[0]}"
    end_date_str = f"{end_date[1]}-{end_date[2]}-{end_date[0]}"

    try:
        data = si.get_data(ticker, start_date=start_date_str, end_date=end_date_str)
        prices = [
            (
                datetime.datetime.strptime(str(index.date()), '%Y-%m-%d'),  # Extract date and convert to string
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume'],
                row['adjclose']
            )
            for index, row in data.iterrows()
        ]
        return prices
    except Exception as e:
        print(f"Could not download Yahoo Finance data for {ticker}: {e}")
        return []

def insert_daily_data_into_db(con, data_vendor_id, symbol_id, daily_data):
    """
    Takes a list of tuples of daily data and adds it to the MySQL database.
    Appends the vendor ID and symbol ID to the data.
    daily_data: List of tuples of the OHLC data (with adj_close and volume)
    """
    # Create the time now
    now = datetime.datetime.utcnow()
    # Amend the data to include the vendor ID and symbol ID
    daily_data = [
        (data_vendor_id, symbol_id, d[0], now, now,
         d[1], d[2], d[3], d[4], d[5], d[6])
        for d in daily_data
    ]
    # Create the insert strings
    column_str = """data_vendor_id, symbol_id, price_date, created_date,
    last_updated_date, open_price, high_price, low_price,
    close_price, volume, adj_close_price"""

    insert_str = ("%s, " * 11)[:-2]
    final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % \
                (column_str, insert_str)
    # Using the MySQL connection, carry out an INSERT INTO for every symbol
    with con:
        cur = con.cursor()
        cur.executemany(final_str, daily_data)
        print(f"{cur.rowcount} symbols were successfully added.")
        con.commit()  # Add this line to commit changes


if __name__ == "__main__":
    # This ignores the warnings regarding Data Truncation
    # from the Yahoo precision to Decimal(19,4) datatypes
    warnings.filterwarnings('ignore')

    con = connect_to_database()
    if con:
        try:
            # Loop over the tickers and insert the daily historical
            # data into the database
            tickers = obtain_list_of_db_tickers(con)
            lentickers = len(tickers)
            for i, t in enumerate(tickers):
                con = connect_to_database()

                print("Adding data for %s: %s out of %s" % (t[1], i + 1, lentickers))

                try:
                    yf_data = get_daily_historic_data_yahoo(t[1])
                    if yf_data:
                        insert_daily_data_into_db(con, '1', t[0], yf_data)
                        print("Successfully added Yahoo Finance pricing data to DB.")
                    else:
                        print(f"No data available for {t[1]} on Yahoo Finance.")
                except Exception as e:
                    print(f"Error for {t[1]}: {e}")

        except Exception as e:
            print(f"Error during data insertion: {e}")
        finally:
            if con and con.is_connected():
                close_database_connection(con)
