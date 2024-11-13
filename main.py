from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

import pandas as pd
import yfinance as yf
import os
import logging
import time

def config_logs(log_filename=None, log_directory='logs', log_level=logging.INFO, log_format=None):
    """
    Configures logging with console and file logging, including timed rotation.

    :param log_filename: Name of the log file (defaults to "log_<date>.log").
    :param log_level: Logging level (default is logging.DEBUG).
    :param log_format: Custom log format (default is a basic format with timestamp and message).
    """
    # Set up the path for the log file in the assets folder
    log_filename = f"log_{datetime.now().strftime('%Y-%m-%d')}.log"


    if log_filename is None:
        log_filename = f"log_{datetime.now().strftime('%Y-%m-%d')}.log"
    log_path = os.path.join(log_directory, log_filename)
    os.makedirs(log_directory, exist_ok=True)

    # Set the log format
    if log_format is None:
        log_format = '%(asctime)s - %(levelname)s - %(funcName)s - %(message)s'
    formatter = logging.Formatter(log_format, datefmt='%H:%M')


    # Create a TimedRotatingFileHandler to rotate logs every midnight, keep 7 backups
    file_handler = TimedRotatingFileHandler(log_path, when="midnight", interval=1, backupCount=7)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger("my_logger")
    logger.setLevel(log_level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("Logging setup completed.")

    return logger

def fetch_symbols(exchange_name, url):
    """
    Fetches a list of ticker symbols from a given URL.

    :param exchange_name: Name of the stock exchange (used for logging)
    :param url: URL containing a table with stock symbols
    :return: List of stock symbols
    """
    try:
        tables = pd.read_html(url)

        # Find the table with a 'Symbol' column
        for table in tables:
            columns = table.columns.astype(str)
            if 'Symbol' in columns:
                symbols = table['Symbol'].tolist()
                symbols = [symbol.strip() for symbol in symbols]
                logger.info(f"Fetched {len(symbols)} symbols from exchange {exchange_name}.")
                return symbols

        raise ValueError("No 'Symbol' column found in any table at the URL.")

    except Exception as e:
        logger.error(f"Error fetching symbols from {url}: {e}")
        return []

def check_data_exists(symbol, year):
    """
    Checks if data for the given symbol and year already exists.

    :param symbol: The stock symbol (e.g., 'AAPL').
    :param year: The year of data to check for (e.g., '2022').
    :return: True if data exists, False otherwise.
    """
    # Define the data file path based on symbol and year
    data_file_path = f"data/{year}/{symbol}_{year}.csv"

    # Check if the file exists
    if os.path.exists(data_file_path):
        logger.info(f"Data for {symbol} in {year} already exists. Skipping download.")
        return True
    else:
        return False

def get_data(symbols, year, delay=12):
    """
    Fetches historical data for multiple symbols for a specified year,
    saving each symbol's data as a separate CSV file.
    Includes a delay between API calls to prevent rate limits.
    """
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    bulk_data = {}

    for symbol in symbols:
        if check_data_exists(symbol, year):
            continue
        data = None
        try:
            data = yf.download(symbol, start=start_date, end=end_date)

        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {e}")
        logger.info(f"Fetched {symbol} for the year {year}")

        if data is not None and not data.empty:
            #logging.debug(f"Data Columns before droplevel {data.columns} for symbol {symbol}")
            data.columns = data.columns.droplevel('Ticker')
            #logging.debug(f"Data Columns after droplevel {data.columns} for symbol {symbol}")

            bulk_data[symbol] = data
            save_as_csv(data, symbol, year)
        time.sleep(delay)

    logger.info(f"Success running get_data(symbols, {year}, delay={delay}) ")
    return bulk_data

def save_as_csv(raw_data, symbol, year):
    """
    Saves the historical data for a specific symbol and year to a CSV file.
    """
    folder_path = f"data/{year}"
    os.makedirs(folder_path, exist_ok=True)
    file_path = f"{folder_path}/{symbol}_{year}.csv"
    raw_data.to_csv(file_path)
    logger.info(f"Data for {symbol} saved to {file_path}")

#def save_bulk_as_csv(symbols, symbol, year):

def query_single_ticker_history(ticker, year):
    """
    Fetches the historical price data for a single ticker using yf.download.

    :param ticker: The stock symbol for which to fetch data
    :return: Prints the historical data for the ticker
    """
    try:
        start_date, end_date = f"{year}-01-01", f"{year}-12-31"
        data = yf.download(ticker, start=start_date, end=end_date, interval="1d")

        if data.empty:
            logger.error(f"No data found for {ticker} for the year {year}.")
            return

        # Print the historical data
        logger.info(f"Raw historical data for {ticker}:")
        logger.info(f"Columns: {data.columns}")
        #logger.info(f"Data Types: {data.dtypes}")
        #logger.info(f"First few rows:\n{data.head()}")

        # Flatten the columns from MultiIndex to simple columns
        data.columns = data.columns.droplevel('Ticker')

        logger.info(f"Columns: {data.columns}")
        logger.info(f"Data Types: {data.dtypes}")
        logger.info(f"First few rows:\n{data.head()}")

        file_path = f"data/{ticker}_{year}.csv"
        data.to_csv(file_path, index=True)
        logger.info(f"Saved {ticker} data to {file_path}")

    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")

# Main function to execute the script
if __name__ == "__main__":
    logger = config_logs()
    # VARIABLES
    url_sp500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies" # SP500
    url_dj30 = "https://stockanalysis.com/list/dow-jones-stocks/" # URL puede que no permita ser leido error 403
    year = 2018
    exchange_name="sp500"

    # Step 1: Fetch the symbols
    symbols = fetch_symbols(exchange_name, url_sp500)

    #sp500_symbols = [...]
    #delisted_symbols = [ 'ABNB', 'AMTM', 'BRK.B',...]
    #listed_symbols = list(set(sp500_symbols) - set(delisted_symbols))
    #listed = ['ADP', 'JNPR', 'DAY', 'MMC', 'GOOGL', 'JKHY', 'MA', 'IRM', 'HOLX', 'FFIV', 'BAC', 'PEP', 'XOM', 'COP', 'IPG', 'PSA', 'SCHW', 'AVY', 'ECL', 'AEE', 'MCO', 'ABBV', 'JPM', 'OMC', 'AON', 'AMP', 'IQV', 'NTAP', 'CDW', 'ROK', 'J', 'DECK', 'FE', 'BA', 'HWM', 'ORLY', 'FAST', 'L', 'UNP', 'MOH', 'EG', 'LULU', 'MTB', 'TDY', 'POOL', 'VMC', 'PAYC', 'RTX', 'HSY', 'LOW', 'CTAS', 'DTE', 'BEN', 'ROL', 'TSLA', 'OXY', 'MDT', 'TROW', 'AFL', 'AXP', 'ADI', 'CL', 'CZR', 'ALL', 'CCI', 'SHW', 'TPR', 'NDSN', 'GOOG', 'LMT', 'DVN', 'CTSH', 'BAX', 'BLDR', 'LHX', 'CHD', 'GPC', 'HD', 'RVTY', 'ACGL', 'PAYX', 'PTC', 'CAH', 'EOG', 'PGR', 'INCY', 'WTW', 'IEX', 'CDNS', 'CMG', 'STLD', 'CPT', 'PRU', 'ETR', 'PNW', 'KMB', 'CBRE', 'CMI', 'MRO', 'WMB', 'SMCI', 'NWSA', 'DOV', 'COST', 'VICI', 'PNR', 'MPC', 'HUBB', 'ESS', 'FTNT', 'IP', 'MCD', 'WST', 'GNRC', 'RL', 'ADSK', 'AMGN', 'NCLH', 'IR', 'MSI', 'MCHP', 'META', 'LDOS', 'TAP', 'KR', 'CPAY', 'KHC', 'WM', 'JBHT', 'ULTA', 'SO', 'C', 'TMUS', 'ANET', 'EQIX', 'KIM', 'ON', 'PSX', 'AKAM', 'TGT', 'CCL', 'EFX', 'MNST', 'AMT', 'HAL', 'URI', 'CPRT', 'GDDY', 'GS', 'EBAY', 'CTRA', 'DHI', 'AMCR', 'BKR', 'CVX', 'KKR', 'FITB', 'TTWO', 'VZ', 'A', 'AJG', 'F', 'WELL', 'HUM', 'AZO', 'KEY', 'FTV', 'LKQ', 'DG', 'IFF', 'UAL', 'XEL', 'HCA', 'VRSK', 'XYL', 'AVB', 'BALL', 'MU', 'TER', 'FDX', 'MAR', 'ED', 'HPQ', 'NOC', 'SLB', 'AMZN', 'TXN', 'COF', 'PYPL', 'ROP', 'CNC', 'T', 'AVGO', 'DELL', 'MO', 'REG', 'AIG', 'KEYS', 'EA', 'ENPH', 'HIG', 'MSFT', 'NDAQ', 'REGN', 'DE', 'EMN', 'HPE', 'COO', 'NUE', 'MMM', 'EIX', 'ORCL', 'SBUX', 'BG', 'INTC', 'FRT', 'MPWR', 'BX', 'MHK', 'AOS', 'FIS', 'UPS', 'NVDA', 'MAA', 'PM', 'CINF', 'CHRW', 'WAB', 'LRCX', 'VST', 'WBD', 'ODFL', 'TXT', 'DIS', 'STX', 'STE', 'ALB', 'OKE', 'CSX', 'TJX', 'NVR', 'VTR', 'SPG', 'CME', 'ALGN', 'DXCM', 'HLT', 'GE', 'BWA', 'ACN', 'NI', 'APH', 'VRSN', 'PPL', 'HAS', 'PCAR', 'ALLE', 'CAG', 'NXPI', 'PEG', 'DLTR', 'PLD', 'SYY', 'TSCO', 'SBAC', 'LLY', 'EW', 'WFC', 'MLM', 'BMY', 'FDS', 'PANW', 'FCX', 'NKE', 'CPB', 'FANG', 'KMI', 'CLX', 'GILD', 'QRVO', 'IDXX', 'MRNA', 'JCI', 'EQR', 'HON', 'CVS', 'QCOM', 'AIZ', 'ATO', 'BIIB', 'NSC', 'IBM', 'ZBH', 'MAS', 'MTCH', 'SWKS', 'GL', 'LYV', 'MCK', 'MOS', 'KLAC', 'LYB', 'NEM', 'STZ', 'DD', 'CRM', 'UHS', 'V', 'LUV', 'PNC', 'CI', 'GPN', 'MGM', 'VLO', 'TECH', 'NTRS', 'KDP', 'NEE', 'ADBE', 'FI', 'WYNN', 'DRI', 'ELV', 'AMD', 'GM', 'MET', 'CHTR', 'SPGI', 'HII', 'PARA', 'HST', 'DFS', 'COR', 'CMS', 'HRL', 'BR', 'GRMN', 'LNT', 'ISRG', 'TSN', 'ANSS', 'JNJ', 'KO', 'NRG', 'LH', 'MRK', 'PODD', 'O', 'SNPS', 'TT', 'WEC', 'ICE', 'RF', 'NOW', 'RCL', 'ROST', 'DAL', 'TEL', 'EVRG', 'D', 'ARE', 'AXON', 'CE', 'IT', 'SWK', 'BXP', 'HBAN', 'JBL', 'GWW', 'VTRS', 'TRGP', 'RJF', 'ZBRA', 'INVH', 'TFX', 'SYF', 'CAT', 'INTU', 'GEN', 'MDLZ', 'UNH', 'NWS', 'K', 'TRMB', 'ABT', 'EMR', 'PG', 'IVZ', 'ADM', 'EL', 'EPAM', 'AME', 'SNA', 'AMAT', 'DUK', 'FMC', 'TMO', 'KMX', 'LW', 'ES', 'EXPE', 'LVS', 'VRTX', 'CBOE', 'APA', 'CF', 'BDX', 'MKTX', 'RSG', 'TDG', 'WAT', 'APTV', 'RMD', 'DVA', 'WBA', 'MSCI', 'PH', 'AAPL', 'WMT', 'CB', 'AEP', 'PHM', 'TRV', 'EQT', 'ETN', 'MTD', 'PWR', 'PCG', 'BKNG', 'AWK', 'STT', 'WRB', 'LEN', 'MS', 'CFG', 'FICO', 'GD', 'TFC', 'CNP', 'PFE', 'BRO', 'ITW', 'EXR', 'PKG', 'HES', 'CTLT', 'SJM', 'DHR', 'UDR', 'DLR', 'GIS', 'SYK', 'MKC', 'AES', 'CMCSA', 'BSX', 'ERIE', 'ZTS', 'CRL', 'CSGP', 'APD', 'DGX', 'CSCO', 'BK', 'NFLX', 'DOC', 'EXPD', 'DPZ', 'BBY', 'WDC', 'LIN', 'WY', 'PFG', 'SRE', 'HSIC', 'EXC', 'TYL', 'YUM', 'FSLR', 'USB', 'GLW', 'BLK', 'PPG']

    # Step 2: Get the data for the specified year
    #example_data = query_single_ticker_history("MSFT",2018)
    try:
        bulk_data=get_data(symbols, year)
        logger.info(f"Data has been saved to CSV files for each symbol in {exchange_name}.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

    # Step 3: Save the data as CSV files
    # save_as_csv(raw_data) is called for each symbol in get_data()