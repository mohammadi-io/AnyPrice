import logging, numpy as np, requests, sys
from datetime import datetime, timedelta
from extensions import session
from getopt import getopt
from model import ExchangeRate, StockPrice
from read_data import read_exchange_rates, read_stock_prices

logging.getLogger().setLevel(logging.INFO)
MARKET_STACK_API_MAX_LIMIT = 1000
MARKET_STACK_API_ACCESS_KEY = "54f39600d905fa2a94597ef8bd994b92"
API_LAYER_API_KEY = "sVhl4tNYYQGR7339uz2OKodq91MkiTSC"
VALID_CURRENCIES = [
    'AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS', 'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT', 'BGN', 'BHD', 'BIF',
    'BMD', 'BND', 'BOB', 'BRL', 'BSD', 'BTC', 'BTN', 'BWP', 'BYN', 'BYR', 'BZD', 'CAD', 'CDF', 'CHF', 'CLF', 'CLP',
    'CNY', 'COP', 'CRC', 'CUC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP', 'DZD', 'EGP', 'ERN', 'ETB', 'EUR', 'FJD',
    'FKP', 'GBP', 'GEL', 'GGP', 'GHS', 'GIP', 'GMD', 'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG', 'HUF', 'IDR',
    'ILS', 'IMP', 'INR', 'IQD', 'IRR', 'ISK', 'JEP', 'JMD', 'JOD', 'JPY', 'KES', 'KGS', 'KHR', 'KMF', 'KPW', 'KRW',
    'KWD', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LVL', 'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK',
    'MNT', 'MOP', 'MRO', 'MUR', 'MVR', 'MWK', 'MXN', 'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR', 'NZD', 'OMR',
    'PAB', 'PEN', 'PGK', 'PHP', 'PKR', 'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF', 'SAR', 'SBD', 'SCR', 'SDG',
    'SEK', 'SGD', 'SHP', 'SLL', 'SOS', 'SRD', 'STD', 'SVC', 'SYP', 'SZL', 'THB', 'TJS', 'TMT', 'TND', 'TOP', 'TRY',
    'TTD', 'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'UYU', 'UZS', 'VND', 'VUV', 'WST', 'XAF', 'XAG', 'XAU', 'XCD', 'XDR',
    'XOF', 'XPF', 'YER', 'ZAR', 'ZMK', 'ZMW', 'ZWL'
]


def get_inputs():
    """This function checks all inputs are correct and provided"""
    symbol = currency = start_date = end_date = 0
    # Remove 1st argument, load_data.py, from the list of command line arguments
    argumentList = sys.argv[1:]
    # Long options
    long_options = ["symbol=", "currency=", "start-date=", "end-date="]
    try:
        # Parsing argument
        arguments, values = getopt(args=argumentList, shortopts=[], longopts=long_options)
    except getopt.error as err:
        # output error, and return with an error code
        logging.info((str(err)))
        logging.info('Please try another input')
        sys.exit(1)

    try:
        # checking each argument
        for currentArgument, currentValue in arguments:
            if currentArgument == '--symbol':
                symbol = currentValue.upper()
            elif currentArgument == '--currency':
                currency = currentValue.upper()
                if currency not in VALID_CURRENCIES:
                    logging.info('Please input correct currency')
                    sys.exit(1)
            elif currentArgument == '--start-date':
                start_date = datetime.strptime(currentValue, "%Y-%m-%d").date()
            elif currentArgument == '--end-date':
                end_date = datetime.strptime(currentValue, "%Y-%m-%d").date()
    except ValueError as err:
        logging.info((str(err)))
        logging.info('Correct your time format')
        sys.exit(1)

    # Check all required inputs are provided
    if not symbol:
        logging.info('Symbol argument is missing, please try again. like --symbol AAPL')
        sys.exit(1)
    elif not currency:
        logging.info('Currency argument is missing, please try again. like --currency EUR')
        sys.exit(1)
    elif not start_date:
        logging.info('start date argument is missing, please try again. like --start-date 2022-09-03')
        sys.exit(1)
    elif not end_date:
        logging.info('end date argument is missing, please try again. like --end-date 2022-09-15')
        sys.exit(1)

    return symbol, currency, start_date, end_date


def get_rates(rates, start_date, end_date):
    """If we don't have the exchange rates in the DB, we get them from APIs and persist them in DB"""
    url = "https://api.apilayer.com/exchangerates_data/timeseries"
    querystring = {"start_date": start_date, "end_date": end_date, "base": "USD"}
    headers = {
        'apikey': API_LAYER_API_KEY,
    }
    try:
        response = requests.request("GET", url, headers=headers, params=querystring)
    except requests.exceptions.RequestException as err:
        logging.info(str(err))
        sys.exit(1)
    if response.status_code != 200:
        logging.info(response.json()["error"]["message"])
        sys.exit(1)
    new_dates_rates = response.json()["rates"]
    distinct_new_dates_rates = []
    new_rates = []

    for date in new_dates_rates:
        rate_obj = ExchangeRate(date=datetime.strptime(date, "%Y-%m-%d").date(), rates=new_dates_rates[date])
        new_rates.append(rate_obj)
        if rate_obj not in rates:
            logging.info(f'Exchange rates for day {rate_obj.date} is going to be added to the DB')
            distinct_new_dates_rates.append(rate_obj)

    if len(distinct_new_dates_rates) > 0:
        session.add_all(distinct_new_dates_rates)
        session.commit()
        logging.info('All above new rates saved to the DB')

    return new_rates


def get_stocks(stocks_in_USD, symbol, start_date, end_date):
    """If we don't have the stock prices in the DB, we get them from APIs and persist them in DB"""
    url = "http://api.marketstack.com/v1/eod"
    querystring = {"access_key": MARKET_STACK_API_ACCESS_KEY, "symbols": symbol, "date_from": start_date,
                   "date_to": end_date, "limit": MARKET_STACK_API_MAX_LIMIT}
    try:
        response = requests.request("GET", url, params=querystring)
    except requests.exceptions.RequestException as err:
        logging.info(str(err))
        sys.exit(1)
    if response.status_code != 200:
        logging.info(response.json()["error"]["message"])
        sys.exit(1)
    new_stocks = response.json()["data"]
    distinct_new_stocks = []
    new_stocks_in_USD = []

    for stock in new_stocks:
        stock_obj = StockPrice(symbol=symbol, date=datetime.strptime(stock['date'][:10], "%Y-%m-%d").date(),
                               price=float(stock['close']))
        new_stocks_in_USD.append(stock_obj)
        if stock_obj not in stocks_in_USD:
            logging.info(
                f'This stock price is going to be added: {stock_obj.date} {stock_obj.symbol} {stock_obj.price}')
            distinct_new_stocks.append(stock_obj)

    if len(distinct_new_stocks) > 0:
        session.add_all(distinct_new_stocks)
        session.commit()
        logging.info('All above stocks saved to the DB')

    return new_stocks_in_USD


if __name__ == '__main__':
    symbol, currency, start_date, end_date = get_inputs()
    stocks_in_USD = list(read_stock_prices(symbol, start_date, end_date))
    rates = list(read_exchange_rates(start_date, end_date))
    number_of_days = (end_date - start_date).days + 1
    number_of_busdays = np.busday_count(start_date, end_date + timedelta(days=1))

    # We check that all needed data is in the DB or not
    if number_of_days > len(rates):
        logging.info("We didn't have all the exchanges rates requested data in DB,"
                     " so we are requesting to the exchangeratesapi.io")
        rates = get_rates(rates, start_date, end_date)

    if number_of_busdays > len(stocks_in_USD):
        logging.info("We didn't have all the stock data requested in DB, so we are requesting to the MarketStack API")
        stocks_in_USD = get_stocks(stocks_in_USD, symbol, start_date, end_date)

    # We print the result in stdout
    for i, stock in enumerate(stocks_in_USD):
        print(stock.date, stock.symbol, currency, "{:.2f}".format(
            stock.price * next((rate.rates[currency] for rate in rates if rate.date == stock.date), None)))
