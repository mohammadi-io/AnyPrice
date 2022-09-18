import getopt, logging, requests, sys
from datetime import datetime
from extensions import session
from model import StockPrice, ExchangeRate
from read_data import read_stock_prices, read_exchange_rates

logging.getLogger().setLevel(logging.INFO)
MARKET_STACK_API_MAX_LIMIT = 1000
MARKET_STACK_API_ACCESS_KEY = "54f39600d905fa2a94597ef8bd994b92"
API_LAYER_API_KEY = "sVhl4tNYYQGR7339uz2OKodq91MkiTSC"


def get_inputs():
    """This function checks all input are correct and provided"""
    symbol = currency = start_date = end_date = 0
    # Remove 1st argument, load_data.py, from the list of command line arguments
    argumentList = sys.argv[1:]
    # Long options
    long_options = ["symbol=", "currency=", "start-date=", "end-date="]
    try:
        # Parsing argument
        arguments, values = getopt.getopt(args=argumentList, shortopts=[], longopts=long_options)

        # checking each argument
        for currentArgument, currentValue in arguments:
            if currentArgument == '--symbol':
                symbol = currentValue.upper()
            elif currentArgument == '--currency':
                currency = currentValue.upper()
            elif currentArgument == '--start-date':
                start_date = currentValue
            elif currentArgument == '--end-date':
                end_date = currentValue
    except getopt.error as err:
        # output error, and return with an error code
        logging.info((str(err)))
        logging.info('Please try another input')
        sys.exit(1)
    # Check all required inputs are provided
    if not (symbol and currency and start_date and end_date):
        logging.info('One of the arguments is missing, please try again.')
        sys.exit(1)
    return symbol, currency, start_date, end_date


def get_stocks_rates(stocks_in_USD, rates, symbol, start_date, end_date):
    """If we don't have the instances in the DB, we get them from APIs and persist them in DB"""
    url = "https://api.apilayer.com/exchangerates_data/timeseries"
    querystring = {"start_date": start_date, "end_date": end_date, "base": "USD"}
    headers = {
        'apikey': API_LAYER_API_KEY,
    }
    new_dates_rates = requests.request("GET", url, headers=headers, params=querystring).json()["rates"]
    distinct_new_dates_rates = []
    new_rates = []
    for date in new_dates_rates:
        rate_obj = ExchangeRate(date=datetime.strptime(date, "%Y-%m-%d").date(), rates=new_dates_rates[date])
        new_rates.append(rate_obj)
        if rate_obj not in rates:
            logging.info(f'this obj is going to be added {rate_obj}')
            distinct_new_dates_rates.append(rate_obj)
    session.add_all(distinct_new_dates_rates)
    session.commit()

    url = "http://api.marketstack.com/v1/eod"
    querystring = {"access_key": MARKET_STACK_API_ACCESS_KEY, "symbols": symbol, "date_from": start_date,
                   "date_to": end_date, "limit": MARKET_STACK_API_MAX_LIMIT}
    new_stocks = requests.request("GET", url, params=querystring).json()["data"]
    distinct_new_stocks = []
    new_stocks_in_USD = []

    for stock in new_stocks:
        stock_obj = StockPrice(symbol=symbol, date=datetime.strptime(stock['date'][:10], "%Y-%m-%d").date(),
                               price=float(stock['close']))
        new_stocks_in_USD.append(stock_obj)
        if stock_obj not in stocks_in_USD:
            logging.info(f'this obj is going to be added {stock_obj}')
            distinct_new_stocks.append(stock_obj)
    session.add_all(distinct_new_stocks)
    session.commit()
    return new_stocks_in_USD, new_rates


if __name__ == '__main__':
    symbol, currency, start_date, end_date = get_inputs()
    stocks_in_USD = list(read_stock_prices(symbol, start_date, end_date))
    rates = list(read_exchange_rates(start_date, end_date))
    number_of_days = (datetime.strptime(end_date, "%Y-%m-%d").date() - datetime.strptime(start_date,
                                                                                         "%Y-%m-%d").date()).days + 1

    # We check that all needed data is in the DB or not
    if number_of_days > len(rates):
        stocks_in_USD, rates = get_stocks_rates(stocks_in_USD, rates, symbol, start_date, end_date)

    # We print the result in stdout
    for i, stock in enumerate(stocks_in_USD):
        print(stock.date, stock.symbol, currency, "{:.2f}".format(
            stock.price * next((rate.rates[currency] for rate in rates if rate.date == stock.date), None)))
