import getopt
import logging
import requests
import sys
from datetime import date, timedelta, datetime
from extensions import session
from model import Example, StockPrice, ExchangeRate
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


if __name__ == '__main__':
    symbol, currency, start_date, end_date = get_inputs()
    stocks_in_USD = list(read_stock_prices(symbol, start_date, end_date))
    rates = list(read_exchange_rates(start_date, end_date))
    number_of_days = (datetime.strptime(end_date, "%Y-%m-%d").date() - datetime.strptime(start_date, "%Y-%m-%d").date()).days + 1
    if number_of_days > len(rates):
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
                print('this obj is going to be added', rate_obj)
                distinct_new_dates_rates.append(rate_obj)
        session.add_all(distinct_new_dates_rates)
        session.commit()
        rates = new_rates

        url = "http://api.marketstack.com/v1/eod"
        querystring = {"access_key": MARKET_STACK_API_ACCESS_KEY, "symbols": symbol, "date_from": start_date,
                       "date_to": end_date, "limit": MARKET_STACK_API_MAX_LIMIT}
        new_stocks = requests.request("GET", url, params=querystring).json()["data"]
        distinct_new_stocks = []
        new_stocks_in_USD = []
        for stock in stocks_in_USD:
            print('For', stock.symbol, stock.date, stock.price)
        for stock in new_stocks:
            stock_obj = StockPrice(symbol=symbol, date=datetime.strptime(stock['date'][:10], "%Y-%m-%d").date(), price=float(stock['close']))
            print(stock_obj.symbol, stock_obj.date, stock_obj.price)
            new_stocks_in_USD.append(stock_obj)
            if stock_obj not in stocks_in_USD:
                print('this obj is going to be added', stock_obj)
                distinct_new_stocks.append(stock_obj)
        session.add_all(distinct_new_stocks)
        session.commit()
        stocks_in_USD = new_stocks_in_USD

    for i, stock in enumerate(stocks_in_USD):
        print(stock.symbol, stock.date, "{:.2f}".format(stock.price * next((rate.rates[currency] for rate in rates if rate.date == stock.date), None)))
    # example = Example(column_a='Hello', column_b='World')
    # example = StockPrice(symbol='MSFT', date=date.today()-timedelta(days=0), price=150.12)
    # example = ExchangeRate(date=date.today(), rates={
    #         "AED": 3.673104,
    #         "AFN": 87.334631,
    #         "ALL": 117.288203,
    #         "AMD": 400.21966,
    #         "ANG": 1.78094,
    #         "AOA": 430.04165,
    #         "ARS": 140.653687,
    #         "AUD": 1.487885,
    #         "AWG": 1.8,
    #         "AZN": 1.70397,
    #         "BAM": 1.952803,
    #         "BBD": 1.995307,
    #         "BDT": 93.903549,
    #         "BGN": 1.958214,
    #         "BHD": 0.376716,
    #         "BIF": 2039.588638,
    #         "BMD": 1,
    #         "BND": 1.391942,
    #         "BOB": 6.828416,
    #         "BRL": 5.253367,
    #         "BSD": 0.988168,
    #         "BTC": 0.000049884929,
    #         "BTN": 78.924966,
    #         "BWP": 12.88393,
    #         "BYN": 2.494334,
    #         "BYR": 19600,
    #         "BZD": 1.991913,
    #         "CAD": 1.326585,
    #         "CDF": 2041.000362,
    #         "CHF": 0.96454,
    #         "CLF": 0.031981,
    #         "CLP": 882.461085,
    #         "CNY": 6.983904,
    #         "COP": 4428.136389,
    #         "CRC": 647.835854,
    #         "CUC": 1,
    #         "CUP": 26.5,
    #         "CVE": 110.094354,
    #         "CZK": 24.483404,
    #         "DJF": 175.927313,
    #         "DKK": 7.424804,
    #         "DOP": 52.418751,
    #         "DZD": 140.297539,
    #         "EGP": 19.257151,
    #         "ERN": 15,
    #         "ETB": 52.170436,
    #         "EUR": 0.998373,
    #         "FJD": 2.252304,
    #         "FKP": 0.864596,
    #         "GBP": 0.875691,
    #         "GEL": 2.820391,
    #         "GGP": 0.864596,
    #         "GHS": 9.807898,
    #         "GIP": 0.864596,
    #         "GMD": 54.503853,
    #         "GNF": 8540.96151,
    #         "GTQ": 7.668414,
    #         "GYD": 206.749538,
    #         "HKD": 7.849595,
    #         "HNL": 24.353252,
    #         "HRK": 7.507704,
    #         "HTG": 116.113025,
    #         "HUF": 404.164504,
    #         "IDR": 14999.2,
    #         "ILS": 3.42415,
    #         "IMP": 0.864596,
    #         "INR": 79.650504,
    #         "IQD": 1442.294444,
    #         "IRR": 42400.000352,
    #         "ISK": 139.560386,
    #         "JEP": 0.864596,
    #         "JMD": 148.230243,
    #         "JOD": 0.70904,
    #         "JPY": 142.93104,
    #         "KES": 118.880236,
    #         "KGS": 81.051304,
    #         "KHR": 4067.395537,
    #         "KMF": 491.603796,
    #         "KPW": 900.000045,
    #         "KRW": 1386.490384,
    #         "KWD": 0.30921,
    #         "KYD": 0.823524,
    #         "KZT": 468.453896,
    #         "LAK": 15544.805551,
    #         "LBP": 1494.184015,
    #         "LKR": 355.751585,
    #         "LRD": 154.000348,
    #         "LSL": 17.630382,
    #         "LTL": 2.95274,
    #         "LVL": 0.60489,
    #         "LYD": 4.895811,
    #         "MAD": 10.523988,
    #         "MDL": 19.171085,
    #         "MGA": 4161.30997,
    #         "MKD": 61.519645,
    #         "MMK": 2075.216614,
    #         "MNT": 3224.442056,
    #         "MOP": 7.989916,
    #         "MRO": 356.999828,
    #         "MUR": 44.000346,
    #         "MVR": 15.470378,
    #         "MWK": 1014.988767,
    #         "MXN": 20.041904,
    #         "MYR": 4.535504,
    #         "MZN": 63.830377,
    #         "NAD": 17.630377,
    #         "NGN": 428.880377,
    #         "NIO": 35.51595,
    #         "NOK": 10.19622,
    #         "NPR": 126.280266,
    #         "NZD": 1.668822,
    #         "OMR": 0.384784,
    #         "PAB": 0.988168,
    #         "PEN": 3.837352,
    #         "PGK": 3.482003,
    #         "PHP": 57.010375,
    #         "PKR": 221.914732,
    #         "PLN": 4.713388,
    #         "PYG": 6819.330038,
    #         "QAR": 3.641038,
    #         "RON": 4.855404,
    #         "RSD": 117.158404,
    #         "RUB": 60.50369,
    #         "RWF": 1022.165643,
    #         "SAR": 3.757309,
    #         "SBD": 8.173384,
    #         "SCR": 13.783038,
    #         "SDG": 578.503678,
    #         "SEK": 10.75849,
    #         "SGD": 1.406404,
    #         "SHP": 1.377404,
    #         "SLL": 14615.000339,
    #         "SOS": 568.503667,
    #         "SRD": 27.952038,
    #         "STD": 20697.981008,
    #         "SVC": 8.646598,
    #         "SYP": 2512.530318,
    #         "SZL": 17.204034,
    #         "THB": 36.422038,
    #         "TJS": 10.094454,
    #         "TMT": 3.51,
    #         "TND": 3.200504,
    #         "TOP": 2.378038,
    #         "TRY": 18.258915,
    #         "TTD": 6.693325,
    #         "TWD": 31.256704,
    #         "TZS": 2304.478059,
    #         "UAH": 36.489342,
    #         "UGX": 3765.064151,
    #         "USD": 1,
    #         "UYU": 40.254605,
    #         "UZS": 10841.745295,
    #         "VND": 23660,
    #         "VUV": 118.961877,
    #         "WST": 2.725578,
    #         "XAF": 654.94184,
    #         "XAG": 0.051097,
    #         "XAU": 0.000597,
    #         "XCD": 2.70255,
    #         "XDR": 0.762917,
    #         "XOF": 654.94184,
    #         "XPF": 119.575037,
    #         "YER": 250.375037,
    #         "ZAR": 17.605204,
    #         "ZMK": 9001.203593,
    #         "ZMW": 15.242973,
    #         "ZWL": 321.999592
    #     })
    # session.add(example)
    # session.commit()

