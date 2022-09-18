import logging
from model import Example, ExchangeRate, StockPrice
from extensions import session
from sqlalchemy import desc

logging.getLogger().setLevel(logging.INFO)


# This is a sample how to read from the database.

def read_stock_prices(symbol, start_date, end_date):
    return session.query(StockPrice).filter(StockPrice.date >= start_date, StockPrice.date <= end_date,
                                            StockPrice.symbol == symbol).order_by(desc(StockPrice.date)).all()


def read_exchange_rates(start_date, end_date):
    return session.query(ExchangeRate).filter(ExchangeRate.date >= start_date,
                                              ExchangeRate.date <= end_date).order_by(desc(ExchangeRate.date)).all()


if __name__ == '__main__':
    rows = session.query(Example).all()
    logging.info(f'We have {len(rows)} rows in Example table')
