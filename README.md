# AnyPrice
Gets any US stock daily close prices in any currency in a date range.

To use this repo, clone it to your local system and install requirements.
Then you can run the programm from command line.
Sample for receivng Microsft stock closing price between 1st of March 2022 and 10th of March 2022 in Euro:
python3 load_data.py --symbol MSFT --start-date 2022-03-01 --end-date 2022-03-10 --currency EUR
2022-03-10 MSFT EUR 259.30
2022-03-09 MSFT EUR 260.56
2022-03-08 MSFT EUR 253.15
2022-03-07 MSFT EUR 256.71
2022-03-04 MSFT EUR 264.77
2022-03-03 MSFT EUR 267.42
2022-03-02 MSFT EUR 270.12
2022-03-01 MSFT EUR 264.94
