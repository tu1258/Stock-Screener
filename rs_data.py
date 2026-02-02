#!/usr/bin/env python
import requests
import json
import time
import bs4 as bs
import datetime as dt
import os
import pickle
import requests
import yaml
import yfinance as yf
import pandas as pd
import dateutil.relativedelta
import numpy as np
import re
from ftplib import FTP
from io import StringIO
from time import sleep

from datetime import date
from datetime import datetime

DIR = os.path.dirname(os.path.realpath(__file__))

if not os.path.exists(os.path.join(DIR, 'data')):
    os.makedirs(os.path.join(DIR, 'data'))
if not os.path.exists(os.path.join(DIR, 'tmp')):
    os.makedirs(os.path.join(DIR, 'tmp'))

try:
    with open(os.path.join(DIR, 'config_private.yaml'), 'r') as stream:
        private_config = yaml.safe_load(stream)
except FileNotFoundError:
    private_config = None
except yaml.YAMLError as exc:
        print(exc)

try:
    with open('config.yaml', 'r') as stream:
        config = yaml.safe_load(stream)
except FileNotFoundError:
    config = None
except yaml.YAMLError as exc:
        print(exc)

def cfg(key):
    try:
        return private_config[key]
    except:
        try:
            return config[key]
        except:
            return None

def read_json(json_file):
    with open(json_file, "r", encoding="utf-8") as fp:
        return json.load(fp)

PRICE_DATA_FILE = os.path.join(DIR, "data", "price_history.json")
REFERENCE_TICKER = cfg("REFERENCE_TICKER")
DATA_SOURCE = cfg("DATA_SOURCE")
ALL_STOCKS = cfg("USE_ALL_LISTED_STOCKS")
TICKER_INFO_FILE = os.path.join(DIR, "data_persist", "ticker_info.json")
TICKER_INFO_DICT = read_json(TICKER_INFO_FILE)
REF_TICKER = {"ticker": REFERENCE_TICKER, "sector": "--- Reference ---", "industry": "--- Reference ---", "universe": "--- Reference ---"}

UNKNOWN = "unknown"

def get_securities(url, ticker_pos = 1, table_pos = 1, sector_offset = 1, industry_offset = 1, universe = "N/A"):
    resp = requests.get(url)
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.findAll('table', {'class': 'wikitable sortable'})[table_pos-1]
    secs = {}
    for row in table.findAll('tr')[table_pos:]:
        sec = {}
        sec["ticker"] = row.findAll('td')[ticker_pos-1].text.strip()
        sec["sector"] = row.findAll('td')[ticker_pos-1+sector_offset].text.strip()
        sec["industry"] = row.findAll('td')[ticker_pos-1+sector_offset+industry_offset].text.strip()
        sec["universe"] = universe
        secs[sec["ticker"]] = sec
    with open(os.path.join(DIR, "tmp", "tickers.pickle"), "wb") as f:
        pickle.dump(secs, f)
    return secs

def get_resolved_securities():
    tickers = {REFERENCE_TICKER: REF_TICKER}
    if ALL_STOCKS:
        return get_tickers_from_nasdaq(tickers)
        # return {"1": {"ticker": "DTST", "sector": "MICsec", "industry": "MICind", "universe": "we"}, "2": {"ticker": "MIGI", "sector": "MIGIsec", "industry": "MIGIind", "universe": "we"}}
    else:
        return get_tickers_from_wikipedia(tickers)

def get_tickers_from_wikipedia(tickers):
    if cfg("NQ100"):
        tickers.update(get_securities('https://en.wikipedia.org/wiki/Nasdaq-100', 2, 3, universe="Nasdaq 100"))
    if cfg("SP500"):
        tickers.update(get_securities('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies', sector_offset=3, universe="S&P 500"))
    if cfg("SP400"):
        tickers.update(get_securities('https://en.wikipedia.org/wiki/List_of_S%26P_400_companies', 2, universe="S&P 400"))
    if cfg("SP600"):
        tickers.update(get_securities('https://en.wikipedia.org/wiki/List_of_S%26P_600_companies', 2, universe="S&P 600"))
    return tickers

def exchange_from_symbol(symbol):
    if symbol == "Q":
        return "NASDAQ"
    if symbol == "A":
        return "NYSE MKT"
    if symbol == "N":
        return "NYSE"
    if symbol == "P":
        return "NYSE ARCA"
    if symbol == "Z":
        return "BATS"
    if symbol == "V":
        return "IEXG"
    return "n/a"

def get_tickers_from_nasdaq(tickers):
    filename = "nasdaqtraded.txt"
    ticker_column = 1
    etf_column = 5
    exchange_column = 3
    test_column = 7
    ftp = FTP('ftp.nasdaqtrader.com')
    ftp.login()
    ftp.cwd('SymbolDirectory')
    lines = StringIO()
    ftp.retrlines('RETR '+filename, lambda x: lines.write(str(x)+'\n'))
    ftp.quit()
    lines.seek(0)
    results = lines.readlines()

    for entry in results:
        sec = {}
        values = entry.split('|')
        ticker = values[ticker_column]
        if re.match(r'^[A-Z]+$', ticker) and values[etf_column] == "N" and values[test_column] == "N":
            sec["ticker"] = ticker
            sec["sector"] = UNKNOWN
            sec["industry"] = UNKNOWN
            sec["universe"] = exchange_from_symbol(values[exchange_column])
            tickers[sec["ticker"]] = sec

    return tickers

SECURITIES = get_resolved_securities().values()

def write_to_file(dict_of_dfs, file):
    output = {}
    for ticker, df in dict_of_dfs.items():
        if isinstance(df, pd.DataFrame):
            # 將 index 轉成字串
            df_copy = df.copy()
            df_copy.index = df_copy.index.astype(str)
            output[ticker] = df_copy.to_dict('index')
        else:
            output[ticker] = df
    with open(file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

def write_price_history_file(tickers_dict):
    write_to_file(tickers_dict, PRICE_DATA_FILE)
        
def write_ticker_info_file(info_dict):
    write_to_file(info_dict, TICKER_INFO_FILE)

def enrich_ticker_data(ticker_response, security):
    ticker_response["sector"] = security["sector"]
    ticker_response["industry"] = security["industry"]
    ticker_response["universe"] = security["universe"]

def print_data_progress(ticker, universe, idx, securities, error_text, elapsed_s, remaining_s):
    dt_ref = datetime.fromtimestamp(0)
    dt_e = datetime.fromtimestamp(elapsed_s)
    elapsed = dateutil.relativedelta.relativedelta (dt_e, dt_ref)
    if remaining_s and not np.isnan(remaining_s):
        dt_r = datetime.fromtimestamp(remaining_s)
        remaining = dateutil.relativedelta.relativedelta (dt_r, dt_ref)
        remaining_string = f'{remaining.hours}h {remaining.minutes}m {remaining.seconds}s'
    else:
        remaining_string = "?"
    print(f'{ticker} from {universe}{error_text} ({idx+1} / {len(securities)}). Elapsed: {elapsed.hours}h {elapsed.minutes}m {elapsed.seconds}s. Remaining: {remaining_string}.')

def get_remaining_seconds(all_load_times, idx, len):
    load_time_ma = pd.Series(all_load_times).rolling(np.minimum(idx+1, 25)).mean().tail(1).item()
    remaining_seconds = (len - idx) * load_time_ma
    return remaining_seconds

def escape_ticker(ticker):
    return ticker.replace(".","-")

def get_info_from_dict(dict, key):
    value = dict[key] if key in dict else "n/a"
    # fix unicode
    # value = value.replace("\u2014", " ")
    return value

def load_ticker_info(ticker, info_dict):
    escaped_ticker = escape_ticker(ticker)
    info = yf.Ticker(escaped_ticker)
    try:
        ticker_info = {
            "info": {
                "industry": get_info_from_dict(info.info, "industry"),
                "sector": get_info_from_dict(info.info, "sector")
            }
        }
    except Exception:
        ticker_info = {
            "info": {
                "industry": "n/a",
                "sector": "n/a"
            }
        }
    info_dict[ticker] = ticker_info



def load_prices_from_yahoo(securities, info={}):
    print("*** Loading Stocks from Yahoo Finance ***")
    today = date.today()
    start = time.time()
    start_date = today - dt.timedelta(days=365 + 183)  # 1.5 years
    tickers_dict = {}
    load_times = []
    failed_tickers = []
    securities = list(securities)[:100]  # 前 100 支股票

    for idx, security in enumerate(securities):
        ticker = security["ticker"]
        ticker_data = None
        
        try:
            yf_ticker = yf.Ticker(ticker)
            df = yf_ticker.history(start=start_date, end=today)

            if df.empty:
                raise ValueError("Empty data returned")
            
            # 轉成 dict
            ticker_data = df.to_dict("index")
            
            # 將 index 轉成字串，避免 JSON dump 出錯
            df.index = df.index.strftime("%Y-%m-%d")
            ticker_data = df.to_dict("index")

        except Exception as e:
            print(f"Failed to download {ticker}: {str(e)}")
            failed_tickers.append(ticker)
            continue

        tickers_dict[ticker] = ticker_data

        # 印出進度
        remaining_seconds = get_remaining_seconds(load_times + [0], idx, len(securities))
        print_data_progress(ticker, security["universe"], idx, securities, "", 0, remaining_seconds)

    if failed_tickers:
        print(f"Failed for {len(failed_tickers)} tickers: {', '.join(failed_tickers[:10])}...")
        with open("failed_tickers.txt", "w") as f:
            f.write("\n".join(failed_tickers))
        print("Saved list of failed tickers to failed_tickers.txt")

    # 寫檔
    write_price_history_file(tickers_dict)
    return tickers_dict

def save_data(source, securities, info = {}):
    if source == "YAHOO":
        load_prices_from_yahoo(securities, info)

def main(forceTDA = False):
    dataSource = DATA_SOURCE if not forceTDA else "TD_AMERITRADE"
    save_data(dataSource, SECURITIES, {"forceTDA": forceTDA})
    write_ticker_info_file(TICKER_INFO_DICT)

if __name__ == "__main__":
    main()
