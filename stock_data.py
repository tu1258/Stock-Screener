import pandas as pd
import yfinance as yf
from ftplib import FTP
from io import StringIO
from datetime import date, timedelta
import time

OUTPUT_FILE = "stock_data.csv"
TICKER_FILE = "stock_ticker.csv"
MAX_TICKERS = 250
DAYS = 400


def get_nasdaq_tickers(limit=None):
    """Download NASDAQ tickers and remove ETFs, test symbols,
    duplicated 5-letter suffix tickers, and shell companies."""

    ftp = FTP("ftp.nasdaqtrader.com")
    ftp.login()
    ftp.cwd("SymbolDirectory")

    data = StringIO()
    ftp.retrlines("RETR nasdaqtraded.txt", lambda x: data.write(x + "\n"))
    ftp.quit()

    data.seek(0)
    raw_tickers = []

    # --- basic NASDAQ filtering ---
    for line in data.readlines():
        cols = line.strip().split("|")
        if len(cols) < 8:
            continue

        ticker = cols[1]
        is_etf = cols[5]
        is_test = cols[7]

        if is_etf == "N" and is_test == "N":
            raw_tickers.append(ticker)

    # --- remove duplicated 5-letter suffix symbols ---
    filtered = []
    shorter_set = set()

    for t in raw_tickers:
        if len(t) < 5:
            filtered.append(t)
            shorter_set.add(t)

    for t in raw_tickers:
        if len(t) == 5:
            prefix4 = t[:4]
            prefix3 = t[:3]
            if prefix4 in shorter_set or prefix3 in shorter_set:
                continue
            filtered.append(t)

    # --- remove shell companies using Yahoo Finance ---
    final_tickers = []
    for i, ticker in enumerate(filtered, 1):
        try:
            info = yf.Ticker(ticker).info
            industry = info.get("industry", "")

            if industry == "Shell Companies":
                continue

            final_tickers.append(ticker)

            print(f"[Industry check {i}/{len(filtered)}] {ticker}")

            time.sleep(0.05)  # avoid Yahoo rate limit

        except Exception:
            # if Yahoo fails, keep ticker to avoid losing real stocks
            final_tickers.append(ticker)

    return final_tickers[:limit] if limit else final_tickers


def main():
    end = date.today()
    start = end - timedelta(days=DAYS)

    tickers = get_nasdaq_tickers(limit=MAX_TICKERS)

    rows = []

    for i, ticker in enumerate(tickers, 1):
        try:
            df = yf.download(
                ticker,
                start=start,
                end=end,
                progress=False,
                auto_adjust=False,
            )

            if df.empty:
                continue

            df = df.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
            df.columns = ["date", "open", "high", "low", "close", "volume"]
            df["ticker"] = ticker
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")

            rows.append(df)
            print(f"[{i}/{len(tickers)}] {ticker}")

            time.sleep(0.1)

        except Exception as e:
            print(f"Failed {ticker}: {e}")

    if not rows:
        raise RuntimeError("No data downloaded")

    result = pd.concat(rows, ignore_index=True)
    result = result[["ticker", "date", "open", "high", "low", "close", "volume"]]
    result.to_csv(OUTPUT_FILE, index=False)

    # save filtered tickers
    pd.DataFrame(tickers, columns=["ticker"]).to_csv(TICKER_FILE, index=False)


if __name__ == "__main__":
    main()
