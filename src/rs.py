import pandas as pd
import yfinance as yf

def calculate_total_rs(stock_close: pd.Series, spx_close: pd.Series) -> float:
    """
    計算單檔股票 total RS score
    """
    n63, n126, n189, n252 = 63, 126, 189, 252

    perf_stock = (
        0.4*(stock_close.iloc[-1]/stock_close.iloc[-n63-1]) +
        0.2*(stock_close.iloc[-1]/stock_close.iloc[-n126-1]) +
        0.2*(stock_close.iloc[-1]/stock_close.iloc[-n189-1]) +
        0.2*(stock_close.iloc[-1]/stock_close.iloc[-n252-1])
    )
    perf_spx = (
        0.4*(spx_close.iloc[-1]/spx_close.iloc[-n63-1]) +
        0.2*(spx_close.iloc[-1]/spx_close.iloc[-n126-1]) +
        0.2*(spx_close.iloc[-1]/spx_close.iloc[-n189-1]) +
        0.2*(spx_close.iloc[-1]/spx_close.iloc[-n252-1])
    )

    total_rs_score = perf_stock / perf_spx * 100
    return total_rs_score

def calculate_rs_ranking(rs_scores: pd.Series) -> pd.Series:
    """
    將 total RS score 對應全市場百分位 -> RS Ranking 1~99
    """
    # 排序百分位
    rs_rank = rs_scores.rank(pct=True) * 99
    rs_rank = rs_rank.round(0)
    rs_rank[rs_rank < 1] = 1
    return rs_rank.astype(int)

def get_stock_data(ticker: str):
    """
    下載股票資料
    """
    return yf.download(ticker, period="400d", interval="1d", auto_adjust=True)['Close']

def get_spx_data():
    return yf.download("^GSPC", period="400d", interval="1d", auto_adjust=True)['Close']
