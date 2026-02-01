import pandas as pd
import yfinance as yf
"""
def calculate_total_rs(stock_close: pd.Series, spx_close: pd.Series) -> float:
    
    計算單檔股票 total RS score
    依據 IBD RS Rating 方法：最後一季權重加倍
    
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
"""
def calculate_total_rs(stock_close: pd.Series, spx_close: pd.Series) -> float:
    """
    計算單檔股票 total RS score
    可處理最少30天的資料
    """
    n_days = [63, 126, 189, 252]
    weights = [0.4, 0.2, 0.2, 0.2]

    # 如果資料不夠長，只取可用天數
    max_len = len(stock_close)
    n_days = [min(n, max_len-1) for n in n_days]  # -1 避免 index error

    # 至少要30天
    if max_len < 30:
        return None  # 或回傳 0，代表無法算

    # 計算 stock 的表現
    perf_stock = sum(weights[i] * (stock_close.iloc[-1] / stock_close.iloc[-n_days[i]-1])
                     for i in range(len(weights)))
    
    # 計算 SPX 的表現
    perf_spx = sum(weights[i] * (spx_close.iloc[-1] / spx_close.iloc[-n_days[i]-1])
                   for i in range(len(weights)))

    total_rs_score = perf_stock / perf_spx * 100
    return total_rs_score


def calculate_rs_ranking(rs_scores: pd.Series) -> pd.Series:
    """
    將 total RS score 對應全市場百分位 -> RS Ranking 1~99
    使用 pandas qcut 模擬 Fred 官方方法
    """
    # 用 qcut 分成 100 個百分位，duplicates="drop" 避免邊界重複
    rs_rank = pd.qcut(rs_scores, 100, labels=False, duplicates="drop")

    # 轉換成 1~99
    rs_rank = (rs_rank + 1).astype(int)  # qcut labels 從 0 開始，所以 +1
    rs_rank[rs_rank > 99] = 99          # 確保不超過 99
    rs_rank[rs_rank < 1] = 1            # 確保不低於 1
    
    return rs_rank

def get_stock_data(ticker: str) -> pd.Series:
    """
    下載股票收盤價資料
    """
    return yf.download(ticker, period="400d", interval="1d", auto_adjust=True)['Close']

def get_spx_data() -> pd.Series:
    """
    下載 S&P500 收盤價
    """
    return yf.download("^GSPC", period="400d", interval="1d", auto_adjust=True)['Close']
