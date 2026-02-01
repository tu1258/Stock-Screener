import yfinance as yf

spx = yf.download("^GSPC", period="5d", interval="1d")
last_close = spx["Close"].iloc[-1]

print("SP500 last close:", float(last_close))
