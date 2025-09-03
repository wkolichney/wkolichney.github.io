import pandas as pd
import yfinance as yf
import sqlalchemy
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://root:root@localhost:3306/finance?charset=utf8mb4")




df = pd.read_excel('C:/Users/wikku/personal_finance/stock.xlsx')

ticker = df['ticker'].to_list()

data = yf.download(ticker, period="1d")
current_prices = data['Close'].iloc[-1]

df['current_price'] = df['ticker'].map(current_prices)


df.to_sql("stocks", engine, if_exists="append", index=False, chunksize=1000, method='multi')