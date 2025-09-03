import pandas as pd
import yfinance as yf
import sqlalchemy
from sqlalchemy import create_engine
from datetime import datetime

engine = create_engine("mysql+pymysql://root:root@localhost:3306/finance?charset=utf8mb4")

df = pd.read_excel('C:/Users/wikku/personal_finance/position.xlsx')

def get_opening_prices(df):
    df['price_open'] = None
    
    for index, row in df.iterrows():
        ticker = row['ticker']
        date_opened = row['date_opened']
        
        try:
            # Get historical data for that specific date
            stock = yf.Ticker(ticker)
            
            # Get data around that date (sometimes exact date might not exist due to weekends/holidays)
            hist = stock.history(start=date_opened, period="5d")
            
            if not hist.empty:
                # Use the opening price of the first available day
                opening_price = hist['Open'].iloc[0]
                df.at[index, 'price_open'] = round(opening_price, 4)
            else:
                print(f"No data found for {ticker} on {date_opened}")
                
        except Exception as e:
            print(f"Error getting price for {ticker}: {e}")
    
    return df

# Usage
df = get_opening_prices(df)

df.to_sql("positions", engine, if_exists="append", index=False, chunksize=1000, method='multi')
