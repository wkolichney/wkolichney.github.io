import pandas as pd
import yfinance as yf
import sqlalchemy
from sqlalchemy import create_engine, text
engine = create_engine("mysql+pymysql://root:root@localhost:3306/finance?charset=utf8mb4")


def update_stock_prices(engine):
    """Update current prices for all stocks in the database"""
    try:
        # Get tickers
        query = "SELECT ticker FROM stocks;"
        df = pd.read_sql(query, engine)
        ticker = df['ticker'].to_list()
        
        if not ticker:
            print("No tickers found in database")
            return
        
        # Get current prices
        print(f"Fetching prices for {len(ticker)} tickers...")
        data = yf.download(ticker, period="1d", progress=False)
        current_prices = data['Close'].iloc[-1]
        
        # Update database
        with engine.connect() as conn:
            for ticker_symbol, price in current_prices.items():
                conn.execute(
                    text("UPDATE stocks SET current_price = :price WHERE ticker = :ticker"),
                    {"price": float(price), "ticker": ticker_symbol}
                )
            conn.commit()
        
        print(f"Successfully updated prices for {len(current_prices)} stocks")
        
    except Exception as e:
        print(f"Error updating prices: {e}")

# Usage
update_stock_prices(engine)