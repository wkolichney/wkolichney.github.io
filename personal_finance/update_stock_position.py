
import pandas as pd
import yfinance as yf
import sqlalchemy
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def insert_positions_safely(df, engine):
    """
    Insert positions while avoiding duplicates by checking existing records first
    """
    if df.empty:
        logger.info("No positions to insert")
        return
    
    try:
        # Create a temporary table with new positions
        df.to_sql('temp_positions', engine, if_exists='replace', index=False)
        
        # Find positions that don't already exist in the database
        # Assuming your unique constraint is on ticker, quantity, date_opened (based on the error)
        query = """
        SELECT tp.* FROM temp_positions tp
        LEFT JOIN positions p ON (
            tp.ticker = p.ticker 
            AND tp.quantity = p.quantity 
            AND tp.date_opened = p.date_opened
            AND tp.account_id = p.account_id
        )
        WHERE p.position_id IS NULL
        """
        
        new_positions_df = pd.read_sql(query, engine)
        
        if not new_positions_df.empty:
            # Insert only the new positions
            new_positions_df.to_sql('positions', engine, if_exists='append', index=False, method='multi')
            logger.info(f"Successfully inserted {len(new_positions_df)} new positions")
            
            # Log which positions were inserted
            for _, row in new_positions_df.iterrows():
                logger.info(f"  Added: {row['ticker']} - {row['quantity']} shares on {row['date_opened']}")
        else:
            logger.info("No new positions to insert (all already exist)")
        
        # Clean up temporary table
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS temp_positions"))
            
    except Exception as e:
        logger.error(f"Error inserting positions safely: {e}")
        # Clean up temporary table on error
        try:
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS temp_positions"))
        except:
            pass
        raise

def get_opening_prices(df):
    """
    Enhanced version that fetches opening prices more efficiently
    """
    df = df.copy()  # Avoid modifying original dataframe
    df['price_open'] = None
    
    # Group by ticker to minimize API calls
    ticker_groups = df.groupby('ticker')
    
    for ticker, group in ticker_groups:
        try:
            # Get the date range we need for this ticker
            dates = pd.to_datetime(group['date_opened'])
            min_date = dates.min()
            max_date = dates.max()
            
            # Add buffer days for weekends/holidays
            start_date = min_date - timedelta(days=7)
            end_date = max_date + timedelta(days=7)
            
            logger.info(f"Fetching data for {ticker} from {start_date.date()} to {end_date.date()}")
            
            # Get historical data for the entire range at once
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date)
            
            if hist.empty:
                logger.warning(f"No historical data found for {ticker}")
                continue
            
            # For each position of this ticker, find the opening price
            for idx in group.index:
                position_date = pd.to_datetime(df.at[idx, 'date_opened'])
                opening_price = find_opening_price_for_date(hist, position_date, ticker)
                
                if opening_price is not None:
                    df.at[idx, 'price_open'] = round(opening_price, 4)
                else:
                    logger.warning(f"Could not find opening price for {ticker} on {position_date.date()}")
                    
        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")
    
    return df

def find_opening_price_for_date(hist_data, target_date, ticker):
    """
    Find the best opening price for a given date from historical data
    """
    try:
        target_date_str = target_date.strftime('%Y-%m-%d')
        
        # First try: exact date match
        if target_date_str in hist_data.index.strftime('%Y-%m-%d'):
            exact_match = hist_data[hist_data.index.strftime('%Y-%m-%d') == target_date_str]
            return exact_match['Open'].iloc[0]
        
        # Second try: find the next available trading day
        future_data = hist_data[hist_data.index.date >= target_date.date()]
        if not future_data.empty:
            logger.info(f"Using next available trading day for {ticker} (requested: {target_date_str}, used: {future_data.index[0].strftime('%Y-%m-%d')})")
            return future_data['Open'].iloc[0]
        
        # Third try: use the last available day before target date
        past_data = hist_data[hist_data.index.date < target_date.date()]
        if not past_data.empty:
            logger.info(f"Using previous trading day for {ticker} (requested: {target_date_str}, used: {past_data.index[-1].strftime('%Y-%m-%d')})")
            return past_data['Open'].iloc[-1]
        
        return None
        
    except Exception as e:
        logger.error(f"Error finding opening price for {ticker} on {target_date}: {e}")
        return None

## MAIN WORKFLOW ##
def main():
    # Database connection
    engine = create_engine("mysql+pymysql://root:root@localhost:3306/finance?charset=utf8mb4")
    
    try:
        # Read new stock trades
        new_stock = pd.read_excel('C:/Users/wikku/personal_finance/stock.xlsx')
        logger.info(f"Read {len(new_stock)} trades from Excel file")
        
        if new_stock.empty:
            logger.info("No trades to process")
            return
        
        # 1. UPDATE STOCKS TABLE (for new stocks only)
        stock_table_df = new_stock.drop(columns=['date_opened', 'quantity', 'account_id']).drop_duplicates()
        
        # Get current prices for stocks table
        tickers = stock_table_df['ticker'].unique().tolist()
        logger.info(f"Fetching current prices for {len(tickers)} unique tickers")
        
        try:
            data = yf.download(tickers, period="1d", progress=False)
            if len(tickers) == 1:
                current_prices = {tickers[0]: data['Close'].iloc[-1]}
            else:
                current_prices = data['Close'].iloc[-1].to_dict()
            
            stock_table_df['current_price'] = stock_table_df['ticker'].map(current_prices)
        except Exception as e:
            logger.warning(f"Could not fetch current prices: {e}")
            stock_table_df['current_price'] = None
        
        # Create temp table and find new stocks
        stock_table_df.to_sql("temp_stock", engine, if_exists="replace", index=False, chunksize=1000, method='multi')
        
        query = """
        SELECT temp_stock.* FROM temp_stock 
        LEFT JOIN stocks ON temp_stock.ticker = stocks.ticker 
        WHERE stocks.ticker IS NULL;
        """
        
        new_stocks_df = pd.read_sql(query, engine)
        
        if not new_stocks_df.empty:
            new_stocks_df.to_sql('stocks', engine, if_exists="append", index=False, chunksize=1000, method='multi')
            logger.info(f"Added {len(new_stocks_df)} new stocks to database: {new_stocks_df['ticker'].tolist()}")
        else:
            logger.info("No new stocks to add")
        
        # Clean up temp table
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS temp_stock"))
        
        # 2. UPDATE POSITIONS TABLE (for ALL positions from Excel)
        logger.info("Processing positions...")
        
        # Prepare all positions data
        all_positions_df = new_stock[['account_id', 'ticker', 'quantity', 'date_opened']].copy()
        
        # Get opening prices for all positions
        all_positions_df = get_opening_prices(all_positions_df)
        
        # Check for failed price fetches
        failed_prices = all_positions_df[all_positions_df['price_open'].isna()]
        if not failed_prices.empty:
            logger.warning(f"Failed to fetch prices for {len(failed_prices)} positions:")
            for _, row in failed_prices.iterrows():
                logger.warning(f"  {row['ticker']} on {row['date_opened']}")
            
            # Remove positions without prices
            all_positions_df = all_positions_df.dropna(subset=['price_open'])
            logger.info(f"Proceeding with {len(all_positions_df)} positions that have valid prices")
        
        if all_positions_df.empty:
            logger.warning("No positions to insert (all price fetches failed)")
            return
        
        # Insert positions using safe method to handle duplicates
        try:
            insert_positions_safely(all_positions_df, engine)
        except Exception as e:
            logger.error(f"Error inserting positions: {e}")
            raise
        
        logger.info("Database update completed successfully!")
        
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    main()