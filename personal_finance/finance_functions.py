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




def insert_ignore_method(pd_table, conn, keys, data_iter):
    """Custom method to ignore duplicates"""
    from sqlalchemy import text
    
    data = [dict(zip(keys, row)) for row in data_iter]
    
    # Build the INSERT IGNORE statement manually
    columns = ', '.join(keys)
    placeholders = ', '.join([f':{key}' for key in keys])
    table_name = pd_table.table.name
    
    stmt = text(f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})")
    
    for row in data:
        conn.execute(stmt, row)