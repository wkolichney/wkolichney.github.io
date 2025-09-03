"""
Dashboard utility functions for stock portfolio analysis
"""

import pandas as pd
import yfinance as yf
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pytz
from sqlalchemy import text

def update_stock_prices(engine):
    """Update current prices for all stocks in the database"""
    try:
        query = "SELECT ticker FROM stocks;"
        df = pd.read_sql(query, engine)
        ticker = df['ticker'].to_list()
        
        if not ticker:
            return False, "No tickers found in database"
        
        data = yf.download(ticker, period="1d", progress=False)
        current_prices = data['Close'].iloc[-1]
        
        with engine.connect() as conn:
            for ticker_symbol, price in current_prices.items():
                conn.execute(
                    text("UPDATE stocks SET current_price = :price WHERE ticker = :ticker"),
                    {"price": float(price), "ticker": ticker_symbol}
                )
            conn.commit()
        
        return True, f"Successfully updated prices for {len(current_prices)} stocks"
        
    except Exception as e:
        return False, f"Error updating prices: {e}"

def get_portfolio_data(engine):
    """Get portfolio data with current prices and calculations"""
    query = """
    SELECT 
        a.account_name,
        a.account_id,
        p.ticker,
        s.company_name,
        s.sector,
        p.quantity,
        p.price_open,
        s.current_price,
        p.date_opened,
        (p.quantity * COALESCE(p.price_open, 0)) as cost_basis,
        (p.quantity * COALESCE(s.current_price, 0)) as current_value,
        ((p.quantity * COALESCE(s.current_price, 0)) - (p.quantity * COALESCE(p.price_open, 0))) as unrealized_gain_loss,
        CASE 
            WHEN p.price_open > 0 THEN 
                (((s.current_price - p.price_open) / p.price_open) * 100)
            ELSE 0 
        END as return_percentage
    FROM positions p
    JOIN stocks s ON p.ticker = s.ticker
    JOIN accounts a ON p.account_id = a.account_id
    WHERE a.account_type = 'investment'
    ORDER BY current_value DESC;
    """
    return pd.read_sql(query, engine)

def get_daily_performance(engine):
    """Get daily performance metrics for the portfolio"""
    try:
        current_query = """
        SELECT 
            p.ticker,
            s.company_name,
            p.quantity,
            p.price_open,
            s.current_price,
            (p.quantity * s.current_price) as current_value
        FROM positions p
        JOIN stocks s ON p.ticker = s.ticker
        JOIN accounts a ON p.account_id = a.account_id
        WHERE a.account_type = 'investment'
        """
        current_df = pd.read_sql(current_query, engine)
        
        if current_df.empty:
            return None
        
        tickers = current_df['ticker'].unique().tolist()
        data = yf.download(tickers, period="2d", progress=False)
        
        daily_changes = []
        
        for ticker in tickers:
            try:
                if len(tickers) == 1:
                    prices = data['Close']
                else:
                    prices = data['Close'][ticker]
                
                if len(prices) >= 2:
                    today_price = prices.iloc[-1]
                    yesterday_price = prices.iloc[-2]
                    price_change = today_price - yesterday_price
                    price_change_pct = (price_change / yesterday_price) * 100
                    
                    position_info = current_df[current_df['ticker'] == ticker].iloc[0]
                    quantity = position_info['quantity']
                    position_change = price_change * quantity
                    
                    daily_changes.append({
                        'ticker': ticker,
                        'company_name': position_info['company_name'],
                        'quantity': quantity,
                        'purchase_price': position_info['price_open'],
                        'yesterday_price': yesterday_price,
                        'today_price': today_price,
                        'price_change': price_change,
                        'price_change_pct': price_change_pct,
                        'position_value': position_info['current_value'],
                        'position_change': position_change
                    })
            except Exception:
                continue
        
        return pd.DataFrame(daily_changes)
        
    except Exception as e:
        print(f"Error getting daily performance: {e}")
        return None

def get_intraday_data(ticker_symbol):
    """Get intraday data for a specific stock"""
    try:
        stock = yf.Ticker(ticker_symbol)
        hist = stock.history(period="1d", interval="1m")
        return hist
    except Exception as e:
        print(f"Error getting intraday data for {ticker_symbol}: {e}")
        return None

def get_market_status():
    """Check if market is currently open"""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    
    is_weekday = now.weekday() < 5
    is_market_hours = market_open <= now <= market_close
    
    is_open = is_weekday and is_market_hours
    current_time = now.strftime('%Y-%m-%d %H:%M:%S')
    
    return is_open, current_time

def update_account_balances(engine):
    """Update account balances based on current positions"""
    query = """
    UPDATE accounts a
    SET balance = (
        SELECT COALESCE(SUM(p.quantity * s.current_price), 0)
        FROM positions p
        JOIN stocks s ON p.ticker = s.ticker
        WHERE p.account_id = a.account_id
    )
    WHERE account_type = 'investment';
    """
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()

def create_daily_performance_chart(daily_df):
    """Create daily performance bar chart"""
    fig = px.bar(daily_df, x='ticker', y='position_change',
                color='price_change_pct',
                color_continuous_scale='RdYlGn',
                title="Today's Position Changes ($)")
    return fig

def create_allocation_charts(df):
    """Create allocation charts by stock and sector"""
    # By stock
    fig_stock = px.bar(df, x='current_value', y='ticker', 
                      title="By Stock", orientation='h')
    fig_stock.update_layout(yaxis={'categoryorder': 'total ascending'})
    
    # By sector
    sector_df = df.groupby('sector')['current_value'].sum().reset_index()
    fig_sector = px.bar(sector_df, x='current_value', y='sector',
                       title="By Sector", orientation='h')
    fig_sector.update_layout(yaxis={'categoryorder': 'total ascending'})
    
    return fig_stock, fig_sector

def create_intraday_chart(intraday_data, ticker):
    """Create intraday price chart"""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=intraday_data.index,
        y=intraday_data['Close'],
        mode='lines',
        name=f'{ticker} Price',
        line=dict(width=2)
    ))
    
    fig.update_layout(
        title=f"{ticker} - Today's Price Movement",
        xaxis_title="Time",
        yaxis_title="Price ($)",
        hovermode='x unified'
    )
    
    return fig

def format_daily_movers_table(daily_df):
    """Format and style the daily movers table"""
    daily_display = daily_df.copy()
    
    # Format numeric columns safely
    daily_display['purchase_price'] = daily_display['purchase_price'].apply(
        lambda x: f"${x:.2f}" if isinstance(x, (int, float)) and pd.notna(x) else "N/A")
    daily_display['yesterday_price'] = daily_display['yesterday_price'].apply(
        lambda x: f"${x:.2f}" if isinstance(x, (int, float)) else str(x))
    daily_display['today_price'] = daily_display['today_price'].apply(
        lambda x: f"${x:.2f}" if isinstance(x, (int, float)) else str(x))
    daily_display['price_change'] = daily_display['price_change'].apply(
        lambda x: f"${x:+.2f}" if isinstance(x, (int, float)) else str(x))
    daily_display['price_change_pct'] = daily_display['price_change_pct'].apply(
        lambda x: f"{x:+.2f}%" if isinstance(x, (int, float)) else str(x))
    daily_display['position_change'] = daily_display['position_change'].apply(
        lambda x: f"${x:+,.2f}" if isinstance(x, (int, float)) else str(x))
    
    return daily_display

def apply_color_styling():
    """Return the color styling function for dataframes"""
    def color_changes(val):
        """Apply color styling to percentage and position changes"""
        if val.name in ['Price Δ%', 'Position Δ']:
            colors = []
            for v in val:
                if isinstance(v, str):
                    if '+' in v and not v.startswith('$-') and not v.startswith('-'):
                        colors.append('color: green; font-weight: bold')
                    elif '-' in v or v.startswith('$-'):
                        colors.append('color: red; font-weight: bold')
                    else:
                        colors.append('color: black')
                else:
                    colors.append('color: black')
            return colors
        return [''] * len(val)
    
    return color_changes

def format_detailed_holdings_table(df):
    """Format the detailed holdings table"""
    display_df = df.copy()
    
    # Safely format numeric columns
    display_df['current_price'] = display_df['current_price'].apply(
        lambda x: f"${x:.2f}" if pd.notna(x) and isinstance(x, (int, float)) else "N/A")
    display_df['price_open'] = display_df['price_open'].apply(
        lambda x: f"${x:.2f}" if pd.notna(x) and isinstance(x, (int, float)) else "N/A")
    display_df['current_value'] = display_df['current_value'].apply(
        lambda x: f"${x:,.2f}" if pd.notna(x) and isinstance(x, (int, float)) else "N/A")
    display_df['cost_basis'] = display_df['cost_basis'].apply(
        lambda x: f"${x:,.2f}" if pd.notna(x) and isinstance(x, (int, float)) else "N/A")
    display_df['unrealized_gain_loss'] = display_df['unrealized_gain_loss'].apply(
        lambda x: f"${x:,.2f}" if pd.notna(x) and isinstance(x, (int, float)) else "N/A")
    display_df['return_percentage'] = display_df['return_percentage'].apply(
        lambda x: f"{x:.2f}%" if pd.notna(x) and isinstance(x, (int, float)) else "N/A")
    display_df['quantity'] = display_df['quantity'].apply(
        lambda x: f"{x:.6f}" if pd.notna(x) and isinstance(x, (int, float)) else "N/A")
    
    return display_df