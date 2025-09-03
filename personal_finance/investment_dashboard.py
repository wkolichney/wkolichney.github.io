import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Import your custom functions
from dashboard_functions import (
    update_stock_prices,
    get_portfolio_data,
    get_daily_performance,
    get_intraday_data,
    get_market_status,
    update_account_balances,
    create_daily_performance_chart,
    create_allocation_charts,
    create_intraday_chart,
    format_daily_movers_table,
    apply_color_styling,
    format_detailed_holdings_table
)

# Streamlit page config
st.set_page_config(
    page_title="Stock Portfolio Dashboard",
    page_icon="📈",
    layout="wide"
)

# Database connection
@st.cache_resource
def get_database_connection():
    return create_engine("mysql+pymysql://root:root@localhost:3306/finance?charset=utf8mb4")

engine = get_database_connection()

# Title
st.title("📈 Stock Portfolio Dashboard")

# Sidebar controls
st.sidebar.header("Controls")

# Market status
is_open, current_time = get_market_status()
if is_open:
    st.sidebar.success("🟢 Market is OPEN")
else:
    st.sidebar.info("🔴 Market is CLOSED")
st.sidebar.caption(f"Current ET time: {current_time}")

st.sidebar.markdown("---")

# Control buttons
if st.sidebar.button("🔄 Update Stock Prices", type="primary"):
    with st.spinner("Updating stock prices..."):
        success, message = update_stock_prices(engine)
        if success:
            update_account_balances(engine)
            st.cache_data.clear()
            st.sidebar.success(message)
        else:
            st.sidebar.error(message)

if st.sidebar.button("💰 Update Account Balances"):
    with st.spinner("Updating account balances..."):
        update_account_balances(engine)
        st.sidebar.success("Account balances updated!")

st.sidebar.markdown("---")
st.sidebar.markdown("💡 **Tips:**")
st.sidebar.markdown("• Update prices regularly for accurate data")
st.sidebar.markdown("• Check that all positions have opening prices")
st.sidebar.markdown("• Account balances auto-update when prices refresh")

# Main dashboard content
try:
    # Get portfolio data
    @st.cache_data(ttl=300)
    def get_cached_portfolio_data(_engine):
        return get_portfolio_data(_engine)
    
    @st.cache_data(ttl=300)
    def get_cached_daily_performance(_engine):
        return get_daily_performance(_engine)

    with st.spinner("Loading portfolio data..."):
        df = get_cached_portfolio_data(engine)
    
    if df.empty:
        st.warning("No portfolio data found. Make sure you have positions in your database.")
        st.stop()
    
    # Portfolio Summary Section
    st.header("Portfolio Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    total_value = df['current_value'].sum()
    total_cost = df['cost_basis'].sum()
    total_gain_loss = df['unrealized_gain_loss'].sum()
    overall_return = ((total_value - total_cost) / total_cost * 100) if total_cost > 0 else 0
    
    with col1:
        st.metric("Total Portfolio Value", f"${total_value:,.2f}")
    with col2:
        st.metric("Total Cost Basis", f"${total_cost:,.2f}")
    with col3:
        st.metric("Unrealized Gain/Loss", f"${total_gain_loss:,.2f}", 
                 delta=f"{overall_return:.2f}%")
    with col4:
        st.metric("Number of Positions", len(df))
    
    # Daily Performance Section
    st.header("📊 Today's Performance")
    
    daily_df = get_cached_daily_performance(engine)
    
    if daily_df is not None and not daily_df.empty:
        # Daily summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        total_position_change = daily_df['position_change'].sum()
        avg_change_pct = daily_df['price_change_pct'].mean()
        winners = len(daily_df[daily_df['price_change'] > 0])
        losers = len(daily_df[daily_df['price_change'] < 0])
        
        with col1:
            st.metric("Today's P&L", f"${total_position_change:,.2f}")
        with col2:
            st.metric("Winners vs Losers", f"{winners} / {losers}")
        with col3:
            if not daily_df.empty:
                best = daily_df.loc[daily_df['price_change_pct'].idxmax()]
                st.metric("Best Performer", best['ticker'], 
                         delta=f"{best['price_change_pct']:.2f}%")
        with col4:
            if not daily_df.empty:
                worst = daily_df.loc[daily_df['price_change_pct'].idxmin()]
                st.metric("Worst Performer", worst['ticker'], 
                         delta=f"{worst['price_change_pct']:.2f}%")
        
        # Daily performance chart
        fig_daily = create_daily_performance_chart(daily_df)
        st.plotly_chart(fig_daily, use_container_width=True)
        
        # Daily movers table with enhanced formatting and colors
        st.subheader("Daily Movers")
        
        # Format the data
        daily_display = format_daily_movers_table(daily_df)
        
        # Select columns for display
        display_columns = ['ticker', 'company_name', 'purchase_price', 'yesterday_price', 
                          'today_price', 'price_change', 'price_change_pct', 'position_change']
        
        # Rename columns for better display
        column_names = {
            'ticker': 'Ticker',
            'company_name': 'Company',
            'purchase_price': 'Purchase Price',
            'yesterday_price': 'Yesterday',
            'today_price': 'Today',
            'price_change': 'Price Δ',
            'price_change_pct': 'Price Δ%',
            'position_change': 'Position Δ'
        }
        
        # Apply styling and display
        color_function = apply_color_styling()
        styled_daily = (daily_display[display_columns]
                       .rename(columns=column_names)
                       .style.apply(color_function))
        
        st.dataframe(styled_daily, use_container_width=True)
        
    else:
        st.info("Update stock prices to see daily performance data.")
    
    # Account Breakdown Section
    st.header("By Account")
    account_summary = df.groupby(['account_name', 'account_id']).agg({
        'current_value': 'sum',
        'cost_basis': 'sum',
        'unrealized_gain_loss': 'sum'
    }).reset_index()
    
    account_summary['return_pct'] = (account_summary['unrealized_gain_loss'] / 
                                   account_summary['cost_basis'] * 100)
    
    for _, row in account_summary.iterrows():
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(f"🏦 {row['account_name']}", f"${row['current_value']:,.2f}")
        with col2:
            st.metric("Cost Basis", f"${row['cost_basis']:,.2f}")
        with col3:
            st.metric("Gain/Loss", f"${row['unrealized_gain_loss']:,.2f}", 
                     delta=f"{row['return_pct']:.2f}%")
        with col4:
            positions_count = len(df[df['account_id'] == row['account_id']])
            st.metric("Positions", positions_count)
    
    # Portfolio Allocation Section
    st.header("Portfolio Allocation")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_stock, fig_sector = create_allocation_charts(df)
        st.plotly_chart(fig_stock, use_container_width=True)
    
    with col2:
        st.plotly_chart(fig_sector, use_container_width=True)
    
    # Intraday Tracking Section
    st.header("📈 Intraday Tracking")
    
    # Get available tickers for selection
    available_tickers = df['ticker'].unique().tolist()
    selected_ticker = st.selectbox("Select stock for intraday view:", 
                                  options=available_tickers)
    
    if selected_ticker:
        with st.spinner(f"Loading intraday data for {selected_ticker}..."):
            intraday_data = get_intraday_data(selected_ticker)
        
        if intraday_data is not None and not intraday_data.empty:
            # Intraday chart
            fig_intraday = create_intraday_chart(intraday_data, selected_ticker)
            st.plotly_chart(fig_intraday, use_container_width=True)
            
            # Intraday stats
            col1, col2, col3, col4 = st.columns(4)
            
            open_price = intraday_data['Open'].iloc[0]
            current_price = intraday_data['Close'].iloc[-1]
            high_price = intraday_data['High'].max()
            low_price = intraday_data['Low'].min()
            
            with col1:
                st.metric("Open", f"${open_price:.2f}")
            with col2:
                st.metric("Current", f"${current_price:.2f}", 
                         delta=f"{((current_price - open_price) / open_price * 100):+.2f}%")
            with col3:
                st.metric("High", f"${high_price:.2f}")
            with col4:
                st.metric("Low", f"${low_price:.2f}")
        else:
            st.warning(f"No intraday data available for {selected_ticker}")
    
    # Detailed Holdings Table
    st.header("Detailed Holdings")
    
    # Format the main dataframe for display
    display_columns = {
        'account_name': 'Account',
        'ticker': 'Ticker',
        'company_name': 'Company',
        'sector': 'Sector',
        'quantity': 'Shares',
        'price_open': 'Open Price',
        'current_price': 'Current Price',
        'cost_basis': 'Cost Basis',
        'current_value': 'Current Value',
        'unrealized_gain_loss': 'Gain/Loss',
        'return_percentage': 'Return %',
        'date_opened': 'Date Opened'
    }
    
    display_df_final = format_detailed_holdings_table(df)[list(display_columns.keys())].rename(columns=display_columns)
    
    # Color coding for gain/loss
    def color_gain_loss(val):
        if 'Gain/Loss' in val.name or 'Return %' in val.name:
            colors = []
            for v in val:
                if '-' in str(v):
                    colors.append('color: red')
                elif '$0.00' in str(v) or '0.00%' in str(v):
                    colors.append('color: black')
                else:
                    colors.append('color: green')
            return colors
        return ['' for _ in val]
    
    styled_df = display_df_final.style.apply(color_gain_loss)
    st.dataframe(styled_df, use_container_width=True)
    
    # Export Section
    st.header("Export Data")
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download Portfolio Data as CSV",
        data=csv,
        file_name=f"portfolio_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

except Exception as e:
    st.error(f"Error loading portfolio data: {e}")
    st.info("Make sure your database is running and contains the required tables with data.")