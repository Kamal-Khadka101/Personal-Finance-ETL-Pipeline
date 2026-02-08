"""
Personal Finance Dashboard
Streamlit app for visualizing transaction data from PostgreSQL
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from database import get_connection

# Page config
st.set_page_config(
    page_title="Personal Finance Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=60)  # Cache for 60 seconds
def load_all_transactions():
    """Load all transactions from database"""
    query = """
    SELECT 
        transaction_date,
        transaction_desc,
        category,
        transaction_type,
        amount,
        month_year
    FROM transactions
    ORDER BY transaction_date DESC
    """
    
    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn, parse_dates=['transaction_date'])
        return df
    finally:
        conn.close()


@st.cache_data(ttl=60)
def get_summary_stats(df):
    """Calculate summary statistics"""
    total_transactions = len(df)
    total_income = df[df['transaction_type'] == 'Credit']['amount'].sum()
    total_expenses = df[df['transaction_type'] == 'Debit']['amount'].sum()
    net_balance = total_income + total_expenses  # Expenses are negative
    
    return {
        'total_transactions': total_transactions,
        'total_income': total_income,
        'total_expenses': abs(total_expenses),
        'net_balance': net_balance
    }


def create_category_pie_chart(df):
    """Create pie chart for spending by category"""
    # Filter only expenses (debits)
    expenses = df[df['transaction_type'] == 'Debit'].copy()
    
    # Group by category
    category_spending = expenses.groupby('category')['amount'].sum().abs().reset_index()
    category_spending.columns = ['Category', 'Amount']
    category_spending = category_spending.sort_values('Amount', ascending=False)
    
    # Create pie chart
    fig = px.pie(
        category_spending,
        values='Amount',
        names='Category',
        title='Spending by Category',
        hole=0.4,  # Donut chart
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Amount: $%{value:,.2f}<br>Percent: %{percent}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=True,
        height=500
    )
    
    return fig


def create_monthly_trend_chart(df):
    """Create line chart for monthly spending trends"""
    # Group by month and transaction type
    monthly = df.groupby(['month_year', 'transaction_type'])['amount'].sum().reset_index()
    
    # Pivot to have income and expenses as separate columns
    monthly_pivot = monthly.pivot(index='month_year', columns='transaction_type', values='amount').reset_index()
    monthly_pivot = monthly_pivot.fillna(0)
    
    # Make expenses positive for display
    if 'Debit' in monthly_pivot.columns:
        monthly_pivot['Expenses'] = monthly_pivot['Debit'].abs()
    else:
        monthly_pivot['Expenses'] = 0
    
    if 'Credit' in monthly_pivot.columns:
        monthly_pivot['Income'] = monthly_pivot['Credit']
    else:
        monthly_pivot['Income'] = 0
    
    # Calculate net
    monthly_pivot['Net'] = monthly_pivot['Income'] - monthly_pivot['Expenses']
    
    # Create figure
    fig = go.Figure()
    
    # Add income trace
    fig.add_trace(go.Scatter(
        x=monthly_pivot['month_year'],
        y=monthly_pivot['Income'],
        mode='lines+markers',
        name='Income',
        line=dict(color='green', width=3),
        marker=dict(size=8)
    ))
    
    # Add expenses trace
    fig.add_trace(go.Scatter(
        x=monthly_pivot['month_year'],
        y=monthly_pivot['Expenses'],
        mode='lines+markers',
        name='Expenses',
        line=dict(color='red', width=3),
        marker=dict(size=8)
    ))
    
    # Add net trace
    fig.add_trace(go.Scatter(
        x=monthly_pivot['month_year'],
        y=monthly_pivot['Net'],
        mode='lines+markers',
        name='Net (Income - Expenses)',
        line=dict(color='blue', width=3, dash='dash'),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        title='Monthly Financial Trends',
        xaxis_title='Month',
        yaxis_title='Amount ($)',
        hovermode='x unified',
        height=500,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig


def create_category_bar_chart(df):
    """Create horizontal bar chart for category breakdown"""
    # Group by category
    category_data = df.groupby(['category', 'transaction_type'])['amount'].sum().reset_index()
    
    # Separate income and expenses
    expenses = category_data[category_data['transaction_type'] == 'Debit'].copy()
    expenses['amount'] = expenses['amount'].abs()
    expenses = expenses.sort_values('amount', ascending=True)
    
    # Create horizontal bar chart
    fig = px.bar(
        expenses,
        x='amount',
        y='category',
        orientation='h',
        title='Spending by Category (Detailed)',
        labels={'amount': 'Amount ($)', 'category': 'Category'},
        color='amount',
        color_continuous_scale='Reds',
        text='amount'
    )
    
    fig.update_traces(
        texttemplate='$%{text:,.2f}',
        textposition='outside'
    )
    
    fig.update_layout(
        height=400,
        showlegend=False
    )
    
    return fig


def create_transaction_timeline(df):
    """Create timeline scatter plot of transactions"""
    # Take recent transactions only
    recent_df = df.head(100).copy()
    
    # Color code by type
    color_map = {'Credit': 'green', 'Debit': 'red', 'Neutral': 'gray'}
    recent_df['color'] = recent_df['transaction_type'].map(color_map)
    
    fig = px.scatter(
        recent_df,
        x='transaction_date',
        y='amount',
        color='transaction_type',
        size=recent_df['amount'].abs(),
        hover_data=['transaction_desc', 'category'],
        title='Recent Transaction Timeline (Last 100)',
        labels={'amount': 'Amount ($)', 'transaction_date': 'Date'},
        color_discrete_map=color_map
    )
    
    fig.update_layout(
        height=400,
        hovermode='closest'
    )
    
    return fig


# ============================================================================
# MAIN DASHBOARD
# ============================================================================

def main():
    # Header
    st.markdown('<h1 class="main-header">💰 Personal Finance Dashboard</h1>', unsafe_allow_html=True)
    
    # Load data
    try:
        with st.spinner('Loading data from database...'):
            df = load_all_transactions()
        
        if df.empty:
            st.warning("📭 No transactions found in the database.")
            st.info("💡 **Getting Started:**\n1. Drop a CSV file into `finance/watch/`\n2. Wait for processing\n3. Refresh this page")
            return
        
        # Calculate summary stats
        stats = get_summary_stats(df)
        
        # ============================================
        # SIDEBAR - Filters
        # ============================================
        st.sidebar.header("🔍 Filters")
        
        # Date range filter
        min_date = pd.to_datetime(df['transaction_date']).min().date()
        max_date = pd.to_datetime(df['transaction_date']).max().date()
        
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        # Category filter
        categories = ['All'] + sorted(df['category'].unique().tolist())
        selected_category = st.sidebar.selectbox("Category", categories)
        
        # Transaction type filter
        trans_types = ['All'] + sorted(df['transaction_type'].unique().tolist())
        selected_type = st.sidebar.selectbox("Transaction Type", trans_types)
        
        # Apply filters
        filtered_df = df.copy()
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            # Convert date objects to pandas Timestamps for comparison
            start_ts = pd.Timestamp(start_date)
            end_ts = pd.Timestamp(end_date)
            filtered_df = filtered_df[
                (filtered_df['transaction_date'] >= start_ts) &
                (filtered_df['transaction_date'] <= end_ts)
            ]
        
        if selected_category != 'All':
            filtered_df = filtered_df[filtered_df['category'] == selected_category]
        
        if selected_type != 'All':
            filtered_df = filtered_df[filtered_df['transaction_type'] == selected_type]
        
        # ============================================
        # SUMMARY METRICS
        # ============================================
        st.header("📊 Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="💳 Total Transactions",
                value=f"{stats['total_transactions']:,}"
            )
        
        with col2:
            st.metric(
                label="💰 Total Income",
                value=f"${stats['total_income']:,.2f}",
                delta="Inflow"
            )
        
        with col3:
            st.metric(
                label="💸 Total Expenses",
                value=f"${stats['total_expenses']:,.2f}",
                delta="Outflow",
                delta_color="inverse"
            )
        
        with col4:
            net_color = "normal" if stats['net_balance'] >= 0 else "inverse"
            st.metric(
                label="📈 Net Balance",
                value=f"${stats['net_balance']:,.2f}",
                delta_color=net_color
            )
        
        st.markdown("---")
        
        # ============================================
        # VISUALIZATIONS
        # ============================================
        
        # Row 1: Pie Chart and Monthly Trends
        col1, col2 = st.columns(2)
        
        with col1:
            st.plotly_chart(create_category_pie_chart(filtered_df), use_container_width=True)
        
        with col2:
            st.plotly_chart(create_monthly_trend_chart(filtered_df), use_container_width=True)
        
        # Row 2: Category Bar Chart
        st.plotly_chart(create_category_bar_chart(filtered_df), use_container_width=True)
        
        # Row 3: Transaction Timeline
        st.plotly_chart(create_transaction_timeline(filtered_df), use_container_width=True)
        
        st.markdown("---")
        
        # ============================================
        # DETAILED DATA TABLE
        # ============================================
        st.header("📋 Transaction Details")
        
        # Show filters applied
        if selected_category != 'All' or selected_type != 'All':
            st.info(f"📌 Showing {len(filtered_df)} transactions (filtered)")
        
        # Format the dataframe for display
        display_df = filtered_df.copy()
        display_df['amount'] = display_df['amount'].apply(lambda x: f"${x:,.2f}")
        display_df = display_df.rename(columns={
            'transaction_date': 'Date',
            'transaction_desc': 'Description',
            'category': 'Category',
            'transaction_type': 'Type',
            'amount': 'Amount'
        })
        
        st.dataframe(
            display_df[['Date', 'Description', 'Category', 'Type', 'Amount']],
            use_container_width=True,
            height=400
        )
        
        # Download button
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Filtered Data as CSV",
            data=csv,
            file_name=f"transactions_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"❌ Error loading data: {e}")
        st.info("💡 Make sure:\n1. Docker is running: `docker compose ps`\n2. Database is accessible: `python test_database.py`")


if __name__ == "__main__":
    main()
