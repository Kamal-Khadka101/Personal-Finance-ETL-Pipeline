"""
Database Module
Handles PostgreSQL connection and data insertion
"""
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from config import DB_CONNECTION_STRING


def get_connection():
    """
    Create a connection to PostgreSQL
    
    Returns:
        psycopg2.connection: Database connection
    """
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        return conn
    except psycopg2.OperationalError as e:
        raise ConnectionError(
            f"❌ Cannot connect to PostgreSQL!\n"
            f"Error: {e}\n"
            f"Make sure Docker container is running: docker-compose up -d"
        )


def create_table_if_not_exists():
    """
    Create transactions table if it doesn't exist
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        transaction_date DATE NOT NULL,
        transaction_desc TEXT,
        category VARCHAR(50) NOT NULL,
        transaction_type VARCHAR(20) NOT NULL,
        amount NUMERIC(12, 2) NOT NULL,
        month_year VARCHAR(7) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create index for faster queries
    CREATE INDEX IF NOT EXISTS idx_transaction_date ON transactions(transaction_date);
    CREATE INDEX IF NOT EXISTS idx_category ON transactions(category);
    CREATE INDEX IF NOT EXISTS idx_month_year ON transactions(month_year);
    """
    
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            conn.commit()
            print("  ✅ Table 'transactions' ready")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def insert_transactions(df):
    """
    Insert cleaned transaction data into PostgreSQL
    
    Args:
        df (pd.DataFrame): Cleaned transaction data
        
    Returns:
        int: Number of rows inserted
    """
    if df.empty:
        print("  ⚠️  No data to insert")
        return 0
    
    # Ensure table exists
    create_table_if_not_exists()
    
    # Prepare data for insertion
    columns = ['transaction_date', 'transaction_desc', 'category', 'transaction_type', 'amount', 'month_year']
    data_tuples = [tuple(row) for row in df[columns].values]
    
    insert_sql = """
    INSERT INTO transactions (transaction_date, transaction_desc, category, transaction_type, amount, month_year)
    VALUES %s
    """
    
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, data_tuples)
            conn.commit()
            rows_inserted = cur.rowcount
            print(f"  ✅ Inserted {rows_inserted} rows into database")
            return rows_inserted
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def get_transaction_count():
    """
    Get total number of transactions in database
    
    Returns:
        int: Total transaction count
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM transactions")
            count = cur.fetchone()[0]
            return count
    finally:
        conn.close()


def get_latest_transactions(limit=10):
    """
    Fetch most recent transactions
    
    Args:
        limit (int): Number of transactions to fetch
        
    Returns:
        pd.DataFrame: Latest transactions
    """
    query = """
    SELECT transaction_date, transaction_desc, category, transaction_type, amount
    FROM transactions
    ORDER BY transaction_date DESC, created_at DESC
    LIMIT %s
    """
    
    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn, params=(limit,))
        return df
    finally:
        conn.close()


def test_connection():
    """
    Test database connection and display info
    
    Returns:
        bool: True if connection successful
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print(f"\n✅ PostgreSQL Connection Successful!")
            print(f"   Version: {version[:50]}...")
            
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'transactions'
                );
            """)
            table_exists = cur.fetchone()[0]
            
            if table_exists:
                cur.execute("SELECT COUNT(*) FROM transactions")
                count = cur.fetchone()[0]
                print(f"   Table 'transactions' exists with {count} rows")
            else:
                print(f"   Table 'transactions' does not exist yet")
        
        conn.close()
        return True
    except Exception as e:
        print(f"\n❌ Database Connection Failed!")
        print(f"   Error: {e}")
        print(f"\n💡 Troubleshooting:")
        print(f"   1. Run: docker-compose up -d")
        print(f"   2. Wait 10 seconds for database to initialize")
        print(f"   3. Check: docker-compose ps")
        return False


if __name__ == "__main__":
    # Test the database connection
    print("="*60)
    print("🔌 Testing Database Connection")
    print("="*60)
    test_connection()
