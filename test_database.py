"""
Database Connection Test Script
Run this to verify your PostgreSQL setup is working
"""
from database import test_connection, get_transaction_count, get_latest_transactions, create_table_if_not_exists
import pandas as pd


def run_database_tests():
    """
    Comprehensive database testing
    """
    print("\n" + "="*60)
    print("🧪 DATABASE CONNECTION TEST")
    print("="*60)
    
    # Test 1: Connection
    print("\n1️⃣ Testing connection...")
    if not test_connection():
        print("\n❌ Connection test failed. Stopping here.")
        print("\n💡 Make sure Docker is running:")
        print("   docker-compose up -d")
        print("   docker-compose ps")
        return False
    
    # Test 2: Table creation
    print("\n2️⃣ Testing table creation...")
    try:
        create_table_if_not_exists()
        print("  ✅ Table created/verified successfully")
    except Exception as e:
        print(f"  ❌ Table creation failed: {e}")
        return False
    
    # Test 3: Count records
    print("\n3️⃣ Checking record count...")
    try:
        count = get_transaction_count()
        print(f"  ✅ Current records in database: {count}")
    except Exception as e:
        print(f"  ❌ Count query failed: {e}")
        return False
    
    # Test 4: Fetch latest records (if any exist)
    if count > 0:
        print("\n4️⃣ Fetching latest transactions...")
        try:
            latest = get_latest_transactions(limit=5)
            print(f"  ✅ Retrieved {len(latest)} rows")
            print("\n  Sample data:")
            print(latest.to_string(index=False))
        except Exception as e:
            print(f"  ❌ Fetch query failed: {e}")
            return False
    else:
        print("\n4️⃣ No transactions in database yet")
        print("  💡 Drop a CSV file into ./finance/watch to add data")
    
    print("\n" + "="*60)
    print("✅ ALL DATABASE TESTS PASSED!")
    print("="*60)
    print("\n🎉 Your database is ready to use!")
    print("\nNext steps:")
    print("  1. Start the file watcher: python watch.py")
    print("  2. Drop CSV files into: ./finance/watch")
    print("  3. Watch the data flow into PostgreSQL!")
    print()
    
    return True


if __name__ == "__main__":
    success = run_database_tests()
    
    if not success:
        print("\n" + "="*60)
        print("❌ TESTS FAILED")
        print("="*60)
        print("\n🔧 Troubleshooting steps:")
        print("  1. docker-compose down")
        print("  2. docker-compose up -d")
        print("  3. Wait 10-15 seconds")
        print("  4. python test_database.py")
        print()
