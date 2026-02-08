# transform.py
"""
Data Transformation Module
Cleans raw bank statement CSV data
"""
import pandas as pd
import numpy as np
from config import CATEGORY_PATTERNS, DATE_FORMAT


def categorize_transaction(description):
    """
    Rule-based categorization based on merchant keywords
    
    Args:
        description (str): Transaction description
        
    Returns:
        str: Category name
    """
    if pd.isna(description):
        return 'Other'
    
    description = str(description)
    
    for category, pattern in CATEGORY_PATTERNS.items():
        if pattern.search(description):
            return category
    
    return 'Other'


def derive_transaction_type(amount):
    """
    Derive transaction type from amount sign
    
    Args:
        amount (float): Transaction amount
        
    Returns:
        str: 'Credit', 'Debit', or 'Neutral'
    """
    if pd.isna(amount):
        return "Unknown"
    elif amount > 0:
        return "Credit"
    elif amount < 0:
        return "Debit"
    else:  # amount == 0
        return "Neutral"


def validate_raw_data(df):
    """
    Validate raw CSV has expected structure
    
    Args:
        df (pd.DataFrame): Raw DataFrame
        
    Raises:
        ValueError: If validation fails
    """
    from config import EXPECTED_COLUMNS
    
    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    print(f"✅ Raw data validation passed ({len(df)} rows)")

def validate_clean_data(df):
    """
    Ensure cleaned data meets quality standards
    
    Args:
        df (pd.DataFrame): Cleaned DataFrame
        
    Returns:
        bool: True if all checks pass
        
    Raises:
        ValueError: If validation fails
    """
    checks = {
        'No nulls in date': df['transaction_date'].isna().sum() == 0,
        'No nulls in amount': df['amount'].isna().sum() == 0,
        'Date is datetime': pd.api.types.is_datetime64_any_dtype(df['transaction_date']),  # ← CHANGED
        'Amount is numeric': pd.api.types.is_numeric_dtype(df['amount']),
        'All categories assigned': df['category'].isna().sum() == 0,
        'All transaction types assigned': df['transaction_type'].isna().sum() == 0,
    }
    
    print("\n📋 Data Quality Checks:")
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"  {status} {check}")
        if not passed:
            # DEBUG: Print actual dtype for datetime check
            if check == 'Date is datetime':
                print(f"      Actual dtype: {df['transaction_date'].dtype}")
            raise ValueError(f"Validation failed: {check}")
    
    print("\n✅ All validation checks passed!")
    return True

def clean_transaction_data(df):
    """
    Main transformation pipeline
    
    Args:
        df (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Cleaned transaction data
    """
    print("\n" + "="*60)
    print("🔄 TRANSFORMATION PIPELINE STARTED")
    print("="*60)
    
    # Step 1: Validate input
    print("\n1️⃣ Validating raw data...")
    validate_raw_data(df)
    
    # Step 2: Remove duplicates
    print("\n2️⃣ Checking for duplicates...")
    initial_rows = len(df)
    df = df.drop_duplicates()
    duplicates_removed = initial_rows - len(df)
    if duplicates_removed > 0:
        print(f"  ⚠️  Removed {duplicates_removed} duplicate rows")
    else:
        print(f"  ✅ No duplicates found")
    
    # Step 3: Apply categorization
    print("\n3️⃣ Categorizing transactions...")
    df['category_corrected'] = df['description'].apply(categorize_transaction)
    category_dist = df['category_corrected'].value_counts()
    print(f"  ✅ Categories assigned:")
    for cat, count in category_dist.items():
        print(f"     - {cat}: {count}")
    
    # Step 4: Derive transaction type
    print("\n4️⃣ Deriving transaction types...")
    df['derived_transaction_type'] = df['amount'].apply(derive_transaction_type)
    type_dist = df['derived_transaction_type'].value_counts()
    print(f"  ✅ Transaction types:")
    for ttype, count in type_dist.items():
        print(f"     - {ttype}: {count}")
    
    # Step 5: Parse dates
    print("\n5️⃣ Parsing dates...")
    df['date_dt'] = pd.to_datetime(df['date'], format=DATE_FORMAT, errors='coerce')
    invalid_dates = df['date_dt'].isna().sum()
    if invalid_dates > 0:
        print(f"  ⚠️  Warning: {invalid_dates} invalid dates found, dropping...")
        df = df.dropna(subset=['date_dt'])
    else:
        print(f"  ✅ All dates parsed successfully")
    
    # Step 6: Create final structure
    print("\n6️⃣ Creating final DataFrame...")
    clean_df = df[[
        "date_dt",
        "description",
        "category_corrected",
        "derived_transaction_type",
        "amount"
    ]].copy()
    
    # Add month_year for aggregation
    clean_df['month_year'] = clean_df['date_dt'].dt.strftime('%Y-%m')
    
    # Rename to database-friendly names
    clean_df.rename(columns={
        "date_dt": "transaction_date",
        "category_corrected": "category",
        "derived_transaction_type": "transaction_type",
        "description": "transaction_desc"
    }, inplace=True)
    
    # Step 7: Final validation
    print("\n7️⃣ Validating cleaned data...")
    validate_clean_data(clean_df)
    
    print("\n" + "="*60)
    print(f"✨ TRANSFORMATION COMPLETE: {len(clean_df)} rows ready")
    print("="*60 + "\n")
    
    return clean_df


# For standalone testing
if __name__ == "__main__":
    # Test with sample data
    test_df = pd.read_csv('sample_data/MOCK_DATA(1).csv')
    cleaned = clean_transaction_data(test_df)
    print(cleaned.head())
    print(f"\nShape: {cleaned.shape}")
    print(f"Columns: {cleaned.columns.tolist()}")
