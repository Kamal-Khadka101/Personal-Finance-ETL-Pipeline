# test_manual.py
"""
Manual test script - processes a single CSV without file watching
Use this to debug your transformation logic
"""
import pandas as pd
from datetime import datetime
from transform import clean_transaction_data


def test_single_file(input_path, output_path=None):
    """
    Test the transformation pipeline on a single CSV
    
    Args:
        input_path: Path to your test CSV file
        output_path: Where to save cleaned CSV (optional)
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    print(f"\n{'='*60}")
    print(f"🧪 MANUAL TEST - Single File Processing")
    print(f"{'='*60}")
    
    try:
        # EXTRACT
        print(f"\n📥 Reading: {input_path}")
        df = pd.read_csv(input_path)
        print(f"  ✅ Loaded {len(df)} rows, {len(df.columns)} columns")
        print(f"  📋 Columns: {list(df.columns)}")
        
        # TRANSFORM
        clean_df = clean_transaction_data(df)
        
        # SAVE
        if output_path is None:
            output_path = f"./test_output/CLEANED_{timestamp}_manual_test.csv"
        
        import os
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        clean_df.to_csv(output_path, index=False)
        
        print(f"\n💾 Saved cleaned data to: {output_path}")
        
        # ANALYSIS
        print(f"\n📊 CLEANED DATA SUMMARY:")
        print(f"  - Total rows: {len(clean_df)}")
        print(f"  - Date range: {clean_df['transaction_date'].min()} to {clean_df['transaction_date'].max()}")
        print(f"  - Total amount: ${clean_df['amount'].sum():,.2f}")
        
        print(f"\n📈 Categories:")
        for cat, count in clean_df['category'].value_counts().items():
            cat_total = clean_df[clean_df['category'] == cat]['amount'].sum()
            print(f"  - {cat}: {count} transactions (${cat_total:,.2f})")
        
        print(f"\n📉 Transaction Types:")
        for ttype, count in clean_df['transaction_type'].value_counts().items():
            print(f"  - {ttype}: {count}")
        
        print(f"\n👀 SAMPLE ROWS (first 5):")
        print(clean_df.head().to_string())
        
        print(f"\n{'='*60}")
        print("✅ TEST COMPLETED SUCCESSFULLY!")
        print(f"{'='*60}\n")
        
        return clean_df
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        print(f"\n{'='*60}")
        print("❌ TEST FAILED")
        print(f"{'='*60}\n")
        return None


if __name__ == "__main__":
    # EDIT THIS PATH to point to your test CSV
    test_file = "sample_data/MOCK_DATA(1).csv"
    
    # Run the test
    result = test_single_file(test_file)
    
    if result is not None:
        print(f"\n✨ You can now open: ./test_output/CLEANED_*_manual_test.csv")
