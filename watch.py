"""
File Watcher with Database Integration
Watches folder for new CSV files and loads them into PostgreSQL
"""
import os
import time
import shutil
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd

from config import WATCH_FOLDER, PROCESSED_FOLDER, FAILED_FOLDER
from transform import clean_transaction_data
from database import insert_transactions, test_connection


class CSVHandler(FileSystemEventHandler):
    """Handles new CSV files dropped into the watch folder"""
    
    def on_created(self, event):
        """Triggered when a new file is created"""
        if event.is_directory or not event.src_path.endswith('.csv'):
            return
        
        print(f"\n{'='*60}")
        print(f"🔔 NEW FILE DETECTED: {os.path.basename(event.src_path)}")
        print(f"{'='*60}")
        
        time.sleep(2)  # Wait for file to be fully written
        self.process_file(event.src_path)
    
    def process_file(self, filepath):
        """Complete ETL pipeline for a single CSV"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.basename(filepath)
        
        try:
            # EXTRACT
            print(f"\n📥 EXTRACT: Reading {filename}...")
            df = pd.read_csv(filepath)
            print(f"  ✅ Loaded {len(df)} rows")
            
            # TRANSFORM
            clean_df = clean_transaction_data(df)
            
            # LOAD INTO DATABASE
            print(f"\n💾 LOAD: Inserting into PostgreSQL...")
            rows_inserted = insert_transactions(clean_df)
            
            # Show summary
            print(f"\n📊 DATABASE SUMMARY:")
            print(f"  ✅ Rows inserted: {rows_inserted}")
            print(f"  📋 Columns: {list(clean_df.columns)}")
            
            # Show preview
            print(f"\n👀 SAMPLE DATA (first 3 rows):")
            preview_cols = ['transaction_date', 'transaction_desc', 'category', 'amount']
            print(clean_df[preview_cols].head(3).to_string(index=False))
            
            print(f"\n📈 CATEGORY DISTRIBUTION:")
            category_counts = clean_df['category'].value_counts()
            for cat, count in category_counts.items():
                cat_total = clean_df[clean_df['category'] == cat]['amount'].sum()
                print(f"  - {cat}: {count} transactions (${cat_total:,.2f})")
            
            # MOVE TO PROCESSED
            self.move_to_processed(filepath, timestamp)
            
            print(f"\n{'='*60}")
            print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
            print(f"{'='*60}\n")
            
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            import traceback
            traceback.print_exc()  # Show full error trace
            self.move_to_failed(filepath, timestamp, str(e))
            print(f"\n{'='*60}")
            print("❌ ETL PIPELINE FAILED")
            print(f"{'='*60}\n")
    
    def move_to_processed(self, filepath, timestamp):
        """Move successfully processed file"""
        os.makedirs(PROCESSED_FOLDER, exist_ok=True)
        new_path = os.path.join(PROCESSED_FOLDER, f"{timestamp}_{os.path.basename(filepath)}")
        shutil.move(filepath, new_path)
        print(f"\n📁 Original file moved to: {new_path}")
    
    def move_to_failed(self, filepath, timestamp, error):
        """Move failed file and log error"""
        os.makedirs(FAILED_FOLDER, exist_ok=True)
        new_path = os.path.join(FAILED_FOLDER, f"{timestamp}_{os.path.basename(filepath)}")
        shutil.move(filepath, new_path)
        
        error_log = new_path.replace('.csv', '_ERROR.txt')
        with open(error_log, 'w') as f:
            f.write(f"Error at: {timestamp}\n")
            f.write(f"File: {os.path.basename(filepath)}\n")
            f.write(f"Error: {error}\n")
        
        print(f"\n📁 Failed file: {new_path}")
        print(f"📄 Error log: {error_log}")


def start_watching():
    """Start watching the folder for new CSV files"""
    print(f"\n{'='*60}")
    print("🔌 CHECKING DATABASE CONNECTION")
    print(f"{'='*60}")
    
    # Test database before starting watcher
    if not test_connection():
        print("\n❌ Cannot start file watcher - database not available")
        print("\n💡 Start the database first:")
        print("   docker-compose up -d")
        return
    
    print(f"\n{'='*60}")
    print("👁️  FILE WATCHER STARTED")
    print(f"{'='*60}")
    print(f"📂 Watching:  {WATCH_FOLDER}")
    print(f"📂 Processed: {PROCESSED_FOLDER}")
    print(f"📂 Failed:    {FAILED_FOLDER}")
    print(f"💾 Database:  PostgreSQL (localhost:5432)")
    print(f"{'='*60}")
    print("\n💡 Drop CSV files into the watch folder")
    print("   Data will be cleaned and loaded into PostgreSQL")
    print("   Press Ctrl+C to stop\n")
    
    # Create folders
    for folder in [WATCH_FOLDER, PROCESSED_FOLDER, FAILED_FOLDER]:
        os.makedirs(folder, exist_ok=True)
    
    event_handler = CSVHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_FOLDER, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping file watcher...")
        observer.stop()
    
    observer.join()
    print("✅ File watcher stopped.\n")


if __name__ == "__main__":
    start_watching()
