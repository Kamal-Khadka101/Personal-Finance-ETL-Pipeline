# watch_test.py
"""
TEST VERSION of File Watcher
Saves cleaned data to CSV instead of PostgreSQL
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


class CSVHandler(FileSystemEventHandler):
    """Handles new CSV files dropped into the watch folder"""
    
    def __init__(self, output_folder):
        super().__init__()
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)
    
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
            
            # SAVE TO CSV (INSTEAD OF DATABASE)
            output_filename = f"CLEANED_{timestamp}_{filename}"
            output_path = os.path.join(self.output_folder, output_filename)
            clean_df.to_csv(output_path, index=False)
            
            print(f"\n💾 SAVE: Writing cleaned data...")
            print(f"  ✅ Saved to: {output_path}")
            print(f"  📊 Rows: {len(clean_df)}")
            print(f"  📋 Columns: {list(clean_df.columns)}")
            
            # Show preview
            print(f"\n👀 PREVIEW (first 3 rows):")
            print(clean_df.head(3).to_string())
            
            print(f"\n📈 CATEGORY DISTRIBUTION:")
            category_counts = clean_df['category'].value_counts()
            for cat, count in category_counts.items():
                print(f"  - {cat}: {count}")
            
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
        print(f"\n📁 Original moved to: {new_path}")
    
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
    OUTPUT_FOLDER = "./output_cleaned"
    
    print(f"\n{'='*60}")
    print("👁️  FILE WATCHER STARTED (TEST MODE)")
    print(f"{'='*60}")
    print(f"📂 Watching:  {WATCH_FOLDER}")
    print(f"📂 Output:    {OUTPUT_FOLDER}")
    print(f"📂 Processed: {PROCESSED_FOLDER}")
    print(f"📂 Failed:    {FAILED_FOLDER}")
    print(f"{'='*60}")
    print("\n💡 TEST MODE - Cleaned data saved as CSV")
    print("   Drop CSV files into watch folder")
    print("   Check output_cleaned/ for results")
    print("   Press Ctrl+C to stop\n")
    
    # Create folders
    for folder in [WATCH_FOLDER, PROCESSED_FOLDER, FAILED_FOLDER, OUTPUT_FOLDER]:
        os.makedirs(folder, exist_ok=True)
    
    event_handler = CSVHandler(OUTPUT_FOLDER)
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
