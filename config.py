# config.py
import os
import re

# Paths - UPDATED FOR LOCAL TESTING
WATCH_FOLDER = os.getenv("FINANCE_WATCH_FOLDER", "./finance/watch")  # ← CHANGED
PROCESSED_FOLDER = os.path.join(WATCH_FOLDER, "../processed")  # ← CHANGED (sibling folder)
FAILED_FOLDER = os.path.join(WATCH_FOLDER, "../failed")        # ← CHANGED (sibling folder)

# Database (unused in test mode, but harmless)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "finance_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "yourpassword")
DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Categorization Rules
CATEGORY_PATTERNS = {
    'Income': re.compile(r'PAYCHECK|SALARY|DEPOSIT.*EMPLOYER|DD\s*-|DIRECT\s*DEPOSIT', re.IGNORECASE),
    'Food & Dining': re.compile(r'STARBUCKS|SBX|CHIPOTLE|7-ELEVEN|7-11|COFFEE|RESTAURANT|SAFA|CHOWMEIN|KHANA', re.IGNORECASE),
    'Entertainment': re.compile(r'NETFLIX|SPOTIFY|HULU|DISNEY', re.IGNORECASE),
    'Transportation': re.compile(r'UBER|LYFT|SHELL|FUEL|GAS|INDRIVE', re.IGNORECASE),
    'Groceries': re.compile(r'WHOLE\s*FOODS|WFM|TRADER\s*JOE|COSTCO', re.IGNORECASE),
    'Bills': re.compile(r'RENT|LANDLORD|UTILITIES|ELECTRIC|WATER', re.IGNORECASE),
    'Shopping': re.compile(r'AMZN|AMAZON.*|TARGET|WALMART', re.IGNORECASE),
}

# CSV Settings
DATE_FORMAT = "%m/%d/%Y"
EXPECTED_COLUMNS = ['date', 'description', 'category', 'transaction_type', 'amount']
