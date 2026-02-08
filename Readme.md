# 💰 Personal Finance ETL Pipeline & Dashboard

A complete end-to-end data pipeline for analyzing personal bank statements. Automatically extracts, transforms, and loads transaction data into PostgreSQL, with an interactive Streamlit dashboard for visualization.

## 🎯 Features

- **🔄 Automated ETL Pipeline**: Drop CSV files, watch them auto-process
- **🧹 Smart Data Cleaning**: Automatic categorization of transactions
- **💾 PostgreSQL Storage**: Persistent database with optimized queries
- **📊 Interactive Dashboard**: Beautiful visualizations with Streamlit
- **🎨 Rich Analytics**: Spending breakdowns, trends, and insights
- **🔍 Advanced Filtering**: Filter by date, category, transaction type
- **📥 Data Export**: Download filtered data as CSV

## 🏗️ Architecture

```
CSV Files  →  File Watcher  →  Transform  →  PostgreSQL  →  Dashboard
(Bank          (watch.py)      (Pandas)       (Docker)      (Streamlit)
Statements)         ↓              ↓              ↓             ↓
                 Detects      Categorize     Store Data    Visualize
                 New Files    Clean Data     Optimize      Analyze
                              Validate       Index         Export
```

## 📋 Prerequisites

- **Python 3.8+** - [Download](https://www.python.org/downloads/)
- **Docker Desktop** - [Download](https://www.docker.com/products/docker-desktop/)
- **Git** (for cloning) - [Download](https://git-scm.com/downloads)

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/finance-etl-pipeline.git
cd finance-etl-pipeline
```

### 2. Set Up Python Environment

```bash
# Create virtual environment
python -m venv venv

# Activate it
# On Linux/Mac:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Environment Variables (Optional)

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your settings if needed
nano .env
```

### 4. Start PostgreSQL Database

```bash
# Start Docker container
docker compose up -d

# Wait for initialization (first time only)
sleep 15

# Verify database is running
python test_database.py
```

### 5. Start the File Watcher (Terminal 1)

```bash
python watch.py
```

### 6. Start the Dashboard (Terminal 2)

```bash
streamlit run dashboard.py
```

Dashboard opens at: **http://localhost:8501**

### 7. Process Your First CSV

```bash
# Drop a CSV file
cp sample_data/MOCK_DATA.csv finance/watch/
```

## 📂 Project Structure

```
finance-etl-pipeline/
├── config.py              # Configuration & categorization rules
├── transform.py           # Data cleaning & transformation logic
├── database.py            # PostgreSQL connection & operations
├── watch.py               # File watcher (main ETL orchestrator)
├── dashboard.py           # Streamlit visualization app
├── test_database.py       # Database connection testing
├── docker-compose.yml     # PostgreSQL container setup
├── init.sql               # Database schema initialization
├── requirements.txt       # Python dependencies
├── .env.example           # Environment variables template
├── .gitignore            # Git ignore rules
├── README.md             # This file
│
├── finance/
│   ├── watch/            # Drop CSV files here
│   ├── processed/        # Successfully processed files
│   └── failed/           # Failed files with error logs
│
└── sample_data/          # Example CSV files
    └── MOCK_DATA.csv
```

## 📊 CSV File Format

Your CSV must have these columns:

| Column             | Description                    | Example              |
|--------------------|--------------------------------|----------------------|
| `date`             | Transaction date (MM/DD/YYYY)  | 01/15/2024           |
| `description`      | Transaction description        | STARBUCKS STORE #123 |
| `category`         | Original category (optional)   | Dining               |
| `transaction_type` | Original type (optional)       | Debit                |
| `amount`           | Transaction amount             | -5.75                |

## 🎨 Customizing Categories

Edit `config.py`:

```python
CATEGORY_PATTERNS = {
    'Income': re.compile(r'PAYCHECK|SALARY', re.IGNORECASE),
    'Food & Dining': re.compile(r'STARBUCKS|CHIPOTLE', re.IGNORECASE),
    'Your Category': re.compile(r'KEYWORD1|KEYWORD2', re.IGNORECASE),
}
```

## 🗄️ Database Operations

```bash
# Connect to PostgreSQL
docker exec -it finance_db psql -U postgres -d finance_db

# Query data
SELECT category, COUNT(*), SUM(amount) FROM transactions GROUP BY category;

# Exit
\q
```

## 🧪 Testing

```bash
# Test database
python test_database.py

# Test transformation
python test_manual.py
```

## 🐛 Troubleshooting

**"Cannot connect to PostgreSQL"**
```bash
docker compose ps
docker compose restart
```

**"No transactions in dashboard"**
```bash
docker exec -it finance_db psql -U postgres -d finance_db -c "SELECT COUNT(*) FROM transactions;"
```

**Dashboard not updating**
- Click "Rerun" (top-right)
- Press `R` key

## 🔒 Security Notes

⚠️ **For local development only** - Uses simplified security. For production:
- Use environment variables
- Enable SSL
- Implement authentication

## 📝 License

MIT License - See [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Streamlit](https://streamlit.io/)
- [PostgreSQL](https://www.postgresql.org/)
- [Plotly](https://plotly.com/)
- [Pandas](https://pandas.pydata.org/)

---

**Made with ❤️ for better financial insights**
