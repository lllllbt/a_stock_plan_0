# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.
所有的对话都需要用中文输出

在复杂任务的时候要用superpowers skills

## Project Overview

This is an A-share (Chinese stock market) daily K-line data fetching tool. It downloads historical daily trading data for all A-share stocks using the AKShare library and stores it in Parquet format with unadjusted prices plus adjustment factors for maximum flexibility.

**Key Design Decision**: The tool stores **unadjusted prices + adjustment factors** rather than pre-adjusted prices. This allows users to dynamically calculate forward-adjusted or backward-adjusted prices as needed for different use cases (backtesting vs. live trading).

## Development Environment Setup

### Virtual Environment (Required)

This system uses an externally-managed Python environment. Always use a virtual environment:

```bash
# Create virtual environment (if not exists)
python3 -m venv venv

# Install dependencies
./venv/bin/pip install -r requirements.txt

# Run scripts
./venv/bin/python3 fetch_a_stock_kline.py
./venv/bin/python3 verify_data.py
```

### Dependencies

- **akshare** (>=1.12.0): Data source for A-share market data
- **pandas** (>=2.0.0): Data processing (note: pandas 3.0+ compatibility required)
- **pyarrow** (>=12.0.0): Parquet file I/O
- **tqdm** (>=4.65.0): Progress bars

## Common Commands

### Data Fetching

```bash
# Test with single stock (recommended for testing)
./venv/bin/python3 fetch_a_stock_kline.py --symbols 000001 --start-date 19910101

# Test with limited stocks
./venv/bin/python3 fetch_a_stock_kline.py --limit 5 --start-date 20200101

# Download specific stocks
./venv/bin/python3 fetch_a_stock_kline.py --symbols 000001,000002,600000

# Incremental update (only fetch new data)
./venv/bin/python3 fetch_a_stock_kline.py --update

# Full download (all A-shares, takes hours)
./venv/bin/python3 fetch_a_stock_kline.py
```

### Data Verification

```bash
# Verify data quality and integrity
./venv/bin/python3 verify_data.py
```

### Quick Data Inspection

```bash
# View downloaded data
./venv/bin/python3 -c "
import pandas as pd
df = pd.read_parquet('data/kline_daily/000001.parquet')
print(df.info())
print(df.head())
"
```

## Architecture

### Data Flow

1. **Stock List Acquisition** (`get_stock_list()`):
   - Fetches all A-share stock codes from AKShare
   - Saves to `data/stock_list.parquet`

2. **K-line Data Fetching** (`fetch_kline_daily()`):
   - Fetches daily OHLCV data via `stock_zh_a_hist(period="daily", adjust="")`
   - Returns unadjusted OHLCV data
   - Much simpler than minute-level data (no resampling needed)

3. **Adjustment Factor Calculation** (`fetch_adj_factor()`):
   - Fetches both forward-adjusted (qfq) and unadjusted daily data
   - Calculates: `adj_factor = qfq_close / raw_close`
   - Direct 1:1 mapping (no need to align like minute data)

4. **Data Merging**:
   - Merges daily bars with adjustment factors by date
   - Forward-fills missing factors, defaults to 1.0 if unavailable

5. **Storage**:
   - One Parquet file per stock: `data/kline_daily/{symbol}.parquet`
   - Columnar format for efficient querying

### Key Modules

**fetch_a_stock_kline.py**:
- `ensure_directories()`: Creates data directories
- `retry_on_error()`: Decorator for network retry logic (3 attempts, exponential backoff)
- `get_stock_list()`: Stock list acquisition
- `fetch_adj_factor()`: Adjustment factor calculation
- `fetch_kline_daily()`: Main data fetching logic
- `save_to_parquet()`: Incremental update support
- `main()`: CLI argument parsing and orchestration

**verify_data.py**:
- `verify_stock_list()`: Checks stock list file
- `verify_kline_data()`: Validates OHLCV data quality (price consistency, no negative volumes)
- `verify_all_kline_files()`: Batch verification and statistics

### Data Schema

Each stock's Parquet file contains:

| Column | Type | Description |
|--------|------|-------------|
| date | datetime64 | Trading date |
| symbol | str | Stock code (e.g., "000001") |
| open | float64 | Unadjusted open price |
| high | float64 | Unadjusted high price |
| low | float64 | Unadjusted low price |
| close | float64 | Unadjusted close price |
| volume | int64 | Trading volume |
| amount | float64 | Trading amount (CNY) |
| adj_factor | float64 | Adjustment factor for dividends/splits |

**To calculate adjusted prices**:
```python
adj_close = close * adj_factor
```

## Critical Implementation Details

### Daily Data Advantages

**Compared to minute-level data, daily data is:**
- **Simpler**: No column mismatch issues (all periods return same columns)
- **More complete**: Historical data goes back to 1991 (vs. ~1.5 months for minute data)
- **More reliable**: Fewer API failures, more stable data source
- **Faster**: Fewer records to download and process

### Pandas 3.0 Compatibility

**IMPORTANT**: Pandas 3.0+ deprecated `fillna(method='ffill')`. Use `ffill()` directly:

```python
# Correct (pandas 3.0+)
df['adj_factor'] = df['adj_factor'].ffill()
df['adj_factor'] = df['adj_factor'].fillna(1.0)

# Incorrect (deprecated)
df['adj_factor'].fillna(method='ffill', inplace=True)
```

### Incremental Update Logic

When `--update` flag is used:
1. Check if stock's Parquet file exists
2. Read existing data and find latest timestamp
3. Only fetch data after latest timestamp
4. Append new data to existing file

This significantly reduces download time for regular updates.

### Error Handling Strategy

- **Network errors**: 3 retries with exponential backoff (0.5s, 1s, 1.5s)
- **Missing data**: Logs failure but continues with other stocks
- **Missing adj_factor**: Defaults to 1.0 with warning
- **API rate limiting**: 0.5s delay between requests

## Testing Workflow

1. **Test single stock first**:
   ```bash
   ./venv/bin/python3 fetch_a_stock_kline.py --symbols 000001 --start-date 19910101
   ```

2. **Verify data quality**:
   ```bash
   ./venv/bin/python3 verify_data.py
   ```

3. **Check data manually**:
   ```python
   import pandas as pd
   df = pd.read_parquet('data/kline_daily/000001.parquet')
   assert len(df) > 0
   assert df['adj_factor'].min() > 0
   assert (df['high'] >= df['low']).all()
   ```

4. **Test incremental update**:
   ```bash
   ./venv/bin/python3 fetch_a_stock_kline.py --symbols 000001 --update
   ```

## Common Issues

### "Length mismatch: Expected axis has 11 elements, new values have 7 elements"

This was an issue with minute-level data. Daily data does not have this problem as all columns are consistent.

### "NDFrame.fillna() got an unexpected keyword argument 'method'"

Pandas 3.0 compatibility issue. See "Pandas 3.0 Compatibility" section above.

### "No module named 'akshare'"

Must use virtual environment. See "Development Environment Setup" section.

### Data source temporarily unavailable

AKShare depends on third-party sources (e.g., East Money). If requests fail:
- Wait and retry later
- Check network connectivity
- Verify AKShare is up-to-date: `./venv/bin/pip install --upgrade akshare`

## Data Usage Examples

### Calculate Adjusted Prices

```python
import pandas as pd

df = pd.read_parquet('data/kline_daily/000001.parquet')

# Backward-adjusted prices (recommended for backtesting)
df['adj_open'] = df['open'] * df['adj_factor']
df['adj_high'] = df['high'] * df['adj_factor']
df['adj_low'] = df['low'] * df['adj_factor']
df['adj_close'] = df['close'] * df['adj_factor']

# Calculate returns
df['returns'] = df['adj_close'].pct_change()
```

### Load Multiple Stocks

```python
import pandas as pd
from pathlib import Path

dfs = []
for file in Path('data/kline_daily').glob('*.parquet'):
    dfs.append(pd.read_parquet(file))

df_all = pd.concat(dfs, ignore_index=True)
```

## Performance Considerations

- **Full download**: ~5000 stocks × ~35 years of daily data = several hours
- **Storage**: ~1-5MB per stock, total ~5-25GB
- **Incremental updates**: Much faster, only fetches new data
- **Parquet benefits**: Columnar storage, efficient compression, fast column-wise queries
- **Historical coverage**: Daily data from 1991 to present (~8000+ trading days)

## Modifying the Code

### Adding New Data Fields

If AKShare adds new fields you want to preserve:

1. Update column selection in `fetch_kline_daily()`
2. Update column list in final selection
3. Update schema documentation in this file
4. Update `verify_data.py` validation logic

### Supporting Different Time Periods

To add minute-level support (5/15/30/60 minutes):

1. Create separate script (e.g., `fetch_a_stock_kline_5m.py`)
2. Add new `KLINE_DIR` constant (e.g., `KLINE_5M_DIR`)
3. Use `stock_zh_a_hist_min_em()` API
4. Handle column mismatch issues (see backup script)
5. Note: Minute data only has ~1.5 months history

### Changing Adjustment Method

Currently uses forward-adjustment (qfq). To use backward-adjustment (hfq):

1. Change `adjust="qfq"` to `adjust="hfq"` in `fetch_adj_factor()` (line 100)
2. Update documentation to reflect the change
3. Note: This affects all historical calculations
