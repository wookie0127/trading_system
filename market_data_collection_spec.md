
# Market Data Collection System Specification

## 1. Overview

This project collects market data for quantitative analysis and backtesting.

The system gathers:

- US market index data (NASDAQ)
- Korean market stocks (KOSPI200)
- Intraday price data (1-minute candles)
- Daily investor trading flows (foreign, institutional, individual)

The final dataset will support analysis of:

- foreign investor trading patterns
- market microstructure
- intraday price movement
- attention-driven trading strategies

---

# 2. Data Scope

## 2.1 US Market

### Target Index

One of the following:

- NASDAQ Composite (`^IXIC`)
- NASDAQ 100 (`^NDX`)

### Data Frequency

1 minute

### Fields

timestamp  
symbol  
open  
high  
low  
close  
volume  

---

## 2.2 Korean Market

### Target Universe

KOSPI200 components

The component list must be stored historically because index components change over time.

### Data Frequency

1 minute

### Fields

timestamp  
symbol  
open  
high  
low  
close  
volume  
trade_value  

---

## 2.3 Investor Trading Data

Investor groups:

- foreign investors
- institutional investors
- individual investors

### Frequency

Daily

### Fields

date  
symbol  

foreign_buy  
foreign_sell  
foreign_net  

institution_buy  
institution_sell  

individual_buy  
individual_sell  

volume  
trade_value  

---

# 3. Data Storage

## Storage Format

Parquet

Reasons:

- columnar format
- fast analytical queries
- compatible with pandas / duckdb / polars

---

## Directory Structure

market_data/

    us/
        nasdaq/
            1min/

    kr/
        kospi200/
            1min/

    kr/
        investor_flow/
            daily/

    metadata/
        kospi200_components/

---

# 4. Data Schema

## 4.1 Intraday Price Table

timestamp   datetime  
symbol      string  

open        float  
high        float  
low         float  
close       float  

volume      float  
trade_value float  

---

## 4.2 Investor Flow Table

date              date  
symbol            string  

foreign_buy       float  
foreign_sell      float  
foreign_net       float  

institution_buy   float  
institution_sell  float  

individual_buy    float  
individual_sell   float  

volume            float  
trade_value       float  

---

# 5. Metadata

## KOSPI200 Component History

Schema:

date  
symbol  
index_name  

Example:

2024-01-01 | 005930 | KOSPI200  
2024-01-01 | 000660 | KOSPI200  

---

# 6. Data Pipeline

## System Architecture

scheduler  
↓  
data collector  
↓  
raw parquet storage  
↓  
data validation  
↓  
analysis dataset  

---

# 7. Collection Workflow

## Step 1

Collect NASDAQ index intraday data.

Frequency:

1 minute

Store to:

market_data/us/nasdaq/1min/

---

## Step 2

Collect KOSPI200 component list.

Store to:

metadata/kospi200_components/

---

## Step 3

Collect KOSPI200 intraday price data.

Frequency:

1 minute

Store to:

market_data/kr/kospi200/1min/

---

## Step 4

Collect investor trading flow data.

Frequency:

daily

Store to:

market_data/kr/investor_flow/daily/

---

# 8. Expected Data Volume

## Per Day

200 stocks × 390 minutes ≈ 78,000 rows

## Per Year

≈ 20 million rows

## 5 Years

≈ 100 million rows

Estimated storage (Parquet):

5 ~ 10 GB

---

# 9. Data Joining Strategy

Investor data is daily while price data is intraday.

The daily investor flow will be joined to intraday data by date.

Example:

| timestamp | symbol | close | foreign_net |
|-----------|--------|------|-------------|
| 09:01 | 005930 | 72000 | -30000000000 |
| 09:02 | 005930 | 72100 | -30000000000 |

---

# 10. Data Validation

Each collection job must validate:

missing timestamps  
duplicate rows  
price <= 0  
volume < 0  

---

# 11. Derived Features

Foreign Flow Ratio

foreign_net / trade_value

Foreign Selling Pressure

foreign_sell / volume

Foreign Trading Share

foreign_trade_value / trade_value

---

# 12. Technology Stack

Python  
httpx  
pandas  
duckdb  
pyarrow  

Optional:

polars  
prefect  
airflow  

---

# 13. Scheduler

intraday price → every minute or after market close  
investor flow → once per day after market close  

Scheduler options:

cron  
prefect  
airflow  

---

# 14. Modules to Implement

nasdaq_collector.py  
kospi200_component_collector.py  
kospi200_intraday_collector.py  
investor_flow_collector.py  
parquet_writer.py  
data_validation.py  

All collectors should use async Python.

---

# 15. Future Extensions

Possible additional datasets:

- USD/KRW exchange rate
- oil price (WTI)
- S&P500 index
- option implied volatility
- news sentiment
