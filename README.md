# Real-Time-Bitcoin-Price-Collector-LSTM-Forecasting-System
This project is a fully automated real-time cryptocurrency data collection and forecasting system built using Python, SQL Server, and Deep Learning (LSTM).




          ┌───────────────────────────┐
          │     Binance API (Live)    │
          └─────────────┬─────────────┘
                        ▼
        ┌──────────────────────────────────┐
        │   Python Collector Script        │
        │  - Fetches BTC Price             │
        │  - Sends to SQL Stored Procedure │
        │  - Logs activity/errors          │
        └─────────────┬────────────────────┘
                      ▼
        ┌──────────────────────────────────┐
        │        SQL Server Database       │
        │  - Raw Price Table               │
        │  - Features Table                │
        │  - Indicator Calculations (SP)   │
        └─────────────┬────────────────────┘
                      ▼
        ┌──────────────────────────────────┐
        │    LSTM Training Pipeline        │
        │  - Windowing & Scaling           │
        │  - Model Training                │
        │  - Save model & scaler           │
        └─────────────┬────────────────────┘
                      ▼
        ┌──────────────────────────────────┐
        │     Prediction Engine            │
        │  - Loads latest indicators       │
        │  - Generates BTC prediction      │
        │  - Logs & saves forecast         │
        └──────────────────────────────────┘

