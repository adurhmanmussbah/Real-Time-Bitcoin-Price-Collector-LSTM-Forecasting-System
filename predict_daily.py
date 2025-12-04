import pyodbc
import pandas as pd
import numpy as np
import joblib
from datetime import datetime, timedelta
from tensorflow.keras.models import load_model

# -----------------------------
# PATHS
# -----------------------------
MODEL_PATH = r"C:\btc_project\models\btc_lstm_model.h5"
SCALER_PATH = r"C:\btc_project\models\scaler.pkl"

# -----------------------------
# SQL CONNECTION
# -----------------------------
DB_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=Joba;"
    "Database=CryptoDB;"
    "Trusted_Connection=yes;"
)

def get_db_conn():
    return pyodbc.connect(DB_CONN_STR)

# -----------------------------
# LOAD DATA FROM DATABASE
# -----------------------------
def load_latest_data():
    conn = get_db_conn()
    query = """
        SELECT [timestamp_utc]
              ,[price]
              ,[ma_20]
              ,[ma_50]
              ,[rsi_14]
              ,[vol_std_20]
              ,[volume]
        FROM [CryptoDB].[dbo].[btc_features]
        ORDER BY [timestamp_utc] ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    return df

# -----------------------------
# PREPARE SEQUENCE FOR LSTM
# -----------------------------
def prepare_sequence(df, scaler, seq_len=60):
    feature_cols = ['price', 'ma_20', 'ma_50', 'rsi_14', 'vol_std_20']
    data = df[feature_cols].values
    
    scaled = scaler.transform(data)
    seq = scaled[-seq_len:]   
    return np.array(seq).reshape(1, seq_len, len(feature_cols))

# -----------------------------
# SAVE PREDICTED PRICE
# -----------------------------
def save_prediction(date_target, price_pred, pct_change):
    conn = get_db_conn()
    cursor = conn.cursor()

    cursor.execute("""
        MERGE BTC_DailyForecast AS target
        USING (SELECT ? AS ForecastDate) AS src
        ON target.ForecastDate = src.ForecastDate
        WHEN MATCHED THEN 
            UPDATE SET PredictedPrice=?, PredictedChangePercent=?, ModelUsed='LSTM'
        WHEN NOT MATCHED THEN
            INSERT (ForecastDate, PredictedPrice, PredictedChangePercent, ModelUsed)
            VALUES (?, ?, ?, 'LSTM');
    """, (date_target, price_pred, pct_change, date_target, price_pred, pct_change))

    conn.commit()
    conn.close()

# -----------------------------
# MAIN
# -----------------------------
def main():

    print("Loading scaler...")
    scaler = joblib.load(SCALER_PATH)

    print("Loading LSTM model...")
    model = load_model(MODEL_PATH, compile=False)   # <<< FIX FOR YOUR ERROR

    print("Loading BTC history from database...")
    df = load_latest_data()

    latest_price = df["price"].iloc[-1]

    X = prepare_sequence(df, scaler)

    # predict scaled value
    pred_scaled = model.predict(X)[0][0]

    # inverse scale - reconstruct full feature array with scaled prediction
    # We only predicted the 'price' feature (index 0), so keep other features as-is
    last_scaled_row = X[0][-1]  # Last row of features from input sequence
    pred_full = last_scaled_row.copy()
    pred_full[0] = pred_scaled  # Replace price with prediction
    predicted_price = scaler.inverse_transform([pred_full])[0][0]

    pct = ((predicted_price - latest_price) / latest_price) * 100

    # predict tomorrow
    target_date = (datetime.utcnow() + timedelta(days=1)).date()

    print(f"\nPrediction saved:")
    print(f"Date: {target_date}")
    print(f"Predicted Price: {predicted_price:.2f} USD")
    print(f"Change: {pct:.2f}%")

    save_prediction(target_date, float(predicted_price), float(pct))

if __name__ == "__main__":
    main()
