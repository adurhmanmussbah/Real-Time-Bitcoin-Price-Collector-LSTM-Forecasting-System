
import os
import pyodbc
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping


# -----------------------
# Config
# -----------------------
SQL_CONN_STR = os.getenv('SQL_CONN_STR') or (
"DRIVER={ODBC Driver 17 for SQL Server};"
"SERVER=Joba;"
"DATABASE=CryptoDB;"
"UID=sa;"
"PWD=P@$$w0rd;"
)
MODEL_DIR = os.getenv('MODEL_DIR') or r"C:\btc_project\models"
os.makedirs(MODEL_DIR, exist_ok=True)


# Training window (example: last 7 days)
END_TS = pd.to_datetime('now', utc=True)
START_TS = END_TS - pd.Timedelta(days=7)

def fetch_data(start_ts, end_ts):
    conn = pyodbc.connect(SQL_CONN_STR)
    cursor = conn.cursor()
    rows = cursor.execute("EXEC dbo.GetTrainingData ?, ?", start_ts.to_pydatetime(), end_ts.to_pydatetime()).fetchall()
    cols = [column[0] for column in cursor.description]
    df = pd.DataFrame.from_records(rows, columns=cols)
    conn.close()
    return df


def build_sequences(df, feature_cols, target_col='price', seq_len=60):
    data = df[feature_cols].values
    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(data)


    X, y = [], []
    for i in range(seq_len, len(data_scaled)):
        X.append(data_scaled[i-seq_len:i])
        y.append(data_scaled[i, feature_cols.index(target_col)])
    X, y = np.array(X), np.array(y)
    return X, y, scaler

def build_model(input_shape):
    model = Sequential()
    model.add(LSTM(128, input_shape=input_shape, return_sequences=True))
    model.add(Dropout(0.2))
    model.add(LSTM(64, return_sequences=False))
    model.add(Dropout(0.2))
    model.add(Dense(32, activation='relu'))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse')
    return model


# -----------------------
# Run training
# -----------------------
if __name__ == '__main__':
    print('Fetching data...')
    df = fetch_data(START_TS, END_TS)
    if df.empty:
        raise SystemExit('No data returned from DB. Increase time window or check features.')


    # convert timestamp to datetime
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)
    df.sort_values('timestamp_utc', inplace=True)
    df.reset_index(drop=True, inplace=True)


    # Choose features
    feature_cols = ['price', 'ma_20', 'ma_50', 'rsi_14', 'vol_std_20']
    df = df[feature_cols].fillna(method='ffill').fillna(method='bfill')


    SEQ_LEN = 60 # number of ticks in sequence (e.g., last 60 * 5s ~= 5 minutes)


    X, y, scaler = build_sequences(df, feature_cols, target_col='price', seq_len=SEQ_LEN)


    # Train / val split
    split = int(0.9 * len(X))
    X_train, X_val = X[:split], X[split:]
    y_train, y_val = y[:split], y[split:]

    print("Building model...")
    model = build_model((SEQ_LEN, len(feature_cols)))

    # Callbacks
    checkpoint_path = os.path.join(MODEL_DIR, "btc_lstm_model.h5")
    checkpoint = ModelCheckpoint(
        checkpoint_path,
        monitor='val_loss',
        save_best_only=True,
        mode='min',
        verbose=1
    )
    early_stop = EarlyStopping(
        monitor='val_loss',
        patience=10,
        restore_best_weights=True
    )

    print("Training...")
    history = model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=25,
        batch_size=32,
        callbacks=[checkpoint, early_stop]
    )

    # Save scaler
    import joblib
    scaler_path = os.path.join(MODEL_DIR, "scaler.pkl")
    joblib.dump(scaler, scaler_path)

    print(f"Model saved to: {checkpoint_path}")
    print(f"Scaler saved to: {scaler_path}")


    print("Training completed!")
