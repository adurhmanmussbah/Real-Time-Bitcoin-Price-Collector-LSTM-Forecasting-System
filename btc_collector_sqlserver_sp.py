import os
import time
import logging
from datetime import datetime
import requests
import pyodbc


SQL_CONN_STR = os.getenv('SQL_CONN_STR') or (
"DRIVER={ODBC Driver 17 for SQL Server};"
"SERVER=Joba;"
"DATABASE=CryptoDB;"
"UID=sa;"
"PWD=P@$$w0rd;"
)
SLEEP_SECONDS = 5
API_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
LOG_FILE = os.getenv('LOG_FILE') or r"C:\btc_project\collector.log"

log_dir = os.path.dirname(LOG_FILE)
os.makedirs(log_dir, exist_ok=True)

# -----------------------
# Logging setup
# -----------------------
logger = logging.getLogger('btc_collector')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)




def get_db_conn():
    return pyodbc.connect(SQL_CONN_STR, timeout=5)

# -----------------------
# Fetch price
# -----------------------
def get_btc_price():
    r = requests.get(API_URL, timeout=5)
    r.raise_for_status()
    return float(r.json()['price'])


def main():
    logger.info('Starting BTC collector')
    conn = None
    while True:
        try:
            price = get_btc_price()
            ts = datetime.utcnow().replace(microsecond=0) # match DATETIME2(3)

            if conn is None:
                conn = get_db_conn()
            cursor = conn.cursor()


            # Call stored procedure
            cursor.execute("EXEC dbo.InsertBTCPrice ?", price)
            conn.commit()


            logger.info(f"Inserted price {price} at {ts}")


        except Exception as e:
            logger.exception('Error in main loop')
    # try to close and reset connection
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None


        time.sleep(SLEEP_SECONDS)

if __name__ == '__main__':
    main()