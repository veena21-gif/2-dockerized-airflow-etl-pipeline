import requests
import psycopg2
import os

def fetch_and_store_stock_data():
    try:
        api_key = os.getenv("API_KEY")
        symbol = "AAPL"

        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        response = requests.get(url).json()

        if "Time Series (Daily)" not in response:
            raise Exception("API returned no time series data")

        latest_date = list(response["Time Series (Daily)"].keys())[0]
        data = response["Time Series (Daily)"][latest_date]

        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                date DATE PRIMARY KEY,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT
            );
        """)

        cur.execute("""
            INSERT INTO stock_prices (date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """,
        (latest_date, data["1. open"], data["2. high"], data["3. low"], data["4. close"], data["5. volume"]))

        conn.commit()
        cur.close()
        conn.close()

        print("Inserted successfully")

    except Exception as e:
        print("Error:", e)
