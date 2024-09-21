import requests
import pandas as pd
from sqlalchemy import create_engine

# Function to fetch stock data
def fetch_stock_data(symbol, api_key):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={api_key}'
    response = requests.get(url)
    data = response.json()

    # Check if the API returned an error
    if "Error Message" in data:
        print("Error fetching data. Check API key or symbol.")
        return pd.DataFrame()

    # Parse stock data
    time_series = data.get('Time Series (1min)', {})
    df = pd.DataFrame.from_dict(time_series, orient='index')
    df = df.rename(columns={
        '1. open': 'open',
        '2. high': 'high',
        '3. low': 'low',
        '4. close': 'close',
        '5. volume': 'volume'
    })
    df.index.name = 'timestamp'

    return df

# Function to store stock data into the SQLite database
def store_data_to_db(df, db_url):
    if not df.empty:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            df.to_sql('stock_prices', conn, if_exists='replace', index=False)
            print("Data successfully stored in the SQLite database")
    else:
        print("No data to store in the database.")
