import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

def plot_stock_data(db_url):
    # Create a database engine
    engine = create_engine(db_url)

    # Read stock data from SQLite
    with engine.connect() as connection:
        df = pd.read_sql('SELECT * FROM stock_prices WHERE symbol = \'AAPL\'', connection)

    # Set the index to the timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)

    # Plot the closing price
    plt.figure(figsize=(14, 7))
    plt.plot(df.index, df['close'], label='Closing Price', color='blue')
    plt.xlabel('Timestamp')
    plt.ylabel('Price')
    plt.title('Stock Closing Price Over Time')
    plt.legend()
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    # SQLite database path
    db_url = 'sqlite:///stock_data.db'
    plot_stock_data(db_url)
