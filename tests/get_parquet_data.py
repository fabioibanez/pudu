import pandas_ta as ta
import pandas as pd

parquet_path = "/Users/fabio/Desktop/small.parquet"

def get_data():
    df = pd.read_parquet(parquet_path)
    for col in df.columns:
        print(col)
    return df
    # df['date'] = pd.to_datetime(df['date'])
    # df = df.set_index('date')
    # df = df.sort_index()
    # df = ta.bbands(df, length=20, std=2, mamode='sma')
    # df = df.dropna()
    # print(df)
    # return df

def pandas_ta_bollinger_bands():
    df = get_data()
    df.ta.bbands(length=20, std=2, mamode='sma')
    print(df)

pandas_ta_bollinger_bands()