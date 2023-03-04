import lakeapi
import datetime
import pandas_ta as ta
import polars as pl
from pyquokka.df import *


def get_lakeapi_data():
    lakeapi.use_sample_data(anonymous_access=True)
    candles = lakeapi.load_data(
        table="candles",
        start=datetime.datetime(2022, 10, 1),
        end=datetime.datetime(2022, 10, 2),
        symbols=["BTC-USDT"],
        exchanges=["BINANCE"],
    )
    return candles

# pandas_ta bollinger bands


def pandas_ta_bollinger_bands():
    df = get_lakeapi_data()
    bbands_df = df.ta.bbands(length=20, std=2, mamode='sma')
    print(bbands_df.tail())


def polars_bollinger_bands():
    # makes a copy of the df to avoid mutating the original
    # bb_df = pl.df.clone()
    df = get_lakeapi_data()
    bb_df = pl.from_pandas(df)
    out = bb_df.groupby_rolling("origin_time", period="20m").agg([
        pl.col("close").mean().alias("bbands_mavg"),
        pl.col("close").std().alias("bbands_std"),
        # this is not working
        # pl.col("close").mean().alias("bbands_upper") + 2 * pl.col("close").std(),
        # pl.col("close").mean().alias("bbands_upper") - 2 * pl.col("close").std(),
    ])
    out = out.with_columns([
        (pl.col("bbands_mavg") + 2 * pl.col("bbands_std")).alias("bbands_upper"),
        (pl.col("bbands_mavg") - 2 * pl.col("bbands_std")).alias("bbands_lower"),
    ])
    print(out.tail())


def pandas_ta_sma():
    df = get_lakeapi_data()
    sma_df = df.ta.sma(length=20)
    print(sma_df)


def polars_sma():
    df = get_lakeapi_data()
    sma_df = pl.from_pandas(df)
    out = sma_df.groupby_rolling("origin_time", closed="left", period="20m").agg([
        pl.col("close").mean().alias("sma")
    ])
    print(out)


def pandas_ta_rsi():
    df = get_lakeapi_data()
    rsi_df = df.ta.rsi(length=14, append=True)
    print(rsi_df.tail())


def polars_rsi():
    df = get_lakeapi_data()
    rsi_df = pl.from_pandas(df)

    delta = rsi_df.select([
        pl.col("origin_time"),        
        pl.col("close").diff().alias("diff"),
    ])

    gain = delta.select([
        pl.col("origin_time"),
        pl.when(pl.col("diff") > 0).then(pl.col("diff")).otherwise(0).alias("gain")
    ])
    loss = delta.select([
        pl.col("origin_time"),
        pl.when(pl.col("diff") < 0).then(pl.col("diff")).otherwise(0).alias("loss")
    ])

    # this is the gain and loss df
    # rsi = gain + loss

    gain_gb = gain.groupby_rolling("origin_time", period="14m").agg([
        pl.col("gain").mean().alias("avg_gain"),
    ])

    loss_gb = loss.groupby_rolling("origin_time", period = "14m").agg([
        pl.col("loss").mean().alias("avg_loss"),
    ])

    out = gain_gb.join(loss_gb, on="origin_time", how="left")

    rsi = out.groupby_rolling("origin_time", period="14m").agg([
        (100 - (100 / (1 + (pl.col("avg_gain") / pl.col("avg_loss")))).alias("rsi"))
    ])

    print(rsi)


def polars_ema(df):
    rsi_df=pl.from_pandas(df)
    out=(rsi_df.select([
        # pl.all().exclude("ewm_col"),
        pl.col("close").ewm_mean(alpha=(2 / (1 + len(macd_df)))).over(
            "origin_time").alias("ewm_col")
    ]))
    print(out)
    return out



def quokka_sma():
    cluster=LocalCluster()

    qc=QuokkaContext(cluster, 4, 2)

    df=get_lakeapi_data()
    polars_df=pl.from_pandas(df)

    # write out to csv then read it back
    polars_df.write_csv("test.csv")

    candles=qc.read_sorted_csv(
        "test.csv", sorted_by = "origin_time", has_header = True)

    window=SlidingWindow("origin_time", "symbol", size_before = datetime.timedelta(minutes=40), aggregation_dict = {
        "avg_close": "AVG(close)"})

    trigger=OnEventTrigger()

    windowed_candles=candles.windowed_transform(window, trigger)
    result=windowed_candles.collect()
    print(result)


# this is the testing function
def main():
    # print("this is the pandas ta bollinger bands")
    # pandas_ta_bollinger_bands()

    # print("this is the polars implementation of bollinger bands")
    # polars_bollinger_bands()

    # print("this is the polars implementation of sma")
    # polars_sma()

    # print("this is the pandas ta implementation of sma")
    # pandas_ta_sma()

    print("this is the pandas ta implementation of rsi")
    pandas_ta_rsi()

    print("this is the polars rsi implementation")
    polars_rsi()

    # think about what you save, fix the quokka implementation
    # print("this is the polars sma implementation")
    # polars_sma()

    # print("this is quokka sma")
    # quokka_sma()

    # polars_MACD()


main()
