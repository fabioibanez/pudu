from pyquokka.df import *


def quokka_window_test():
    cluster = LocalCluster()
    qc = QuokkaContext(cluster, 4, 2)

    quotes = qc.read_sorted_csv(
        "/Users/fabio/Desktop/quotes.csv", "time", has_header=True)
    # trades = qc.read_sorted_csv("/home/ziheng/tpc-h/trades.csv", "time", has_header = True)

    window = SlidingWindow("time", "symbol", size_before=100000, aggregation_dict={
                           "avg_bid": "AVG(bid)"})
    trigger = OnEventTrigger()

    quotes = quotes.filter("ask > 1")
    windowed_quotes = quotes.windowed_transform(window, trigger)
    result = windowed_quotes.collect()
    print(result)


quokka_window_test()
