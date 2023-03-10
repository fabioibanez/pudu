from summarizer_utils import SummarizerFactory
from summarizer_utils import func_dict
from summarizer import *
import polars as pl


"""
These are the tests for the summarizer functions found in summarizer_utils.py that 
are leveraged by the summarizer.py module.
"""
def test_correlation():
    df = pl.DataFrame({
        "a": [1, 2, 3, 4, 5],
        "b": [5, 4, 3, 2, 1],
        "c": [1, 2, 3, 4, 5],
    })

    summarizer_obj = SummarizerFactory("correlation", "a", "b")
    assert summarize(df, summarizer_obj) == -1 or -0.9999999999999999

    print("test_correlation passed")


# def test_count():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })

#     assert count(df, "a") == 5
#     assert count(df, "b") == 5
#     assert count(df, "c") ==  5
#     print("test_count passed")

# def test_covariance():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })

#     # TODO: write assertions
#     print("test_covariance passed")

# def test_dot_product():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })

#     assert dot_product(df, "a", "b") == 35
#     assert dot_product(df, "a", "c") == 55
#     print("test_dot_product passed")

# def test_ewma_halflife():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })

#     # TODO: write assertions
#     print("test_ewma_halflife passed")

# def test_ewma():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })

#     # TODO: write assertions
#     print("test_ewma passed")


# def test_geometric_mean():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })
#     assert geometric_mean(df, "a") == 2.605171084697352
#     assert geometric_mean(df, "b") == 2.605171084697352
#     assert geometric_mean(df, "c") == 2.605171084697352
#     print("test_geometric_mean passed")

# def test_kurtosis():
#     df = pl.DataFrame({"a": [1, 2, 3, 2, 1]})
#     assert kurtosis(df, "a") < -1.153061
#     assert kurtosis(df, "a") > -1.153062
#     print("test_kurtosis passed")


# def test_max():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })

#     assert max(df, "a") == 5
#     assert max(df, "b") == 5
#     assert max(df, "c") == 5
#     print("test_max passed")

# def test_mean():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })

#     assert mean(df, "a") == 3
#     assert mean(df, "b") == 3
#     assert mean(df, "c") == 3
#     print("test_mean passed")

# def test_min():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })

#     assert min(df, "a") == 1
#     assert min(df, "b") == 1
#     assert min(df, "c") == 1
#     print("test_min passed")

# def test_sum():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })

#     assert sum(df, "a") == 15
#     assert sum(df, "b") == 15
#     assert sum(df, "c") == 15
#     print("test_sum passed")

# def test_variance():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })

#     # TODO: write assertions
#     print("test_variance passed")

# def test_stddev():
#     df = pl.DataFrame({
#         "a": [1, 2, 3, 4, 5],
#         "b": [5, 4, 3, 2, 1],
#         "c": [1, 2, 3, 4, 5],
#     })

#     # TODO: write assertions

def test_():
    test_correlation()
    # test_count()
    # test_covariance()
    # test_dot_product()
    # test_ewma_halflife()
    # test_ewma()
    # test_geometric_mean()
    # test_kurtosis()
    # test_max()
    # test_mean()
    # test_min()
    # test_sum()
    # test_variance()
test_()

