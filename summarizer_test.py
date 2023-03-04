from summarizer_utils import *
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
    assert correlation(df, "a", "b") == 0.0
    assert correlation(df, "a", "c") == 1.0

def test_():
    test_correlation()

test_()

