"""
This is the file that contains all of the implementations of the summarizers
"""

import polars as pl
from summarizer import *

# these are the supported summarizer functions that are implemented in polars
__all__ = [
    'SummarizerFactory',
    'correlation',
    'count',
    'covariance',
    'dot_product',
    'ema_halflife',
    'ewma',
    'geometric_mean',
    'kurtosis',
    'linear_regression',
    'max',
    'mean',
    'min',
    'nth_central_moment',
    'nth_moment',
    'product',
    'quantile',
    'skewness',
    'stddev',
    'sum',
    'variance',
    'weighted_correlation',
    'weighted_covariance',
    'weighted_mean',
    'zscore',
]

def correlation(df, column, other_column):
    """
    Calculates pairwise correlation between columns.

    Example:
        >>> df = pl.DataFrame({
        ...     "a": [1, 2, 3, 4, 5],
        ...     "b": [5, 4, 3, 2, 1],
        ...     "c": [1, 2, 3, 4, 5],
        ... })
        >>> df.corr("a", "b")
        0.0
        >>> df.corr("a", "c")
        1.0
    """
    return df.select(pl.corr("a", "b"))

def count(df, column):
    """
    Counts the number of non-null values in a column.
    """
    return df.count()

def covariance(df, column, other_column):
    """
    Calculates the covariance between two columns.
    """
    return df.cov(column, other_column)

def dot_product(df, column, other_column):
    """
    Calculates the dot product between two columns.
    """
    return df.dot(column, other_column)

def ema_halflife(df, column, halflife):
    """
    Calculates the exponential moving average of a column.
    """
    return df.ewm(halflife=halflife).mean()

def ewma(df, column, alpha):
    """
    Calculates the exponential moving average of a column.
    """
    return df.ewm(alpha=alpha).mean()

def geometric_mean(df, column):
    """
    Calculates the geometric mean of a column.
    """
    return df.geometric_mean()

def kurtosis(df, column):
    """
    Calculates the kurtosis of a column.
    """
    return df.kurtosis()

def linear_regression(df, column, other_column):
    """
    Calculates the linear regression of a column.
    """
    return df.linreg(column, other_column)

def max(df, column):
    """
    Calculates the maximum value of a column.
    """
    return df.max()

def mean(df, column):
    """
    Calculates the mean of a column.
    """
    return df.mean()

def min(df, column):
    """
    Calculates the minimum value of a column.
    """
    return df.min()

def nth_central_moment(df, column, n):
    """
    Calculates the nth central moment of a column.
    """
    return df.nth_central_moment(n)

def nth_moment(df, column, n):
    """
    Calculates the nth moment of a column.
    """
    return df.nth_moment(n)

def product(df, column):
    """
    Calculates the product of a column.
    """
    return df.product()

def quantile(df, column, quantile):
    """
    Calculates the quantile of a column.
    """
    return df.quantile(quantile)

def skewness(df, column):
    """
    Calculates the skewness of a column.
    """
    return df.skewness()

def stddev(df, column):
    """
    Calculates the standard deviation of a column.
    """
    return df.std()

def sum(df, column):
    """
    Calculates the sum of a column.
    """
    return df.sum()

def variance(df, column):
    """
    Calculates the variance of a column.
    """
    return df.var()

def weighted_correlation(df, column, other_column, weight_column):
    """
    Calculates the Pearson correlation coefficient between two columns.
    """
    return df.corr(column, other_column, weight_column)

def weighted_covariance(df, column, other_column, weight_column):
    """
    Calculates the covariance between two columns.
    """
    return df.cov(column, other_column, weight_column)

def weighted_mean(df, column, weight_column):
    """
    Calculates the mean of a column.
    """
    return df.mean(weight_column)

def zscore(df, column):
    """
    Calculates the zscore of a column.
    """
    pass


