
"""
This module contains the summarizer factory and all the summarizers for
quokka. The summarizers are functions that take a list of values and return a
single value. These summarizers are meant to be used by the groupby function 
to aggregate data and return desired feature engineering results.
"""

import polars as pl

# these are the supported summarizer functions that are implemented in polars
__all__ = [
    'correlation',
    'count',
    'covariance',
    'dot_product',
    'ewma_halflife',
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

class SummarizerFactory:
    def __init__(self, func, *args):
        self.func = func_dict[func]
        self.args = args

    def __call__(self, df, *args):
        return self.func(df, *self.args)

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

        out = df.select(pl.corr(column, other_column))
        return out[0, 0]

    def count(df, col):
        """
        Counts the number of non-null values in a column.
        """
        out = df.select(pl.col(col).count().alias("count"))
        return out[0,0]
        

    # weird behaviour
    def covariance(df, column, other_column):
        """
        Calculates the covariance between two columns.
        """
        out = df.select(pl.cov(column, other_column))
        print(out[0, 0])
        return out[0, 0]

    def dot_product(df, column, other_column):
        """
        Calculates the dot product between two columns.
        """
        out = df.select(pl.col(column).dot(pl.col(other_column)).alias("dot_product"))
        return out[0, 0]

    # TODO: should support min_periods: minimum number of observations in window required to have a value (otherwise result is null)
    def ewma_halflife(df, column, halflife):
        """
        Calculates the exponential moving average of a column.
        """
        out = df.select(pl.col(column).ewm_mean(half_life = halflife).alias("ewma"))
        return out

    def ewma(df, column, alpha):
        """
        Calculates the exponential moving average of a column.
        """
        out = df.select(pl.col(column).ewm_mean(alpha = alpha).alias("ewma"))
        print(out)

    def geometric_mean(df, column):
        """
        Calculates the geometric mean of a column.
        """
        out = df.select(pl.col(column).geometric_mean().alias("geometric_mean"))
        print(out)
        # return df.geometric_mean()

    def kurtosis(df, column):
        """
        Calculates the kurtosis of a column.
        """
        out = df.select(pl.col(column).kurtosis().alias("kurtosis"))
        return out[0, 0]

    def linear_regression(df, column, other_column):
        """
        Calculates the linear regression of a column.
        """
        return df.linreg(column, other_column)

    def max(df, column):
        """
        Calculates the maximum value of a column.
        """
        out = df.select(pl.col(column).max().alias("max"))
        return out[0,0]

    def mean(df, column):
        """
        Calculates the mean of a column.
        """
        out = df.select(pl.col(column).mean().alias("mean"))
        return out[0,0]

    def min(df, column):
        """
        Calculates the minimum value of a column.
        """
        out = df.select(pl.col(column).min().alias("min"))
        return out[0,0]


    def stddev(df, column):
        """
        Calculates the standard deviation of a column.
        """
        out = df.select(pl.col(column).std().alias("stddev"))
        return out[0,0]

    def sum(df, column):
        """
        Calculates the sum of a column.
        """
        out = df.select(pl.col(column).sum().alias("sum"))
        return out[0,0]

    def variance(df, column):
        """
        Calculates the variance of a column.
        """
        out = df.select(pl.col(column).var().alias("variance"))
        print(out)
        return out[0,0]

    #################### TODO: IMPLEMENT THESE ####################
    def nth_central_moment(df, column, n):
        """
        Calculates the nth central moment of a column.
        """
        pass

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
        pass

    def zscore(df, column):
        """
        Calculates the zscore of a column.
        """
        pass

func_dict = {
    "correlation": SummarizerFactory.correlation,
    "count": SummarizerFactory.count,
    "covariance": SummarizerFactory.covariance,
    "dot_product": SummarizerFactory.dot_product,
    "ewma_halflife": SummarizerFactory.ewma_halflife,
    "ewma": SummarizerFactory.ewma,
    "geometric_mean": SummarizerFactory.geometric_mean,
    "kurtosis": SummarizerFactory.kurtosis,
    "linear_regression": SummarizerFactory.linear_regression,
    "max": SummarizerFactory.max,
    "mean": SummarizerFactory.mean,
    "min": SummarizerFactory.min,
    "stddev": SummarizerFactory.stddev,
    "sum": SummarizerFactory.sum,
    "variance": SummarizerFactory.variance,
    "nth_central_moment": SummarizerFactory.nth_central_moment,
    "nth_moment": SummarizerFactory.nth_moment,
    "product": SummarizerFactory.product,
    "quantile": SummarizerFactory.quantile,
    "skewness": SummarizerFactory.skewness,
    "weighted_correlation": SummarizerFactory.weighted_correlation,
    "weighted_covariance": SummarizerFactory.weighted_covariance,
    "weighted_mean": SummarizerFactory.weighted_mean,
    "zscore": SummarizerFactory.zscore
}
