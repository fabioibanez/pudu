import polars as pl
from summarizer_utils import *

# TODO: Where do I check for the validity of the input arguments?
def summarize(df, SummarizerFactory):
    """
    This function takes a polars DataFrame, and a SummarizerFactory object, and returns
    a polars DataFrame with the summarizer function applied to the DataFrame.
    """
    return SummarizerFactory.__call__(df, *SummarizerFactory.args)



    
    

