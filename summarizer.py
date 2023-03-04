"""
This module contains the summarizer factory and all the summarizers for
quokka. The summarizers are functions that take a list of values and return a
single value. These summarizers are meant to be used by the groupby function 
to aggregate data and return desired feature engineering results.
"""
import polars as pl
from summarizer import *

# also create the summarize function which takes in a SummarizerFactory, I believe

class SummarizerFactory:
    def __init__(self, func, *args):
        self.func = func
        self.args = args



# am i going to check upstream for the right parameters?
SummarizerFactory("ewma")

    

