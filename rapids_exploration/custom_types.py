from typing import Union

import pandas as pd
import polars as pl

DataFrameType = Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame]
