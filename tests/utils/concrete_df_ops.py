import pandas as pd
import polars as pl

from rapids_exploration.custom_types import DataFrameType
from rapids_exploration.df_operations.df_operation import DFOperation
from rapids_exploration.df_operations.pd_df_operations.pd_df_operation import PdDfOperation
from rapids_exploration.df_operations.pl_df_operations.pl_df_operation import PlDfOperation
from rapids_exploration.df_operations.pl_lf_operations.pl_lf_operation import PlLfOperation


class PdDfNoOp(PdDfOperation):
    def execute_on(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


class PlDfNoOp(PlDfOperation):
    def execute_on(self, df: pl.DataFrame) -> pl.DataFrame:
        return df


class PlLfNoOp(PlLfOperation):
    def execute_on(self, lf: pl.LazyFrame) -> pl.DataFrame:
        return lf.collect()


class IncorrectSubclassOp(DFOperation):
    def execute_on(self, frame: DataFrameType) -> DataFrameType:
        return frame
