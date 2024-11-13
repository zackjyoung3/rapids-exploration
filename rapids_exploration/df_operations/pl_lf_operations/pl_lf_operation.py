from abc import ABC, abstractmethod

import polars as pl

from rapids_exploration.df_operations.df_operation import DFOperation


class PlLfOperation(DFOperation, ABC):
    @abstractmethod
    def execute_on(self, df: pl.LazyFrame) -> pl.DataFrame:
        pass
