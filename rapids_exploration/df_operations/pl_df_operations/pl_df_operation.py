from abc import ABC, abstractmethod

import polars as pl

from rapids_exploration.df_operations.df_operation import DFOperation


class PlDfOperation(DFOperation, ABC):
    @abstractmethod
    def execute_on(self, df: pl.DataFrame) -> pl.DataFrame:
        pass
