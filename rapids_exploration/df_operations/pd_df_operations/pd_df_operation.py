from abc import ABC, abstractmethod

import pandas as pd

from rapids_exploration.df_operations.df_operation import DFOperation


class PdDfOperation(DFOperation, ABC):
    @abstractmethod
    def execute_on(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
