from abc import ABC, abstractmethod

from rapids_exploration.custom_types import DataFrameType


class DFOperation(ABC):
    @abstractmethod
    def execute_on(self, df: DataFrameType) -> DataFrameType:
        pass
