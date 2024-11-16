from time import perf_counter
from typing import List, Union

import numpy as np
import polars as pl

import rapids_exploration.constants as C
from rapids_exploration.custom_types import DataFrameType
from rapids_exploration.df_operations.df_operation import DFOperation
from rapids_exploration.df_operations.pd_df_operations.pd_df_operation import (
    PdDfOperation,
)
from rapids_exploration.df_operations.pl_df_operations.pl_df_operation import (
    PlDfOperation,
)
from rapids_exploration.df_operations.pl_lf_operations.pl_lf_operation import (
    PlLfOperation,
)
from rapids_exploration.testing_utils.test_res import TestRes


class OperationsPerformanceTester:
    def __init__(
        self, base_df: pl.DataFrame, num_trials: int = C.DEFAULT_TEST_TRIALS
    ) -> None:
        self._base_df = base_df
        self._num_trials = num_trials

    @property
    def base_df(self) -> pl.DataFrame:
        return self._base_df

    @property
    def num_trials(self) -> int:
        return self._num_trials

    def setup_df_for_operation(self, operation: DFOperation) -> DataFrameType:
        operation_type = type(operation)

        if issubclass(operation_type, PdDfOperation):
            return self.base_df.to_pandas()
        if issubclass(operation_type, PlDfOperation):
            # Polars DataFrames are immutable, thus, we can safely return the original
            return self.base_df
        if issubclass(operation_type, PlLfOperation):
            # Convert to LazyFrame for lazy operations
            return self.base_df.lazy()

        raise ValueError(f"Unsupported operation: {operation_type}")

    def time_operation(self, operation: DFOperation) -> float:
        df = self.setup_df_for_operation(operation)

        start = perf_counter()
        operation.execute_on(df)
        end = perf_counter()

        return end - start

    def _run_time_trial(self, operations: List[DFOperation]) -> np.ndarray:
        """Returns an array of execution times for each operation, maintaining 1-to-1 alignment with `operations`"""
        times = [self.time_operation(operation) for operation in operations]

        return np.array(times)

    def test(self, operations: Union[DFOperation, List[DFOperation]]) -> List[TestRes]:
        if isinstance(operations, DFOperation):
            operations = [operations]

        # array to accum total execution times for each operation, aligned with `operations` list
        total_exe_times = np.zeros(len(operations))
        for _ in range(self.num_trials):
            total_exe_times += self._run_time_trial(operations)

        return [
            TestRes(operation, self._num_trials, float(total_exe_time))
            for operation, total_exe_time
            in zip(operations, total_exe_times)
        ]
