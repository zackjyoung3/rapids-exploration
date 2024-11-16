from collections import OrderedDict
from dataclasses import dataclass
from functools import cached_property
from typing import Any

from rapids_exploration.df_operations.df_operation import DFOperation


@dataclass(frozen=True)
class TestRes:
    operation_performed: DFOperation
    trials_run: int
    time_taken: float

    @cached_property
    def avg_time_taken(self) -> float:
        return self.time_taken / self.trials_run

    def as_ordered_dict(self) -> OrderedDict[str, Any]:
        """
        Return as ordered dict so that order of attributes and props consistent if ever
        desired to directly use this for purposes like building a table with results
        """
        return OrderedDict(
            (
                ("operation_performed", self.operation_performed),
                ("trials_run", self.trials_run),
                ("time_taken", self.time_taken),
                ("avg_time_taken", self.avg_time_taken),
            )
        )
