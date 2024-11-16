from dataclasses import dataclass
from typing import Iterable, List, Optional, Type, Union

import pandas as pd
import pandas.testing as pdt
import polars as pl
import polars.testing as plt
import pytest

import rapids_exploration.constants as C
from rapids_exploration.df_operations.df_operation import DFOperation
from rapids_exploration.testing_utils.test_res import TestRes
from rapids_exploration.testing_utils.perf_tester import OperationsPerformanceTester
from tests.utils.concrete_df_ops import IncorrectSubclassOp, PdDfNoOp, PlDfNoOp, PlLfNoOp


def test_init_no_num_trials_results_in_default(base_df):
    expected_num_trials = C.DEFAULT_TEST_TRIALS

    perf_tester = OperationsPerformanceTester(base_df)

    assert perf_tester.base_df.equals(base_df)
    assert perf_tester.num_trials == expected_num_trials


def test_init_num_trials_provided(base_df):
    trials_passed = 30

    perf_tester = OperationsPerformanceTester(base_df, trials_passed)

    assert perf_tester.base_df.equals(base_df)
    assert perf_tester.num_trials == trials_passed


def test_setup_df_for_operation_pd_op_returns_pd_df(base_df):
    expected_df = base_df.to_pandas()
    perf_tester = OperationsPerformanceTester(base_df)
    pd_df_no_op = PdDfNoOp()

    setup_df = perf_tester.setup_df_for_operation(pd_df_no_op)

    assert isinstance(setup_df, pd.DataFrame)
    pdt.assert_frame_equal(setup_df, expected_df)


def test_setup_df_for_operation_pl_df_op_returns_base_pl_df(base_df):
    expected_df = base_df
    perf_tester = OperationsPerformanceTester(base_df)
    pl_df_no_op = PlDfNoOp()

    setup_df = perf_tester.setup_df_for_operation(pl_df_no_op)

    # since dfs are immutable, we expect base df instance to be returned
    assert setup_df is expected_df


def test_setup_df_for_operation_pl_lf_op_returns_pl_lf(base_df):
    expected_df = base_df.lazy()
    perf_tester = OperationsPerformanceTester(base_df)
    pl_lf_no_op = PlLfNoOp()

    setup_df = perf_tester.setup_df_for_operation(pl_lf_no_op)

    assert isinstance(setup_df, pl.LazyFrame)
    plt.assert_frame_equal(setup_df, expected_df)


def test_setup_df_for_invalid_op_passed_raises(base_df):
    perf_tester = OperationsPerformanceTester(base_df)
    unsupported_subclass_instance = IncorrectSubclassOp()
    expected_error_type = type(unsupported_subclass_instance)

    with pytest.raises(ValueError) as e:
        perf_tester.setup_df_for_operation(unsupported_subclass_instance)

    assert str(e.value) == f"Unsupported operation: {expected_error_type}"


def test_time_operation_returns_time_elapsed(base_df, mocker):
    mocked_start = 10.5
    mocked_end = 12.5
    expected_time = mocked_end - mocked_start

    mocked_perf_counter = mocker.patch("rapids_exploration.testing_utils.perf_tester.perf_counter")
    mocked_perf_counter.side_effect = [mocked_start, mocked_end]
    mocked_execute_on = mocker.patch.object(PdDfNoOp, "execute_on")
    mocked_execute_on.return_value = None

    no_op_w_execute_mocked = PdDfNoOp()
    op_perf_tester = OperationsPerformanceTester(base_df)

    op_time = op_perf_tester.time_operation(no_op_w_execute_mocked)

    assert op_time == expected_time
    mocked_execute_on.assert_called_once()


@dataclass
class OpTesterSetup:
    perf_tester: OperationsPerformanceTester
    ops_to_test: List[DFOperation]
    expected_results: List[TestRes]

    def get_first_op_for_op_only_test(self) -> DFOperation:
        if len(self.ops_to_test) == 0:
            raise ValueError("No operations were provided")
        return self.ops_to_test[0]


def create_expected_res(op_performed: DFOperation, timer_mock_interval: int, trials_run):
    return TestRes(op_performed, trials_run, timer_mock_interval*trials_run)


def get_generator_for_uniform_timed_ops(expected_op_times: List[float], expected_num_trials: int) -> Iterable[float]:
    for _ in range(expected_num_trials):
        for expected_op_time in expected_op_times:
            yield expected_op_time  # start time yielded
            yield 2 * expected_op_time  # end time yielded, s.t. (end - start) == expected_op_time


def create_ops_to_test_w_execute_on_mocked(op_types: List[Type[DFOperation]], mocker) -> List[DFOperation]:
    ops_to_test = []
    for op_cls in op_types:
        mocked_execute_on = mocker.patch.object(op_cls, "execute_on")
        mocked_execute_on.return_value = None
        ops_to_test.append(op_cls())
    return ops_to_test


def derive_expected_results(ops_to_test: List[DFOperation], expected_op_times: List[float], expected_num_trials: int) -> List[TestRes]:
    return [
        create_expected_res(op_to_test, expected_op_time,  expected_num_trials)
        for op_to_test, expected_op_time
        in zip(ops_to_test, expected_op_times)
    ]


def init_perf_tester_passing_num_trials_only_when_supplied(base_df: pl.DataFrame, expected_num_trials: int, num_trials: Optional[int]) -> OperationsPerformanceTester:
    if num_trials is None:
        return OperationsPerformanceTester(base_df)
    return OperationsPerformanceTester(base_df, expected_num_trials)


def setup_test(
        base_df: pl.DataFrame,
        op_types: Union[Type[DFOperation], List[Type[DFOperation]]],
        mocker,
        num_trials: Optional[int] = None
) -> OpTesterSetup:
    if isinstance(op_types, type):
        op_types = [op_types]  # assuming for the sake of testing we are not passing incorrect input
    expected_num_trials = num_trials if num_trials is not None else C.DEFAULT_TEST_TRIALS
    # 1st op will always have time 1, 2nd 2, and so on (when multiple of course if just one then single op has 1)
    expected_op_times = [float(i) for i in range(1, len(op_types) + 1)]
    generator_to_ensure_expected_op_times = get_generator_for_uniform_timed_ops(expected_op_times, expected_num_trials)

    mocked_perf_counter = mocker.patch("rapids_exploration.testing_utils.perf_tester.perf_counter")
    mocked_perf_counter.side_effect = generator_to_ensure_expected_op_times
    ops_to_test = create_ops_to_test_w_execute_on_mocked(op_types, mocker)
    expected_results = derive_expected_results(ops_to_test, expected_op_times, expected_num_trials)
    perf_tester = init_perf_tester_passing_num_trials_only_when_supplied(base_df, expected_num_trials, num_trials)

    return OpTesterSetup(perf_tester=perf_tester, ops_to_test=ops_to_test, expected_results=expected_results)


def setup_default_num_trials_test(base_df: pl.DataFrame, op_types: Union[Type[DFOperation], List[Type[DFOperation]]], mocker) -> OpTesterSetup:
    """For explicitness in naming"""
    return setup_test(base_df, op_types, mocker)


def test_test_for_a_single_operation_default_num_trials_ran(base_df, mocker):
    single_op = PlDfNoOp
    test_setup = setup_default_num_trials_test(base_df, single_op, mocker)
    perf_tester = test_setup.perf_tester
    op_to_test = test_setup.get_first_op_for_op_only_test()
    expected_results = test_setup.expected_results

    test_results = perf_tester.test(op_to_test)

    assert test_results == expected_results


def test_test_for_a_single_operation_custom_num_trials_ran(base_df, mocker):
    single_op = PlDfNoOp
    test_setup = setup_test(base_df, single_op, mocker, num_trials=5)
    perf_tester = test_setup.perf_tester
    op_to_test = test_setup.get_first_op_for_op_only_test()
    expected_results = test_setup.expected_results

    test_results = perf_tester.test(op_to_test)

    assert test_results == expected_results


def test_test_for_multiple_operations_default_num_trials_ran(base_df, mocker):
    multiple_ops = [PlDfNoOp, PdDfNoOp, PlLfNoOp]
    test_setup = setup_default_num_trials_test(base_df, multiple_ops, mocker)
    perf_tester = test_setup.perf_tester
    ops_to_test = test_setup.ops_to_test
    expected_results = test_setup.expected_results

    test_results = perf_tester.test(ops_to_test)

    assert test_results == expected_results


def test_test_for_multiple_operations_custom_num_trials_ran(base_df, mocker):
    multiple_ops = [PlDfNoOp, PdDfNoOp]
    test_setup = setup_test(base_df, multiple_ops, mocker, num_trials=10)
    perf_tester = test_setup.perf_tester
    ops_to_test = test_setup.ops_to_test
    expected_results = test_setup.expected_results

    test_results = perf_tester.test(ops_to_test)

    assert test_results == expected_results
