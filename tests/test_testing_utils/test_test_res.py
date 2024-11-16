from collections import OrderedDict

from rapids_exploration.testing_utils.test_res import TestRes
from tests.test_testing_utils.conftest import mock_operation


def test_init(mock_operation):
    trials_run = 10
    time_taken = 50.0
    test_res = TestRes(
        operation_performed=mock_operation, trials_run=trials_run, time_taken=time_taken
    )

    assert test_res.trials_run == trials_run
    assert test_res.time_taken == time_taken
    assert test_res.operation_performed == mock_operation


def test_avg_time_taken_computation(mock_operation):
    trials_run = 10
    time_taken = 50.0
    test_res = TestRes(
        operation_performed=mock_operation, trials_run=10, time_taken=50.0
    )

    avg_time = test_res.avg_time_taken

    assert avg_time == time_taken / trials_run


def test_as_dict(mock_operation):
    trials_run = 10
    time_taken = 50.0
    expected_avg_time_taken = time_taken / trials_run
    test_res = TestRes(
        operation_performed=mock_operation, trials_run=10, time_taken=50.0
    )

    result = test_res.as_ordered_dict()

    expected_dict = OrderedDict((
        ("operation_performed", test_res.operation_performed),
        ("trials_run", trials_run),
        ("time_taken", time_taken),
        ("avg_time_taken", expected_avg_time_taken),
    ))

    assert result == expected_dict
