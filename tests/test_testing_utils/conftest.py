import polars as pl
import pytest

from rapids_exploration.df_operations.df_operation import DFOperation


@pytest.fixture
def mock_operation(mocker):
    yield mocker.Mock(spec=DFOperation)


@pytest.fixture
def base_df() -> pl.DataFrame:
    """A mock df with column test_col that contains vals 1-10"""
    yield pl.DataFrame({"test_col": [i for i in range(10)]})
