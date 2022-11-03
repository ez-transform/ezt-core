import polars as pl
import pytest
from ezt.build.dfmodel.py_model import py_model
from ezt.util.exceptions import PyModelException


def test_py_model_decorator_df(polars_dataframe):
    @py_model
    def my_df_func():
        return polars_dataframe

    df = my_df_func()

    assert isinstance(df, pl.DataFrame)
    assert my_df_func.is_py_model


def test_py_model_decorator_lf(polars_lazyframe):
    @py_model
    def my_lf_func():
        return polars_lazyframe

    df = my_lf_func()

    assert isinstance(df, pl.DataFrame)
    assert my_lf_func.is_py_model


def test_py_model_decorator_invalid_type(pyarrow_table):
    @py_model
    def my_pa_func():
        return pyarrow_table

    with pytest.raises(PyModelException) as e:
        df = my_pa_func()

    assert e.value.message == "Incorrect return type in model."
