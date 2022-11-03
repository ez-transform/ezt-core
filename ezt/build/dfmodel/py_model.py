from functools import wraps

import polars as pl
from ezt.util.config import Config
from ezt.util.exceptions import PyModelException


def py_model(func):
    @wraps(func)
    def wrapper(*args, **kwargs):

        df = func(*args, **kwargs)
        # TODO: add metadata if user opted in

        if isinstance(df, pl.LazyFrame):
            return df.collect()
        elif isinstance(df, pl.DataFrame):
            return df
        else:
            raise PyModelException

    wrapper.is_py_model = True
    return wrapper
