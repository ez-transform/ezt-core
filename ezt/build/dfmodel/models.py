import os

import deltalake as dl
import polars as pl
import pyarrow.parquet as pq
from ezt.util.config import Config
from ezt.util.helpers import get_s3_filesystem, prepare_s3_path
from ezt.util.exceptions import EztAuthenticationException


def get_model(name: str) -> pl.LazyFrame:
    """Returns the model as a polars lazyframe."""
    model = Config(os.getcwd()).get_model(name)

    model_getter_func = _get_model_getter_func(model)
    return model_getter_func(model)


def _get_model_getter_func(model):

    if model["filesystem"] == "local":

        if model["write_settings"]["file_type"] == "parquet":
            return _get_local_parq_model
        elif model["write_settings"]["file_type"] == "delta":
            return _get_local_delta_model

    elif model["filesystem"] == "s3":

        if model["write_settings"]["file_type"] == "parquet":
            return _get_s3_parq_model
        elif model["write_settings"]["file_type"] == "delta":
            return _get_s3_delta_model


def _get_local_delta_model(model):

    result_lazy = pl.from_arrow(
        dl.DeltaTable(table_uri=f'{model["destination"]}/{model["name"]}').to_pyarrow_table()
    ).lazy()
    return result_lazy


def _get_s3_delta_model(model):

    if os.getenv("AWS_ACCESS_KEY_ID") is None or os.getenv("AWS_SECRET_ACCESS_KEY") is None:
        raise EztAuthenticationException(
            "Environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY need to be set to authenticate to S3."
        )

    # required for writing delta tables to s3 without LockClient. Opts out of concurrent writes.
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"

    table_path = prepare_s3_path(model["destination"])

    result_lazy = pl.from_arrow(
        dl.DeltaTable(table_uri=f'{table_path}/{model["name"]}').to_pyarrow_table()
    ).lazy()
    return result_lazy


def _get_s3_parq_model(model):

    s3 = get_s3_filesystem()
    dataset = pq.ParquetDataset(
        path_or_paths=f"{model['destination']}/{model['name']}", filesystem=s3
    )

    return pl.from_arrow(dataset.read()).lazy()


def _get_local_parq_model(model):

    table = pq.read_table(f"{model['destination']}/{model['name']}.parquet")
    return pl.from_arrow(table).lazy()
