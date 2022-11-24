import os

import polars as pl

# import pyarrow.dataset as
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from ezt.util.config import Config
from ezt.util.exceptions import EztConfigException
from ezt.util.helpers import get_s3_filesystem


def get_source(name: str) -> pl.LazyFrame:

    config = Config(os.getcwd())

    source_dict = config.get_source(name)

    source_getter = _get_source_getter_func(source_dict)
    return source_getter(source_dict)


def _get_source_getter_func(source_dict):
    if source_dict["filesystem"] == "s3":

        if source_dict["format"] == "parquet":
            return _get_s3_parq_source
        elif source_dict["format"] == "delta":
            return _get_s3_delta_source
        elif source_dict["format"] == "csv":
            return _get_s3_csv_source

    elif source_dict["filesystem"] == "local":

        if source_dict["format"] == "csv":
            return _get_csv_local_source
        elif source_dict["format"] == "parquet":
            return _get_parq_local_source


def _get_s3_parq_source(source):
    """
    Function that returns the parquet-source in s3 as a LazyFrame.
    Since lazy evaluation on remote storage not currently supported
    in polars, a dataframe is first created (read into memory) before it
    is converted to a lazyframe.
    """
    # set up s3 filesystem keys
    s3 = get_s3_filesystem()

    # removes leading and ending forwardslashes
    path = source["path"]
    if path.endswith("/"):
        path = path[:-1]

    if path.startswith("/"):
        path = path[1:]
    elif path.startswith("s3://"):
        path = path[5:]

    if source["path_type"] == "folder":
        target_path = f"s3://{path}/"
        dset = ds.dataset(target_path, format="parquet")
        table = pl.scan_ds(dset)
    elif source["path_type"] == "file":
        target_path = f"{path}"
        pa_table = pq.read_table(target_path, filesystem=s3)
        table = pl.from_arrow(pa_table).lazy()
    else:
        raise EztConfigException("Invalid or missing value for key 'path_type' in sources.yml.")

    return table


def _get_csv_local_source(source_dict):
    """Function that returns a lazyframe of a local csv-source."""
    if "csv_properties" in source_dict:
        df = pl.scan_csv(source_dict["path"], **source_dict["csv_properties"])
    else:
        df = pl.scan_csv(source_dict["path"])

    return df


def _get_parq_local_source(source_dict):
    """Function that returns a lazyframe of a local parquet-source."""
    return pl.scan_parquet(source_dict["path"])


def _get_s3_delta_source(source_dict):
    """Function that returns a lazyframe of a remote delta-source."""
    raise NotImplementedError("Delta sources in remote storage are not yet supported.")


def _get_s3_csv_source(source_dict):
    """Function that returns a lazyframe of a remote delta-source."""
    raise NotImplementedError("CSV sources in remote storage are not yet supported.")
